package repository

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"sort"
	"strconv"
	"strings"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

//go:embed migrations/*.sql
var embeddedMigrations embed.FS

type schemaMigrationRecord struct {
	Version   uint64    `gorm:"column:version;primaryKey"`
	AppliedAt time.Time `gorm:"column:applied_at;not null;autoCreateTime"`
}

func (schemaMigrationRecord) TableName() string {
	return "schema_migrations"
}

type migrationFile struct {
	Version uint64
	Name    string
	SQL     string
}

type migrationRunner struct {
	db   *gorm.DB
	fsys fs.FS
}

type appliedVersionsLoader func(context.Context) (map[uint64]struct{}, error)

type migrationApplier func(context.Context, migrationFile) error

type migrationRecorder func(context.Context, uint64) error

func newMigrationRunner(db *gorm.DB) *migrationRunner {
	return &migrationRunner{
		db:   db,
		fsys: embeddedMigrations,
	}
}

func (r *migrationRunner) Run(ctx context.Context) error {
	if r.db == nil {
		return fmt.Errorf("migration db is nil")
	}
	if err := r.ensureMigrationTable(ctx); err != nil {
		return err
	}
	return runMigrationFiles(ctx, r.fsys, r.appliedVersions, r.execMigration, r.recordMigration)
}

func (r *migrationRunner) ensureMigrationTable(ctx context.Context) error {
	if r.db.WithContext(ctx).Migrator().HasTable(&schemaMigrationRecord{}) {
		return nil
	}
	return r.db.WithContext(ctx).Migrator().CreateTable(&schemaMigrationRecord{})
}

func (r *migrationRunner) appliedVersions(ctx context.Context) (map[uint64]struct{}, error) {
	var records []schemaMigrationRecord
	if err := r.db.WithContext(ctx).Order("version ASC").Find(&records).Error; err != nil {
		return nil, err
	}

	applied := make(map[uint64]struct{}, len(records))
	for _, record := range records {
		applied[record.Version] = struct{}{}
	}
	return applied, nil
}

func (r *migrationRunner) execMigration(ctx context.Context, file migrationFile) error {
	return r.db.WithContext(ctx).Exec(file.SQL).Error
}

func (r *migrationRunner) recordMigration(ctx context.Context, version uint64) error {
	record := schemaMigrationRecord{Version: version}
	return r.db.WithContext(ctx).
		Clauses(clause.OnConflict{DoNothing: true}).
		Create(&record).Error
}

func runMigrationFiles(
	ctx context.Context,
	fsys fs.FS,
	loadApplied appliedVersionsLoader,
	apply migrationApplier,
	record migrationRecorder,
) error {
	applied, err := loadApplied(ctx)
	if err != nil {
		return err
	}
	files, err := loadMigrationFiles(fsys)
	if err != nil {
		return err
	}
	for _, file := range files {
		if _, ok := applied[file.Version]; ok {
			continue
		}
		if err := apply(ctx, file); err != nil {
			return fmt.Errorf("apply migration %s: %w", file.Name, err)
		}
		if err := record(ctx, file.Version); err != nil {
			return fmt.Errorf("record migration %s: %w", file.Name, err)
		}
		applied[file.Version] = struct{}{}
	}
	return nil
}

func loadMigrationFiles(fsys fs.FS) ([]migrationFile, error) {
	entries, err := fs.ReadDir(fsys, "migrations")
	if err != nil {
		return nil, err
	}

	files := make([]migrationFile, 0, len(entries))
	seen := make(map[uint64]string, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}

		version, err := parseMigrationVersion(entry.Name())
		if err != nil {
			return nil, err
		}
		if previous, ok := seen[version]; ok {
			return nil, fmt.Errorf("duplicate migration version %d in %s and %s", version, previous, entry.Name())
		}
		seen[version] = entry.Name()

		data, err := fs.ReadFile(fsys, pathJoin("migrations", entry.Name()))
		if err != nil {
			return nil, err
		}
		sql := strings.TrimSpace(string(data))
		if sql == "" {
			continue
		}
		files = append(files, migrationFile{
			Version: version,
			Name:    entry.Name(),
			SQL:     sql,
		})
	}

	sort.Slice(files, func(i, j int) bool {
		if files[i].Version == files[j].Version {
			return files[i].Name < files[j].Name
		}
		return files[i].Version < files[j].Version
	})
	return files, nil
}

func parseMigrationVersion(name string) (uint64, error) {
	base := strings.TrimSuffix(name, ".sql")
	prefix := base
	if idx := strings.Index(base, "_"); idx >= 0 {
		prefix = base[:idx]
	}
	if prefix == "" {
		return 0, fmt.Errorf("invalid migration filename %q", name)
	}
	version, err := strconv.ParseUint(prefix, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse migration version from %q: %w", name, err)
	}
	return version, nil
}

func pathJoin(elem ...string) string {
	return strings.Join(elem, "/")
}
