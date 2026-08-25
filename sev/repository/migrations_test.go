package repository

import (
	"context"
	"errors"
	"io/fs"
	"testing"
	"testing/fstest"
)

func TestLoadMigrationFilesOrdersByVersion(t *testing.T) {
	fsys := fstest.MapFS{
		"migrations/0002_second.sql": &fstest.MapFile{Data: []byte("CREATE TABLE second (id INT);")},
		"migrations/0001_first.sql":  &fstest.MapFile{Data: []byte("CREATE TABLE first (id INT);")},
	}

	files, err := loadMigrationFiles(fsys)
	if err != nil {
		t.Fatalf("load migration files: %v", err)
	}
	if len(files) != 2 {
		t.Fatalf("expected 2 files, got %d", len(files))
	}
	if files[0].Version != 1 || files[1].Version != 2 {
		t.Fatalf("unexpected order: %#v", files)
	}
}

func TestRunMigrationFilesSkipsAppliedVersions(t *testing.T) {
	fsys := fstest.MapFS{
		"migrations/0001_first.sql":  &fstest.MapFile{Data: []byte("CREATE TABLE first (id INT);")},
		"migrations/0002_second.sql": &fstest.MapFile{Data: []byte("CREATE TABLE second (id INT);")},
	}

	var executed []string
	var recorded []uint64
	err := runMigrationFiles(
		context.Background(),
		fsys,
		func(context.Context) (map[uint64]struct{}, error) {
			return map[uint64]struct{}{1: {}}, nil
		},
		func(_ context.Context, file migrationFile) error {
			executed = append(executed, file.Name)
			return nil
		},
		func(_ context.Context, version uint64) error {
			recorded = append(recorded, version)
			return nil
		},
	)
	if err != nil {
		t.Fatalf("run migrations: %v", err)
	}
	if len(executed) != 1 || executed[0] != "0002_second.sql" {
		t.Fatalf("unexpected executed migrations: %#v", executed)
	}
	if len(recorded) != 1 || recorded[0] != 2 {
		t.Fatalf("unexpected recorded versions: %#v", recorded)
	}
}

func TestRunMigrationFilesPropagatesApplyErrors(t *testing.T) {
	fsys := fstest.MapFS{
		"migrations/0001_first.sql": &fstest.MapFile{Data: []byte("CREATE TABLE first (id INT);")},
	}

	want := errors.New("boom")
	err := runMigrationFiles(
		context.Background(),
		fsys,
		func(context.Context) (map[uint64]struct{}, error) {
			return map[uint64]struct{}{}, nil
		},
		func(context.Context, migrationFile) error {
			return want
		},
		func(context.Context, uint64) error {
			t.Fatal("record should not be called after apply failure")
			return nil
		},
	)
	if err == nil || !errors.Is(err, want) {
		t.Fatalf("expected wrapped apply error, got %v", err)
	}
}

func TestParseMigrationVersionRejectsInvalidNames(t *testing.T) {
	if _, err := parseMigrationVersion("bad.sql"); err == nil {
		t.Fatalf("expected invalid filename to fail")
	}
}

var _ fs.FS = fstest.MapFS{}
