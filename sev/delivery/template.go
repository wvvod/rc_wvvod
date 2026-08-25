package delivery

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

var templateTokenPattern = regexp.MustCompile(`{{\s*([^{}]+?)\s*}}`)
var singleTemplateTokenPattern = regexp.MustCompile(`^\s*{{\s*([^{}]+?)\s*}}\s*$`)

type templateSegment struct {
	literal *string
	token   *string
}

func singleToken(template string) (string, bool) {
	match := singleTemplateTokenPattern.FindStringSubmatch(template)
	if len(match) != 2 {
		return "", false
	}
	return strings.TrimSpace(match[1]), true
}

func splitTemplate(template string) ([]templateSegment, error) {
	matches := templateTokenPattern.FindAllStringSubmatchIndex(template, -1)
	if len(matches) == 0 {
		literal := template
		return []templateSegment{{literal: &literal}}, nil
	}

	segments := make([]templateSegment, 0, len(matches)*2+1)
	last := 0
	for _, match := range matches {
		start, end := match[0], match[1]
		if start > last {
			literal := template[last:start]
			segments = append(segments, templateSegment{literal: &literal})
		}

		if len(match) < 4 {
			return nil, fmt.Errorf("invalid template token in %q", template)
		}

		token := strings.TrimSpace(template[match[2]:match[3]])
		segments = append(segments, templateSegment{token: &token})
		last = end
	}

	if last < len(template) {
		literal := template[last:]
		segments = append(segments, templateSegment{literal: &literal})
	}

	return segments, nil
}

func resolveToken(token string, payload map[string]any, secrets map[string]string) (any, error) {
	switch {
	case token == "payload":
		return payload, nil
	case strings.HasPrefix(token, "payload."):
		return lookupPath(payload, strings.Split(strings.TrimPrefix(token, "payload."), "."))
	case strings.HasPrefix(token, "secret."):
		key := strings.TrimPrefix(token, "secret.")
		value, ok := secrets[key]
		if !ok {
			return nil, fmt.Errorf("secret %q not found", key)
		}
		return value, nil
	default:
		return nil, fmt.Errorf("unsupported token %q", token)
	}
}

func lookupPath(current any, path []string) (any, error) {
	if len(path) == 0 {
		return current, nil
	}

	segment := path[0]
	rest := path[1:]

	switch value := current.(type) {
	case map[string]any:
		next, ok := value[segment]
		if !ok {
			return nil, fmt.Errorf("payload path %q not found", strings.Join(path, "."))
		}
		return lookupPath(next, rest)
	case map[string]string:
		next, ok := value[segment]
		if !ok {
			return nil, fmt.Errorf("payload path %q not found", strings.Join(path, "."))
		}
		if len(rest) > 0 {
			return nil, fmt.Errorf("payload path %q does not resolve to nested value", strings.Join(path, "."))
		}
		return next, nil
	case []any:
		index, err := strconv.Atoi(segment)
		if err != nil {
			return nil, fmt.Errorf("payload path %q expects numeric index", strings.Join(path, "."))
		}
		if index < 0 || index >= len(value) {
			return nil, fmt.Errorf("payload index %d out of range", index)
		}
		return lookupPath(value[index], rest)
	case []string:
		index, err := strconv.Atoi(segment)
		if err != nil {
			return nil, fmt.Errorf("payload path %q expects numeric index", strings.Join(path, "."))
		}
		if index < 0 || index >= len(value) {
			return nil, fmt.Errorf("payload index %d out of range", index)
		}
		if len(rest) > 0 {
			return nil, fmt.Errorf("payload path %q does not resolve to nested value", strings.Join(path, "."))
		}
		return value[index], nil
	default:
		if len(path) > 0 {
			return nil, fmt.Errorf("payload path %q not found", strings.Join(path, "."))
		}
		return current, nil
	}
}
