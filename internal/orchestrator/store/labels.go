package store

import (
	"context"
	"errors"
	"regexp"

	"github.com/jackc/pgx/v5"

	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
)

// Labels are free-form metadata set by whoever manages a resource. Nothing in
// edgerun interprets them.
const (
	maxLabels     = 32
	maxLabelValue = 253
)

// A key is lower-case letters, digits and '.', '_', '/', '-', starting and
// ending with a letter or digit, 1-63 characters: "owner", "app.example.com/managed-by".
var labelKey = regexp.MustCompile(`^[a-z0-9]([a-z0-9._/-]{0,61}[a-z0-9])?$`)

// ValidateLabels checks the form of a set of labels.
func ValidateLabels(m map[string]string) error {
	if len(m) > maxLabels {
		return invalid("at most %d labels, got %d", maxLabels, len(m))
	}
	for k, v := range m {
		if !labelKey.MatchString(k) {
			return invalid("label key %q: want 1-63 lower-case letters, digits and '.', '_', '/', '-', starting and ending with a letter or digit", k)
		}
		if len(v) > maxLabelValue {
			return invalid("label %q: the value is longer than %d characters", k, maxLabelValue)
		}
		for i := 0; i < len(v); i++ {
			if v[i] < 0x20 || v[i] > 0x7e {
				return invalid("label %q: the value may contain only printable ASCII", k)
			}
		}
	}
	return nil
}

// labelArg is a label set as a statement argument: never nil, because a nil map
// would be stored as JSON null.
func labelArg(m map[string]string) map[string]string {
	if m == nil {
		return map[string]string{}
	}
	return m
}

// SetLabels replaces a function's labels.
func (s *Store) SetLabels(ctx context.Context, functionID string, labels map[string]string) (*controlv1.Function, error) {
	fid, err := parseID("function", functionID)
	if err != nil {
		return nil, err
	}
	if err := ValidateLabels(labels); err != nil {
		return nil, err
	}
	f, err := scanFunction(s.q(ctx).QueryRow(ctx,
		`UPDATE functions SET labels = $2 WHERE id = $1 RETURNING `+functionCols, fid, labelArg(labels)))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, notFound("function %s", fid)
	}
	return f, err
}
