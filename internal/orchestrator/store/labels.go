package store

import (
	"context"
	"errors"
	"regexp"
	"strconv"
	"strings"

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

// LabelFilter selects resources by their labels: all of Match (exact values),
// a label for each key of Has, and none for any key of Lacks.
type LabelFilter struct {
	Match map[string]string
	Has   []string
	Lacks []string
}

// Validate refuses a filter that is malformed or contradicts itself: it could
// only match nothing, which for a caller about to act on what it finds (a
// prune) is the dangerous way to be wrong. A malformed key in Match is not
// refused; it matches nothing, as it always has.
func (f LabelFilter) Validate() error {
	for _, keys := range [][]string{f.Has, f.Lacks} {
		for _, k := range keys {
			if !labelKey.MatchString(k) {
				return invalid("label key %q: want 1-63 lower-case letters, digits and '.', '_', '/', '-', starting and ending with a letter or digit", k)
			}
		}
	}
	for _, k := range f.Lacks {
		if _, ok := f.Match[k]; ok {
			return invalid("label key %q is required by match_labels and excluded by lacks_label_keys", k)
		}
		for _, h := range f.Has {
			if h == k {
				return invalid("label key %q is in both has_label_keys and lacks_label_keys", k)
			}
		}
	}
	return nil
}

// where appends the conditions on column labels to args and returns them as
// SQL ("TRUE" if the filter selects everything).
func (f LabelFilter) where(args []any) (string, []any) {
	var cond []string
	if len(f.Match) > 0 {
		args = append(args, f.Match)
		cond = append(cond, "labels @> $"+strconv.Itoa(len(args)))
	}
	if len(f.Has) > 0 {
		args = append(args, f.Has)
		cond = append(cond, "labels ?& $"+strconv.Itoa(len(args))+"::text[]")
	}
	if len(f.Lacks) > 0 {
		args = append(args, f.Lacks)
		cond = append(cond, "NOT (labels ?| $"+strconv.Itoa(len(args))+"::text[])")
	}
	if len(cond) == 0 {
		return "TRUE", args
	}
	return strings.Join(cond, " AND "), args
}

// patched returns cur with set applied and remove removed, after checking the
// patch and the result.
func patched(cur, set map[string]string, remove []string) (map[string]string, error) {
	for _, k := range remove {
		if !labelKey.MatchString(k) {
			return nil, invalid("label key %q: want 1-63 lower-case letters, digits and '.', '_', '/', '-', starting and ending with a letter or digit", k)
		}
		if _, ok := set[k]; ok {
			return nil, invalid("label key %q is both set and removed", k)
		}
	}
	if err := ValidateLabels(set); err != nil {
		return nil, err
	}
	out := make(map[string]string, len(cur)+len(set))
	for k, v := range cur {
		out[k] = v
	}
	for _, k := range remove {
		delete(out, k)
	}
	for k, v := range set {
		out[k] = v
	}
	return out, ValidateLabels(out)
}

// PatchLabels changes some of a function's labels, atomically with respect to
// other changes of its labels.
func (s *Store) PatchLabels(ctx context.Context, functionID string, set map[string]string, remove []string) (*controlv1.Function, error) {
	fid, err := parseID("function", functionID)
	if err != nil {
		return nil, err
	}
	var out *controlv1.Function
	err = s.inTx(ctx, func(tx pgx.Tx) error {
		var cur map[string]string
		if err := tx.QueryRow(ctx, `SELECT labels FROM functions WHERE id = $1 FOR UPDATE`, fid).Scan(&cur); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return notFound("function %s", fid)
			}
			return err
		}
		next, err := patched(cur, set, remove)
		if err != nil {
			return err
		}
		out, err = scanFunction(tx.QueryRow(ctx, `UPDATE functions SET labels = $2 WHERE id = $1 RETURNING `+functionCols, fid, labelArg(next)))
		return err
	})
	return out, err
}

// SetRouteLabels replaces a route's labels. Nothing else of a route changes.
func (s *Store) SetRouteLabels(ctx context.Context, routeID string, labels map[string]string) (*controlv1.Route, error) {
	rid, err := parseID("route", routeID)
	if err != nil {
		return nil, err
	}
	if err := ValidateLabels(labels); err != nil {
		return nil, err
	}
	r, err := scanRoute(s.q(ctx).QueryRow(ctx, `UPDATE routes SET labels = $2 WHERE id = $1 RETURNING `+routeCols, rid, labelArg(labels)))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, notFound("route %s", rid)
	}
	return r, err
}

// PatchRouteLabels is PatchLabels for a route.
func (s *Store) PatchRouteLabels(ctx context.Context, routeID string, set map[string]string, remove []string) (*controlv1.Route, error) {
	rid, err := parseID("route", routeID)
	if err != nil {
		return nil, err
	}
	var out *controlv1.Route
	err = s.inTx(ctx, func(tx pgx.Tx) error {
		var cur map[string]string
		if err := tx.QueryRow(ctx, `SELECT labels FROM routes WHERE id = $1 FOR UPDATE`, rid).Scan(&cur); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return notFound("route %s", rid)
			}
			return err
		}
		next, err := patched(cur, set, remove)
		if err != nil {
			return err
		}
		out, err = scanRoute(tx.QueryRow(ctx, `UPDATE routes SET labels = $2 WHERE id = $1 RETURNING `+routeCols, rid, labelArg(next)))
		return err
	})
	return out, err
}
