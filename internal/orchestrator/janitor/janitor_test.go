package janitor

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"
)

type fake struct {
	invoc    []int // what each DeleteInvocationsBefore call returns
	calls    int
	cutoffs  []time.Time
	limits   []int
	workers  int
	invErr   error
	pruned   bool
	graceGot time.Duration
}

func (f *fake) DeleteInvocationsBefore(_ context.Context, t time.Time, limit int) (int, error) {
	f.cutoffs, f.limits = append(f.cutoffs, t), append(f.limits, limit)
	if f.invErr != nil {
		return 0, f.invErr
	}
	n := f.invoc[f.calls]
	f.calls++
	return n, nil
}

func (f *fake) PruneWorkers(_ context.Context, g time.Duration) (int, error) {
	f.pruned, f.graceGot = true, g
	return f.workers, nil
}

func cfg(f *fake) Config {
	return Config{Store: f, InvocationRetention: 24 * time.Hour, Batch: 100, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
}

func TestOnceDeletesInBatchesUntilTheBacklogIsGone(t *testing.T) {
	f := &fake{invoc: []int{100, 100, 100, 37}, workers: 2}
	inv, w, err := Once(context.Background(), cfg(f))
	if err != nil || inv != 337 || w != 2 {
		t.Fatalf("inv=%d workers=%d err=%v", inv, w, err)
	}
	if f.calls != 4 {
		t.Errorf("calls = %d: must keep going while batches come back full, and stop at the first short one", f.calls)
	}
	for _, l := range f.limits {
		if l != 100 {
			t.Errorf("batch limit = %d", l)
		}
	}
	if age := time.Since(f.cutoffs[0]); age < 23*time.Hour || age > 25*time.Hour {
		t.Errorf("cutoff is %v old, want ~24h", age)
	}
	if f.graceGot != time.Hour {
		t.Errorf("worker grace = %v, want the 1h default", f.graceGot)
	}
}

func TestOnceStopsOnErrorWithoutPruningWorkers(t *testing.T) {
	f := &fake{invErr: errors.New("db down")}
	if _, _, err := Once(context.Background(), cfg(f)); err == nil {
		t.Fatal("the error must be reported")
	}
	if f.pruned {
		t.Error("a failed pass must stop, not carry on")
	}
}

func TestRunPassesPeriodicallyAndStops(t *testing.T) {
	f := &fake{invoc: make([]int, 1000)}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { Run(ctx, cfg(f), 10*time.Millisecond); close(done) }()
	time.Sleep(120 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not stop")
	}
	if f.calls < 3 {
		t.Errorf("only %d passes in 120ms at a 10ms interval", f.calls)
	}
}
