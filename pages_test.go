package edgerun

import (
	"context"
	"errors"
	"testing"
)

func TestPagesFetchesLazilyAndStopsWhenTheConsumerDoes(t *testing.T) {
	var asked []string
	fetch := func(_ context.Context, token string) ([]int, string, error) {
		asked = append(asked, token)
		switch token {
		case "":
			return []int{1, 2}, "p2", nil
		case "p2":
			return nil, "p3", nil // an empty page in the middle is not the end
		default:
			return []int{3}, "", nil
		}
	}
	var got []int
	for v, err := range pages(context.Background(), fetch) {
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, v)
	}
	if len(got) != 3 || got[2] != 3 || len(asked) != 3 {
		t.Errorf("got %v after asking for %q", got, asked)
	}

	asked = nil
	for v := range pages(context.Background(), fetch) {
		if v == 2 {
			break
		}
	}
	if len(asked) != 1 {
		t.Errorf("a consumer that stops must not cause more fetching: asked %q", asked)
	}
}

func TestPagesYieldsAnErrorOnceAndEnds(t *testing.T) {
	boom := errors.New("boom")
	n := 0
	for _, err := range pages(context.Background(), func(context.Context, string) ([]int, string, error) {
		n++
		if n == 1 {
			return []int{1}, "next", nil
		}
		return nil, "", boom
	}) {
		if err != nil && !errors.Is(err, boom) {
			t.Fatal(err)
		}
	}
	if n != 2 {
		t.Errorf("fetched %d pages", n)
	}
}
