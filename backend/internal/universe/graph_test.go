package universe

import (
	"slices"
	"testing"
)

func build(n int, edges ...[2]int) *graph {
	g := newGraph(n)
	for _, e := range edges {
		g.add(e[0], e[1])
	}
	return g
}

func TestStronglyConnected(t *testing.T) {
	cases := map[string]struct {
		g    *graph
		want bool
	}{
		"empty":        {build(0), true},
		"single node":  {build(1), true},
		"cycle":        {build(3, [2]int{0, 1}, [2]int{1, 2}, [2]int{2, 0}), true},
		"two-way pair": {build(2, [2]int{0, 1}, [2]int{1, 0}), true},
		// A path is the trap case: 0 reaches 2 but 2 never gets back.
		"path":      {build(3, [2]int{0, 1}, [2]int{1, 2}), false},
		"island":    {build(3, [2]int{0, 1}, [2]int{1, 0}), false},
		"sink":      {build(3, [2]int{0, 1}, [2]int{1, 0}, [2]int{1, 2}), false},
		"no source": {build(3, [2]int{0, 1}, [2]int{1, 0}, [2]int{2, 1}), false},
	}
	for label, c := range cases {
		t.Run(label, func(t *testing.T) {
			if got := c.g.stronglyConnected(); got != c.want {
				t.Errorf("stronglyConnected = %v, want %v", got, c.want)
			}
		})
	}
}

func TestHopsFromAny(t *testing.T) {
	// 0 - 1 - 2 - 3, plus an orphan at 4.
	g := build(5,
		[2]int{0, 1}, [2]int{1, 0},
		[2]int{1, 2}, [2]int{2, 1},
		[2]int{2, 3}, [2]int{3, 2},
	)
	if got := hopsFromAny(g.out, []int{0}); !slices.Equal(got, []int{0, 1, 2, 3, -1}) {
		t.Errorf("from 0 = %v", got)
	}
	// Two starts: every sector takes the nearer one.
	if got := hopsFromAny(g.out, []int{0, 3}); !slices.Equal(got, []int{0, 1, 1, 0, -1}) {
		t.Errorf("from 0 and 3 = %v", got)
	}
	if got := hopsFromAny(g.out, nil); !slices.Equal(got, []int{-1, -1, -1, -1, -1}) {
		t.Errorf("from nothing = %v", got)
	}
}

// Direction matters: reaching the trunk is asked of the reverse graph.
func TestHopsRespectsDirection(t *testing.T) {
	g := build(3, [2]int{0, 1}, [2]int{1, 2})
	if got := hopsFromAny(g.out, []int{0}); !slices.Equal(got, []int{0, 1, 2}) {
		t.Errorf("forward from 0 = %v", got)
	}
	if got := hopsFromAny(g.out, []int{2}); !slices.Equal(got, []int{-1, -1, 0}) {
		t.Errorf("forward from 2 = %v", got)
	}
	if got := hopsFromAny(g.in, []int{2}); !slices.Equal(got, []int{2, 1, 0}) {
		t.Errorf("reverse from 2 = %v", got)
	}
}
