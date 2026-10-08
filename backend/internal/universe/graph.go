package universe

// If you're wondering why there are graph traversal functions here
// and not the use of a library, then wonder no more. There's only
// 90-ish lines of code to do the small subset of graph functionality
// needed. This isn't enough to violate my "no dependencies unless absolutely
// necessary" rule.

// graph is a directed adjacency over 0-based sector indices.
type graph struct {
	out [][]int
	in  [][]int
}

func newGraph(n int) *graph {
	return &graph{out: make([][]int, n), in: make([][]int, n)}
}

func (g *graph) add(from, to int) {
	g.out[from] = append(g.out[from], to)
	g.in[to] = append(g.in[to], from)
}

// stronglyConnected indicates whether one vertex reaches all and all reach it.
func (g *graph) stronglyConnected() bool {
	n := len(g.out)
	if n == 0 {
		return true
	}
	return reachable(g.out, 0) == n && reachable(g.in, 0) == n
}

// reachable counts the vertices reachable from start, inclusive.
func reachable(adj [][]int, start int) int {
	seen := make([]bool, len(adj))
	seen[start] = true
	count := 1
	queue := []int{start}
	for len(queue) > 0 {
		v := queue[0]
		queue = queue[1:]
		for _, w := range adj[v] {
			if seen[w] {
				continue
			}
			seen[w] = true
			count++
			queue = append(queue, w)
		}
	}
	return count
}

// hopsFromAny returns hops from the nearest start to every vertex, -1 if
// unreachable.
func hopsFromAny(adj [][]int, starts []int) []int {
	dist := make([]int, len(adj))
	for i := range dist {
		dist[i] = -1
	}
	queue := make([]int, 0, len(starts))
	for _, s := range starts {
		if dist[s] == -1 {
			dist[s] = 0
			queue = append(queue, s)
		}
	}
	for len(queue) > 0 {
		v := queue[0]
		queue = queue[1:]
		for _, w := range adj[v] {
			if dist[w] != -1 {
				continue
			}
			dist[w] = dist[v] + 1
			queue = append(queue, w)
		}
	}
	return dist
}

// adjacency builds the graph of a finished universe.
func (u *Universe) adjacency() *graph {
	g := newGraph(len(u.Sectors))
	for _, l := range u.Lanes {
		g.add(l.From-1, l.To-1)
	}
	return g
}

// publicAdjacency builds the graph of the lanes public at creation.
func (u *Universe) publicAdjacency() *graph {
	g := newGraph(len(u.Sectors))
	for _, l := range u.PublicAtBigBang {
		g.add(l.From-1, l.To-1)
	}
	return g
}
