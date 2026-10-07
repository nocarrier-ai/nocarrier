package universe

// graph is a directed adjacency structure over sectors indexed from 0. It is
// rebuilt from the working edge set whenever the one-way pass needs to test a
// conversion, which happens once per candidate at generation time and never
// afterwards.
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

// stronglyConnected reports whether every sector can reach every other one.
// A graph is strongly connected exactly when one vertex reaches all vertices
// and all vertices reach it, so two traversals from sector 0 suffice.
//
// This is the check that enforces the rule that nobody gets stranded. Every
// conversion in the character pass is tested against it and reverted if it
// fails.
func (g *graph) stronglyConnected() bool {
	n := len(g.out)
	if n == 0 {
		return true
	}
	return reachable(g.out, 0) == n && reachable(g.in, 0) == n
}

// reachable counts the vertices reachable from start over adj, start included.
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

// hopsFromAny returns the number of lanes from the nearest member of starts to
// every sector, or -1 where none reaches it.
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

// adjacency builds the forward and reverse adjacency of a finished universe,
// indexed from 0.
func (u *Universe) adjacency() *graph {
	g := newGraph(len(u.Sectors))
	for _, l := range u.Lanes {
		g.add(l.From-1, l.To-1)
	}
	return g
}
