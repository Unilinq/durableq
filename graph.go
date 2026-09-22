package durableq

import (
	"fmt"
	"strings"
)

// stepNode is the minimal shape buildStepGraph needs to validate and resolve
// a job's step topology. after is exactly what the step was configured with:
// nil means "no predecessor", not "use the default" - resolving the default
// (the step declared before it) is wire()'s job, before it calls this.
type stepNode struct {
	id    string
	after []string
}

// buildStepGraph validates a job's step topology and returns, for every step
// id, the successor ids it fans out to. nodes must be given in declaration
// order; nodes[0] is the step every other step must be reachable from.
//
// Fan-out only: every step has at most one predecessor, so the only valid
// shape is a tree rooted at nodes[0]. A join - a step naming more than one
// predecessor - is rejected outright, not silently accepted, because it is a
// different feature with different delivery semantics reserved for later.
func buildStepGraph(nodes []stepNode) (map[string][]string, error) {
	if len(nodes) == 0 {
		return map[string][]string{}, nil
	}

	ids := make(map[string]bool, len(nodes))
	for _, n := range nodes {
		ids[n.id] = true
	}

	parent := make(map[string]string, len(nodes))
	successors := make(map[string][]string, len(nodes))
	for _, n := range nodes {
		if len(n.after) > 1 {
			return nil, fmt.Errorf(
				"durableq: join is not supported (step %q names %d predecessors: %s)",
				n.id, len(n.after), strings.Join(n.after, ", "))
		}
		if len(n.after) == 1 {
			pred := n.after[0]
			if !ids[pred] {
				return nil, fmt.Errorf(
					"durableq: step %q names an unknown predecessor %q", n.id, pred)
			}
			parent[n.id] = pred
			successors[pred] = append(successors[pred], n.id)
		}
	}

	// Cycle detection: chase parent pointers from every node. Since a node has
	// at most one parent, this is a plain linked-list walk; a repeated node
	// before reaching one with no parent is a cycle.
	const (
		white = 0
		gray  = 1
		black = 2
	)
	color := make(map[string]int, len(nodes))
	var path []string
	var walk func(id string) error
	walk = func(id string) error {
		switch color[id] {
		case black:
			return nil
		case gray:
			start := 0
			for i, p := range path {
				if p == id {
					start = i
					break
				}
			}
			cycle := append(append([]string(nil), path[start:]...), id)
			return fmt.Errorf("durableq: cycle in step graph: %s", strings.Join(cycle, " -> "))
		}
		color[id] = gray
		path = append(path, id)
		if p, ok := parent[id]; ok {
			if err := walk(p); err != nil {
				return err
			}
		}
		path = path[:len(path)-1]
		color[id] = black
		return nil
	}
	for _, n := range nodes {
		if err := walk(n.id); err != nil {
			return nil, err
		}
	}

	// Reachability: every step must be reached by walking successor edges
	// from the first step. Edges are authoritative for topology, so anything
	// not reached this way is disconnected from the job's entry point.
	visited := map[string]bool{nodes[0].id: true}
	queue := []string{nodes[0].id}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for _, s := range successors[cur] {
			if !visited[s] {
				visited[s] = true
				queue = append(queue, s)
			}
		}
	}
	for _, n := range nodes {
		if !visited[n.id] {
			return nil, fmt.Errorf(
				"durableq: step %q is not reached from the first step %q", n.id, nodes[0].id)
		}
	}

	return successors, nil
}
