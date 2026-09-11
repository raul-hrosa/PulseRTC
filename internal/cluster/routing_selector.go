package cluster

import "context"

// NodeSelector chooses which node should host a room.
// ships only LocalNodeSelector; a load-aware selector arrives with real load
// balancing later.
type NodeSelector interface {
	SelectNode(ctx context.Context, roomID string) (NodeInfo, error)
}

// LocalNodeSelector implements the rule:
//
//	existing room -> its current owner
//	new room      -> this node
type LocalNodeSelector struct{ c *Cluster }

// NewLocalNodeSelector binds a selector to a cluster.
func NewLocalNodeSelector(c *Cluster) *LocalNodeSelector { return &LocalNodeSelector{c: c} }

func (s *LocalNodeSelector) SelectNode(ctx context.Context, roomID string) (NodeInfo, error) {
	if res, ok := s.c.ResolveRoom(ctx, roomID); ok {
		if info, hit := s.c.registry.Get(res.OwnerID); hit {
			return info, nil
		}
		return NodeInfo{ID: res.OwnerID}, nil
	}
	if !s.c.Ready() {
		return NodeInfo{}, &Error{Code: CodeNodeShuttingDown, Message: "this node is draining"}
	}
	return s.c.Self(), nil
}
