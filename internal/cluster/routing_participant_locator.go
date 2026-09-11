package cluster

import "sync"

// ParticipantLocation is the distributable location of a participant's session.
// The WebRTC session itself (PeerConnection, tracks) never
// leaves its node — only this record is shared.
type ParticipantLocation struct {
	ParticipantID string `json:"participantId"`
	RoomID        string `json:"roomId"`
	NodeID        string `json:"nodeId"`
}

// ParticipantLocator answers "which node holds this participant's session?".
type ParticipantLocator interface {
	Register(loc ParticipantLocation)
	Remove(participantID string)
	Lookup(participantID string) (ParticipantLocation, bool)
	InRoom(roomID string) []ParticipantLocation
	CountByNode(nodeID string) int
}

// InMemoryParticipantLocator is the implementation.
type InMemoryParticipantLocator struct {
	mu   sync.RWMutex
	byID map[string]ParticipantLocation
}

// NewInMemoryParticipantLocator builds an empty locator.
func NewInMemoryParticipantLocator() *InMemoryParticipantLocator {
	return &InMemoryParticipantLocator{byID: make(map[string]ParticipantLocation)}
}

func (p *InMemoryParticipantLocator) Register(loc ParticipantLocation) {
	p.mu.Lock()
	p.byID[loc.ParticipantID] = loc
	p.mu.Unlock()
}

func (p *InMemoryParticipantLocator) Remove(participantID string) {
	p.mu.Lock()
	delete(p.byID, participantID)
	p.mu.Unlock()
}

func (p *InMemoryParticipantLocator) Lookup(participantID string) (ParticipantLocation, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	loc, ok := p.byID[participantID]
	return loc, ok
}

func (p *InMemoryParticipantLocator) InRoom(roomID string) []ParticipantLocation {
	p.mu.RLock()
	defer p.mu.RUnlock()
	var out []ParticipantLocation
	for _, loc := range p.byID {
		if loc.RoomID == roomID {
			out = append(out, loc)
		}
	}
	return out
}

func (p *InMemoryParticipantLocator) CountByNode(nodeID string) int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	n := 0
	for _, loc := range p.byID {
		if loc.NodeID == nodeID {
			n++
		}
	}
	return n
}
