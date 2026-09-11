package room

import "sync"

// Participant is anything that can receive raw message bytes.
// The signaling layer supplies the concrete implementation (a WebSocket client).
type Participant interface {
	ID() string
	Send(data []byte)
}

// Room holds a set of participants able to exchange signaling messages.
type Room struct {
	id string

	mu           sync.RWMutex
	participants map[string]Participant
}

func newRoom(id string) *Room {
	return &Room{
		id:           id,
		participants: make(map[string]Participant),
	}
}

// ID returns the room identifier.
func (r *Room) ID() string { return r.id }

// Add inserts a participant into the room.
func (r *Room) Add(p Participant) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.participants[p.ID()] = p
}

// Remove drops a participant from the room and reports whether the room is now empty.
func (r *Room) Remove(id string) (empty bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.participants, id)
	return len(r.participants) == 0
}

// Get returns the participant with the given id, if present in this room.
func (r *Room) Get(id string) (Participant, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	p, ok := r.participants[id]
	return p, ok
}

// ParticipantIDs returns the ids of every participant currently in the room.
func (r *Room) ParticipantIDs() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ids := make([]string, 0, len(r.participants))
	for id := range r.participants {
		ids = append(ids, id)
	}
	return ids
}

// Size returns the number of participants in the room.
func (r *Room) Size() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.participants)
}

// Broadcast sends data to every participant except the one identified by exceptID.
// Pass an empty exceptID to send to everyone.
func (r *Room) Broadcast(data []byte, exceptID string) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for id, p := range r.participants {
		if id == exceptID {
			continue
		}
		p.Send(data)
	}
}
