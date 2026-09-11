package room

import "sync"

// Manager owns the lifecycle of every Room.
type Manager struct {
	mu    sync.Mutex
	rooms map[string]*Room
}

// NewManager creates an empty room manager.
func NewManager() *Manager {
	return &Manager{rooms: make(map[string]*Room)}
}

// GetOrCreate returns the room with the given id, creating it if necessary.
func (m *Manager) GetOrCreate(id string) *Room {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.rooms[id]
	if !ok {
		r = newRoom(id)
		m.rooms[id] = r
	}
	return r
}

// Get returns the room with the given id, if it exists.
func (m *Manager) Get(id string) (*Room, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.rooms[id]
	return r, ok
}

// RemoveParticipant removes a participant from a room and deletes the room
// once it becomes empty, so idle rooms do not leak.
func (m *Manager) RemoveParticipant(roomID, participantID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.rooms[roomID]
	if !ok {
		return
	}
	if empty := r.Remove(participantID); empty {
		delete(m.rooms, roomID)
	}
}

// Count returns the number of active rooms.
func (m *Manager) Count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.rooms)
}
