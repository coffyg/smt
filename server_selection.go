package smt

// ServerSelector optimizes server selection with an internal buffer 
// to reduce channel operations and contention
type ServerSelector struct {
	servers []string
	index   int
	count   int
}

// NewServerSelector creates a new server selector
func NewServerSelector(serverCount int) *ServerSelector {
	return &ServerSelector{
		servers: make([]string, serverCount),
		index:   0,
		count:   0,
	}
}

// Add adds a server to the selector
func (s *ServerSelector) Add(server string) {
	if s.count < len(s.servers) {
		s.servers[s.count] = server
		s.count++
	}
}

// Get gets the next server from the selector in round-robin fashion
func (s *ServerSelector) Get() (string, bool) {
	if s.count == 0 {
		return "", false
	}
	
	server := s.servers[s.index]
	s.index = (s.index + 1) % s.count
	return server, true
}