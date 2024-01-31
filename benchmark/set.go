package benchmark

// Set data structure using map
type Set struct {
	elements map[interface{}]bool
}

// NewSet creates a new Set
func NewSet() *Set {
	return &Set{
		elements: make(map[interface{}]bool),
	}
}

// Add an element to the set
func (s *Set) Add(element interface{}) {
	s.elements[element] = true
}

// Remove an element from the set
func (s *Set) Remove(element interface{}) {
	delete(s.elements, element)
}

// Contains checks if an element is in the set
func (s *Set) Contains(element interface{}) bool {
	_, exists := s.elements[element]

	return exists
}

// Size returns the number of elements in the set
func (s *Set) Size() int {
	return len(s.elements)
}
