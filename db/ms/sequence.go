package ms

// GetNextVal returns the next value from a sequence.
// Meilisearch does not support sequences; this is a no-op stub.
func (s *msSession) GetNextVal(sequenceName string) (uint64, error) {
	return 0, nil
}
