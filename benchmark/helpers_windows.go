//go:build windows
// +build windows

package benchmark

// adjustFilenoUlimit adjusts file descriptor limits on Windows (no-op)
func (b *Benchmark) adjustFilenoUlimit() int {
	return 0
}
