//go:build darwin || linux
// +build darwin linux

package benchmark

import (
	"os/exec"
	"runtime"
	"syscall"
	"testing"

	"github.com/acronis/perfkit/logger"
)

type LogLevel int

func TestAdjustFilenoUlimit(t *testing.T) {
	b := &Benchmark{
		Logger: logger.NewPlaneLogger(logger.LevelDebug, true),
	}

	// Save original limits to restore after test
	var origLimit syscall.Rlimit
	err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &origLimit)
	if err != nil {
		t.Fatalf("Failed to get original rlimit: %v", err)
	}

	// Run the function
	result := b.adjustFilenoUlimit()

	// Check result
	if result != 0 {
		t.Errorf("Expected adjustFilenoUlimit to return 0, got %d", result)
	}

	// Verify the limit was changed
	var newLimit syscall.Rlimit
	err = syscall.Getrlimit(syscall.RLIMIT_NOFILE, &newLimit)
	if err != nil {
		t.Fatalf("Failed to get new rlimit: %v", err)
	}

	// The function sets both Cur and Max to 1048576
	expectedLimit := uint64(1048576)
	if newLimit.Cur != expectedLimit {
		t.Errorf("Expected current limit to be %d, got %d", expectedLimit, newLimit.Cur)
	}

	// Restore original limits
	err = syscall.Setrlimit(syscall.RLIMIT_NOFILE, &origLimit)
	if err != nil {
		t.Logf("Warning: Failed to restore original rlimit: %v", err)
	}
}

func TestGetSysctlValueInt(t *testing.T) {
	// Skip if sysctl command is not available
	_, err := exec.LookPath("sysctl")
	if err != nil {
		t.Skip("sysctl command not available, skipping test")
	}

	type sysctlTest struct {
		name    string
		key     string
		wantErr bool
	}

	tests := []sysctlTest{
		{
			name:    "Invalid key",
			key:     "invalid.nonexistent.key",
			wantErr: true,
		},
	}

	if runtime.GOOS == "darwin" {
		tests = append(tests, sysctlTest{
			name:    "Valid key",
			key:     "kern.ipc.somaxconn", // This should exist on both Darwin and Linux
			wantErr: false,
		})
	} else if runtime.GOOS == "linux" {
		tests = append(tests, sysctlTest{
			name:    "Valid key",
			key:     "net.core.somaxconn", // This should exist on both Darwin and Linux
			wantErr: false,
		})
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := GetSysctlValueInt(tt.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetSysctlValueInt() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}

	// Test with a key that should return an integer value
	// This is platform-specific, so we'll try a few common ones
	intKeys := []string{
		"hw.ncpu",            // Darwin
		"hw.physicalcpu",     // Darwin
		"kern.maxfiles",      // Darwin
		"kernel.pid_max",     // Linux
		"kernel.threads-max", // Linux
	}

	foundValidKey := false
	for _, key := range intKeys {
		val, err := GetSysctlValueInt(key)
		if err == nil {
			foundValidKey = true
			if val <= 0 {
				t.Errorf("Expected positive integer for %s, got %d", key, val)
			}
			break
		}
	}

	if !foundValidKey {
		t.Log("Could not find a valid integer sysctl key for testing positive case")
	}
}
