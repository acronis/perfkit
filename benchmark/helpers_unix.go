//go:build darwin || linux
// +build darwin linux

package benchmark

import (
	"bytes"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
)

// adjustFilenoUlimit adjusts file descriptor limits on Linux and Darwin
func (b *Benchmark) adjustFilenoUlimit() int {
	var rLimit syscall.Rlimit
	fileno := uint64(1048576)

	err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	if err != nil {
		fmt.Println("Error Getting Rlimit ", err)

		return -1
	}

	rLimit.Max = fileno
	rLimit.Cur = fileno

	err = syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	if err != nil {
		fmt.Println("Error Setting Rlimit ", err)

		return -1
	}

	err = syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	if err != nil {
		fmt.Println("Error Getting Rlimit ", err)

		return -1
	}

	b.Logger.Debug(fmt.Sprintf("Changing file descriptor limits to: %v", rLimit))

	return 0
}

// GetSysctlValueInt returns int64 value of given sysctl key
func GetSysctlValueInt(key string) (int64, error) {
	cmd := exec.Command("sysctl", "-n", key)
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	if err != nil {
		return 0, fmt.Errorf("error running sysctl: %w", err)
	}

	val, err := strconv.ParseInt(strings.TrimSpace(out.String()), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("error parsing sysctl value: %w", err)
	}

	return val, nil
}
