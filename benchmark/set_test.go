package benchmark

import (
	"testing"
)

func TestNewSet(t *testing.T) {
	s := NewSet()
	if s.Size() != 0 {
		t.Errorf("NewSet() error = %v, wantErr %v", s.Size(), 0)
	}
}

func TestAdd(t *testing.T) {
	s := NewSet()
	s.Add("element")
	if !s.Contains("element") {
		t.Errorf("Add() error, element not found")
	}
}

func TestRemove(t *testing.T) {
	s := NewSet()
	s.Add("element")
	s.Remove("element")
	if s.Contains("element") {
		t.Errorf("Remove() error, element found")
	}
}

func TestContains(t *testing.T) {
	s := NewSet()
	s.Add("element")
	if !s.Contains("element") {
		t.Errorf("Contains() error, element not found")
	}
}

func TestSize(t *testing.T) {
	s := NewSet()
	s.Add("element1")
	s.Add("element2")
	if s.Size() != 2 {
		t.Errorf("Size() error = %v, wantErr %v", s.Size(), 2)
	}
}
