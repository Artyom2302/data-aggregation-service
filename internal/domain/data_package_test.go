package domain

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestNewDataPackage(t *testing.T) {
	tests := []struct {
		name    string
		numbers []int
		want    int
	}{
		{"max in middle", []int{3, 9, 1}, 9},
		{"max first", []int{9, 3, 1}, 9},
		{"max last", []int{1, 3, 9}, 9},
		{"single", []int{7}, 7},
		{"all negative", []int{-5, -2, -9}, -2},
		{"duplicates", []int{4, 4, 4}, 4},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw := RawPackage{
				ID:            uuid.New(),
				CreatedAt:     time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC),
				RandomNumbers: tt.numbers,
			}

			pkg, err := NewDataPackage(raw)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if pkg.Payload != tt.want {
				t.Errorf("payload: want %d, got %d", tt.want, pkg.Payload)
			}
			if pkg.ID != raw.ID {
				t.Errorf("id: want %s, got %s", raw.ID, pkg.ID)
			}
			if !pkg.CreatedAt.Equal(raw.CreatedAt) {
				t.Errorf("created_at: want %v, got %v", raw.CreatedAt, pkg.CreatedAt)
			}
		})
	}
}

func TestNewDataPackage_Empty(t *testing.T) {
	for name, numbers := range map[string][]int{
		"nil":   nil,
		"empty": {},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := NewDataPackage(RawPackage{RandomNumbers: numbers})
			if !errors.Is(err, ErrEmptyPackage) {
				t.Errorf("want ErrEmptyPackage, got %v", err)
			}
		})
	}
}
