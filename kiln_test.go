package kiln

import "testing"

func TestKilnOpenClose(t *testing.T) {
	db, err := Open("test_db.kiln")
	if err != nil {
		t.Fatalf("Failed to open kiln database: %v", err)
	}
	if db == nil {
		t.Fatal("Expected non-nil Engine, got nil")
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Failed to close kiln database: %v", err)
	}
}
