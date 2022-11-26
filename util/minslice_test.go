package util

import "testing"

func TestMinSliceUnsigned(t *testing.T) {
	arr := []uint64{46, 78, 67, 10032, 505, 23, 2032, 1056}

	got := MinSliceUnsigned(arr)
	var want uint64 = 23

	if got != want {
		t.Errorf("Got %d, want %d", got, want)
	}
}
