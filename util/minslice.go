package util

// Linearna pretraga najmanjeg elementa u slice-u
func MinSliceUnsigned(arr []uint64) uint64 {
	minEl := arr[0]

	for _, el := range arr {
		if el < minEl {
			minEl = el
		}
	}

	return minEl
}
