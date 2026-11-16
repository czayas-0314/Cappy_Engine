//go:build !darwin

package engine

// currentRSS is not implemented off macOS in the spike.
func currentRSS() int64 { return -1 }

func maxRSS() int64 { return -1 }
