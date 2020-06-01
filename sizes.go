package main

import "fmt"

const (
	KiB = 1 << 10
	MiB = 1 << 20
	GiB = 1 << 30
	TiB = 1 << 40

	KB = 1e3
	MB = 1e6
	GB = 1e9
	TB = 1e12
)

func humanizeSize(f float64, base2 bool) string {

	if base2 {
		switch {
		case f > TiB:
			return fmt.Sprintf("%0.2f TB", f/TiB)
		case f > GiB:
			return fmt.Sprintf("%0.2f GB", f/GiB)
		case f > MiB:
			return fmt.Sprintf("%0.2f MB", f/MiB)
		case f > KiB:
			return fmt.Sprintf("%0.2f KB", f/KiB)
		}
	} else {
		switch {
		case f > TB:
			return fmt.Sprintf("%0.2f TB", f/TB)
		case f > GB:
			return fmt.Sprintf("%0.2f GB", f/GB)
		case f > MB:
			return fmt.Sprintf("%0.2f MB", f/MB)
		case f > KB:
			return fmt.Sprintf("%0.2f KB", f/KB)
		}
	}

	// Whether we want base 10 or base 2, bytes are bytes.
	return fmt.Sprintf("%0.0f bytes", f)
}

