package clock

import "time"

// UTC returns the current wall-clock time normalized to UTC.
func UTC() time.Time {
	return time.Now().UTC()
}
