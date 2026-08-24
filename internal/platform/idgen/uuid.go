package idgen

import "github.com/google/uuid"

// UUID returns a collision-resistant UUID suitable for persistent records.
func UUID() string {
	return uuid.NewString()
}
