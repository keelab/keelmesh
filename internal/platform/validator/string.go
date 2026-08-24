package validator

import (
	"strings"

	"github.com/keelab/keelith/validation"
)

// RequiredString returns a Keelith validation error when value is blank. The
// rejected value is never retained in the violation or framework metadata.
func RequiredString(field string, value string) error {
	if strings.TrimSpace(value) != "" {
		return nil
	}
	validationError, err := validation.New(validation.Violation{
		Field:   field,
		Rule:    "required",
		Message: "must not be empty",
	})
	if err != nil {
		return err
	}
	return validation.FrameworkError(validationError)
}
