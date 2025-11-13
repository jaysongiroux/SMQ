package config

import (
	"fmt"
	"strings"
	"unicode"
)

// Helper functions for common validations
func ValidatePositive(name string, value int) error {
	if value <= 0 {
		return fmt.Errorf("%s must be greater than 0 (got %d)", name, value)
	}
	return nil
}

func ValidateNonNegative(name string, value int) error {
	if value < 0 {
		return fmt.Errorf("%s must be >= 0 (got %d)", name, value)
	}
	return nil
}

func ValidatePort(name string, port int) error {
	if port <= 0 || port > 65535 {
		return fmt.Errorf("%s must be a valid port (1-65535, got %d)", name, port)
	}
	return nil
}

func ValidateMinValue(name string, value, minValue int) error {
	if value < minValue {
		return fmt.Errorf("%s must be greater than %d (got %d)", name, minValue, value)
	}
	return nil
}

func ValidateMaxValue(name string, value, maxValue int) error {
	if value > maxValue {
		return fmt.Errorf("%s must be less than %d (got %d)", name, maxValue, value)
	}
	return nil
}

func ValidateRange(name string, value, min, max int) error {
	if value < min || value > max {
		return fmt.Errorf("%s must be between %d and %d (got %d)", name, min, max, value)
	}
	return nil
}

func ValidateNonEmpty(name, value string) error {
	if value == "" {
		return fmt.Errorf("%s is not configured (must be set via environment variable)", name)
	}
	return nil
}

func ValidateOneOf(name, value string, valid []string) error {
	for _, v := range valid {
		if value == v {
			return nil
		}
	}
	return fmt.Errorf("%s must be one of: %s (got %s)", name, strings.Join(valid, ", "), value)
}

func ValidateBoolean(name, value string) error {
	if value != "true" && value != "false" {
		return fmt.Errorf("%s must be a boolean (true or false)", name)
	}
	return nil
}

func ValidateApiKey(name, value string) error {
	genericErrorMessage := `
		Please use a password generator or UUID at the very least.
		To generate a password, visit: https://www.passwordgenerator.net/
		To generate a UUID, visit: https://www.uuidgenerator.net/
		or use a CLI tool like ssh-keygen
	`

	minApiKeyLength := 12
	if len(value) < minApiKeyLength {
		return fmt.Errorf("%s must be at least %d characters long. %s", name, minApiKeyLength, genericErrorMessage)
	}

	// check variety of letters and numbers
	hasLetters := false
	hasNumbers := false
	for _, char := range value {
		if unicode.IsLetter(char) {
			hasLetters = true
		}
		if unicode.IsNumber(char) {
			hasNumbers = true
		}
	}
	if !hasLetters || !hasNumbers {
		return fmt.Errorf("%s must contain both letters and numbers. %s", name, genericErrorMessage)
	}
	return nil
}
