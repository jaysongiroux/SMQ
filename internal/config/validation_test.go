package config

import (
	"strings"
	"testing"
)

func TestValidatePositive(t *testing.T) {
	tests := []struct {
		name      string
		value     int
		expectErr bool
	}{
		{"positive value", 10, false},
		{"one", 1, false},
		{"zero", 0, true},
		{"negative", -5, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidatePositive("test_field", tt.value)
			if (err != nil) != tt.expectErr {
				t.Errorf("ValidatePositive(%d) error = %v, expectErr %v", tt.value, err, tt.expectErr)
			}
			if err != nil && !strings.Contains(err.Error(), "test_field") {
				t.Errorf("Error should contain field name, got: %v", err)
			}
		})
	}
}

func TestValidateNonNegative(t *testing.T) {
	tests := []struct {
		name      string
		value     int
		expectErr bool
	}{
		{"positive", 10, false},
		{"zero", 0, false},
		{"negative", -1, true},
		{"large negative", -100, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateNonNegative("test_field", tt.value)
			if (err != nil) != tt.expectErr {
				t.Errorf("ValidateNonNegative(%d) error = %v, expectErr %v", tt.value, err, tt.expectErr)
			}
		})
	}
}

func TestValidatePort(t *testing.T) {
	tests := []struct {
		name      string
		port      int
		expectErr bool
	}{
		{"valid port 80", 80, false},
		{"valid port 443", 443, false},
		{"valid port 8080", 8080, false},
		{"valid port 1", 1, false},
		{"valid port 65535", 65535, false},
		{"zero port", 0, true},
		{"negative port", -1, true},
		{"port too high", 65536, true},
		{"port way too high", 100000, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidatePort("port", tt.port)
			if (err != nil) != tt.expectErr {
				t.Errorf("ValidatePort(%d) error = %v, expectErr %v", tt.port, err, tt.expectErr)
			}
			if err != nil && !strings.Contains(err.Error(), "1-65535") {
				t.Errorf("Error should mention valid port range, got: %v", err)
			}
		})
	}
}

func TestValidateMinValue(t *testing.T) {
	tests := []struct {
		name      string
		value     int
		minValue  int
		expectErr bool
	}{
		{"value equals min", 10, 10, false},
		{"value above min", 15, 10, false},
		{"value below min", 5, 10, true},
		{"negative values", -5, -10, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateMinValue("test_field", tt.value, tt.minValue)
			if (err != nil) != tt.expectErr {
				t.Errorf("ValidateMinValue(%d, %d) error = %v, expectErr %v",
					tt.value, tt.minValue, err, tt.expectErr)
			}
		})
	}
}
func TestValidateMaxValue(t *testing.T) {
	tests := []struct {
		name      string
		value     int
		maxValue  int
		expectErr bool
	}{
		{"value equals max", 10, 10, false},
		{"value below max", 5, 10, false},
		{"value above max", 15, 10, true},
		{"negative value below max", -15, -10, false}, // -15 < -10, so valid
		{"negative value above max", -5, -10, true},   // -5 > -10, so invalid
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateMaxValue("test_field", tt.value, tt.maxValue)
			if (err != nil) != tt.expectErr {
				t.Errorf("ValidateMaxValue(%d, %d) error = %v, expectErr %v",
					tt.value, tt.maxValue, err, tt.expectErr)
			}
		})
	}
}
func TestValidateRange(t *testing.T) {
	tests := []struct {
		name      string
		value     int
		min       int
		max       int
		expectErr bool
	}{
		{"value in range", 50, 1, 100, false},
		{"value at min", 1, 1, 100, false},
		{"value at max", 100, 1, 100, false},
		{"value below range", 0, 1, 100, true},
		{"value above range", 101, 1, 100, true},
		{"negative range", -5, -10, 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateRange("test_field", tt.value, tt.min, tt.max)
			if (err != nil) != tt.expectErr {
				t.Errorf("ValidateRange(%d, %d, %d) error = %v, expectErr %v",
					tt.value, tt.min, tt.max, err, tt.expectErr)
			}
			if err != nil && !strings.Contains(err.Error(), "between") {
				t.Errorf("Error should mention range, got: %v", err)
			}
		})
	}
}

func TestValidateNonEmpty(t *testing.T) {
	tests := []struct {
		name      string
		value     string
		expectErr bool
	}{
		{"non-empty string", "value", false},
		{"single character", "a", false},
		{"whitespace", "  ", false}, // Whitespace is not empty
		{"empty string", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateNonEmpty("test_field", tt.value)
			if (err != nil) != tt.expectErr {
				t.Errorf("ValidateNonEmpty(%q) error = %v, expectErr %v", tt.value, err, tt.expectErr)
			}
			if err != nil && !strings.Contains(err.Error(), "environment variable") {
				t.Errorf("Error should mention environment variable, got: %v", err)
			}
		})
	}
}

func TestValidateOneOf(t *testing.T) {
	validOptions := []string{"memory", "disk", "redis"}

	tests := []struct {
		name      string
		value     string
		expectErr bool
	}{
		{"valid option memory", "memory", false},
		{"valid option disk", "disk", false},
		{"valid option redis", "redis", false},
		{"invalid option", "invalid", true},
		{"empty string", "", true},
		{"case sensitive", "Memory", true}, // Assuming case-sensitive
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateOneOf("buffer_type", tt.value, validOptions)
			if (err != nil) != tt.expectErr {
				t.Errorf("ValidateOneOf(%q) error = %v, expectErr %v", tt.value, err, tt.expectErr)
			}
			if err != nil {
				if !strings.Contains(err.Error(), "memory") {
					t.Errorf("Error should list valid options, got: %v", err)
				}
			}
		})
	}
}

func TestValidateBoolean(t *testing.T) {
	tests := []struct {
		name      string
		value     string
		expectErr bool
	}{
		{"true", "true", false},
		{"false", "false", false},
		{"True (capitalized)", "True", true},
		{"FALSE (capitalized)", "FALSE", true},
		{"1", "1", true},
		{"0", "0", true},
		{"yes", "yes", true},
		{"no", "no", true},
		{"empty", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateBoolean("test_bool", tt.value)
			if (err != nil) != tt.expectErr {
				t.Errorf("ValidateBoolean(%q) error = %v, expectErr %v", tt.value, err, tt.expectErr)
			}
			if err != nil && !strings.Contains(err.Error(), "boolean") {
				t.Errorf("Error should mention boolean, got: %v", err)
			}
		})
	}
}

func TestValidateApiKey(t *testing.T) {
	tests := []struct {
		name      string
		value     string
		expectErr bool
		errMsg    string
	}{
		{
			name:      "valid key with letters and numbers",
			value:     "abc123def456",
			expectErr: false,
		},
		{
			name:      "valid UUID format",
			value:     "550e8400-e29b-41d4-a716-446655440000",
			expectErr: false,
		},
		{
			name:      "valid long key",
			value:     "MySecurePassword123456",
			expectErr: false,
		},
		{
			name:      "too short",
			value:     "short1",
			expectErr: true,
			errMsg:    "at least 12 characters",
		},
		{
			name:      "exactly 12 chars valid",
			value:     "pass12345678",
			expectErr: false,
		},
		{
			name:      "only letters",
			value:     "onlylettershere",
			expectErr: true,
			errMsg:    "letters and numbers",
		},
		{
			name:      "only numbers",
			value:     "123456789012",
			expectErr: true,
			errMsg:    "letters and numbers",
		},
		{
			name:      "empty string",
			value:     "",
			expectErr: true,
			errMsg:    "at least 12 characters",
		},
		{
			name:      "special characters with letters and numbers",
			value:     "Pass123!@#$%",
			expectErr: false,
		},
		{
			name:      "only special characters",
			value:     "!@#$%^&*()_+",
			expectErr: true,
			errMsg:    "letters and numbers",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateApiKey("api_key", tt.value)
			if (err != nil) != tt.expectErr {
				t.Errorf("ValidateApiKey(%q) error = %v, expectErr %v", tt.value, err, tt.expectErr)
			}
			if err != nil && tt.errMsg != "" {
				if !strings.Contains(err.Error(), tt.errMsg) {
					t.Errorf("Error should contain %q, got: %v", tt.errMsg, err)
				}
			}
			if err != nil && !strings.Contains(err.Error(), "passwordgenerator") {
				t.Errorf("Error should contain helpful URL, got: %v", err)
			}
		})
	}
}

func TestValidationErrorMessages(t *testing.T) {
	t.Run("error messages contain field name", func(t *testing.T) {
		err := ValidatePositive("my_field", -1)
		if err == nil {
			t.Fatal("Expected error")
		}
		if !strings.Contains(err.Error(), "my_field") {
			t.Errorf("Error should contain field name 'my_field', got: %v", err)
		}
	})

	t.Run("error messages contain actual value", func(t *testing.T) {
		err := ValidateRange("count", 150, 1, 100)
		if err == nil {
			t.Fatal("Expected error")
		}
		if !strings.Contains(err.Error(), "150") {
			t.Errorf("Error should contain actual value '150', got: %v", err)
		}
	})

	t.Run("range error shows min and max", func(t *testing.T) {
		err := ValidateRange("value", 50, 10, 20)
		if err == nil {
			t.Fatal("Expected error")
		}
		if !strings.Contains(err.Error(), "10") || !strings.Contains(err.Error(), "20") {
			t.Errorf("Error should show range bounds, got: %v", err)
		}
	})
}

func TestValidateApiKeyEdgeCases(t *testing.T) {
	t.Run("mixed case letters with numbers", func(t *testing.T) {
		err := ValidateApiKey("key", "AbCdEf123456")
		if err != nil {
			t.Errorf("Expected valid key, got error: %v", err)
		}
	})

	t.Run("numbers at different positions", func(t *testing.T) {
		keys := []string{
			"1abcdefghijk", // Number at start
			"abcdefg1hijk", // Number in middle
			"abcdefghijk1", // Number at end
		}
		for _, key := range keys {
			err := ValidateApiKey("key", key)
			if err != nil {
				t.Errorf("Key %q should be valid, got error: %v", key, err)
			}
		}
	})

	t.Run("unicode characters", func(t *testing.T) {
		// Unicode letters should count as letters
		err := ValidateApiKey("key", "café1234café")
		if err != nil {
			t.Errorf("Expected unicode letters to be valid, got: %v", err)
		}
	})
}
