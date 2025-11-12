package models

import (
	"encoding/json"
	"testing"
	"time"
)

func TestUnixTime_UnmarshalJSON(t *testing.T) {
	tests := []struct {
		name      string
		jsonInput string
		wantUnix  int64
		wantErr   bool
	}{
		{
			name:      "unix timestamp as integer",
			jsonInput: `1609459200`,
			wantUnix:  1609459200,
			wantErr:   false,
		},
		{
			name:      "RFC3339 string",
			jsonInput: `"2021-01-01T00:00:00Z"`,
			wantUnix:  1609459200,
			wantErr:   false,
		},
		{
			name:      "invalid format",
			jsonInput: `"invalid"`,
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var ut UnixTime
			err := json.Unmarshal([]byte(tt.jsonInput), &ut)

			if (err != nil) != tt.wantErr {
				t.Errorf("UnmarshalJSON() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr && ut.Unix() != tt.wantUnix {
				t.Errorf("UnmarshalJSON() got = %v, want %v", ut.Unix(), tt.wantUnix)
			}
		})
	}
}

func TestUnixTime_MarshalJSON(t *testing.T) {
	testTime := time.Unix(1609459200, 0)
	ut := UnixTime{Time: testTime}

	got, err := json.Marshal(ut)
	if err != nil {
		t.Fatalf("MarshalJSON() error = %v", err)
	}

	want := `1609459200`
	if string(got) != want {
		t.Errorf("MarshalJSON() got = %s, want %s", got, want)
	}
}
