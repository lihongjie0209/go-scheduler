package core

import (
	"strings"
	"testing"
)

func TestNormalizeCancelReason(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		input   string
		want    string
		wantErr bool
	}{
		{name: "default reason", want: "cancelled by operator"},
		{name: "trimmed reason", input: "  maintenance  ", want: "maintenance"},
		{name: "too long", input: strings.Repeat("x", 501), wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := normalizeCancelReason(tt.input)
			if (err != nil) != tt.wantErr {
				t.Fatalf("error = %v, wantErr %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Fatalf("reason = %q, want %q", got, tt.want)
			}
		})
	}
}
