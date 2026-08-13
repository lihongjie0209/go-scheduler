package core

import "testing"

func TestNormalizeDependencyIDs(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		input   []string
		want    []string
		wantErr bool
	}{
		{name: "trim and deduplicate", input: []string{" child-b ", "child-a", "child-b"}, want: []string{"child-a", "child-b"}},
		{name: "reject empty", input: []string{"child-a", " "}, wantErr: true},
		{name: "reject too many", input: make([]string, 101), wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := normalizeDependencyIDs(tt.input)
			if (err != nil) != tt.wantErr {
				t.Fatalf("error = %v, wantErr %v", err, tt.wantErr)
			}
			if len(got) != len(tt.want) {
				t.Fatalf("ids = %v, want %v", got, tt.want)
			}
			for index := range tt.want {
				if got[index] != tt.want[index] {
					t.Fatalf("ids = %v, want %v", got, tt.want)
				}
			}
		})
	}
}
