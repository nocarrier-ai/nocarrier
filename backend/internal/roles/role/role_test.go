package role

import (
	"slices"
	"testing"
)

func TestParse(t *testing.T) {
	tests := []struct {
		in      string
		want    []string
		wantErr bool
	}{
		{in: "all", want: Order},
		{in: "decide,sim", want: []string{Sim, Decide}},
		{in: " SIM , sim,query ", want: []string{Sim, DataRead}},
		{in: "all,sim", want: Order},
		{in: "", wantErr: true},
		{in: " , ", wantErr: true},
		{in: "sim,gateway", wantErr: true},
	}
	for _, tt := range tests {
		got, err := Parse(tt.in)
		if tt.wantErr {
			if err == nil {
				t.Errorf("Parse(%q) = %v, want error", tt.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("Parse(%q) unexpected error: %v", tt.in, err)
			continue
		}
		if !slices.Equal(got, tt.want) {
			t.Errorf("Parse(%q) = %v, want %v", tt.in, got, tt.want)
		}
	}
}
