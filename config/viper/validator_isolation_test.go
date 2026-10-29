package viper_test

import (
	"context"
	"testing"

	"github.com/nostosx/servex-contrib/config/viper"
	"github.com/nostosx/servex/config"
)

func TestValidatorSnapshotAssignmentDoesNotAffectOtherValidators(t *testing.T) {
	c, err := viper.New(t.Context(), []config.Source{src(map[string]any{"port": 8080})},
		viper.WithValidator(func(_ context.Context, candidate config.Reader) error {
			// Replacing the caller-owned view must not replace a later validator's view.
			view, ok := candidate.(*viper.Snapshot)
			if !ok {
				t.Fatalf("candidate type = %T", candidate)
			}
			*view = viper.Snapshot{}
			if candidate.Int("port") != 0 {
				t.Fatal("replacement was not empty")
			}
			return nil
		}),
		viper.WithValidator(func(_ context.Context, candidate config.Reader) error {
			if candidate.Int("port") != 8080 {
				t.Error("validator received a replaced snapshot")
			}
			return nil
		}))
	if err != nil {
		t.Fatal(err)
	}
	if c.Int("port") != 8080 {
		t.Fatal("validator replaced the published tree")
	}
}
