package lsm

import (
	"testing"

	"github.com/vradovic/naisp-projekat/config"
)

func TestSizeTiered(t *testing.T) {
	config.Init()
	err := SizeTiered()
	if err != nil {
		t.Errorf("%v", err)
	}
}
