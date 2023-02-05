package lsm

import (
	"github.com/vradovic/naisp-projekat/config"
	"testing"
)

func TestSizeTiered(t *testing.T) {
	config.Init()
	err := SizeTiered()
	if err != nil {
		t.Errorf("%v", err)
	}
}
