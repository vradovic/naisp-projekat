package main

import (
	"github.com/vradovic/naisp-projekat/config"
	"github.com/vradovic/naisp-projekat/io"
	"github.com/vradovic/naisp-projekat/lsm"
	"github.com/vradovic/naisp-projekat/structures"
)

func main() {
	config.Init()
	structures.Init()
	err := io.Menu()
	if err != nil {
		panic("Greska")
	}

	lsm.LeveledCompaction()
}
