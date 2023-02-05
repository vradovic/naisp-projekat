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
<<<<<<< HEAD

	lsm.LeveledCompaction()
=======
>>>>>>> 0420c7af431d4d640e2692f3d5c97f7adac11060
}
