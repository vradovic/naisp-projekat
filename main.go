package main

import (
	"github.com/vradovic/naisp-projekat/io"
	"github.com/vradovic/naisp-projekat/structures"
)

func main() {
	structures.Init()
	err := io.Menu()
	if err != nil {
		panic("Greska")
	}
}
