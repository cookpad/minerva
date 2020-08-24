package merger_test

import (
	"log"
	"os"
)

func mustOpen(fpath string) *os.File {
	rfd, err := os.Open(fpath)
	if err != nil {
		log.Fatal(err)
	}
	return rfd
}
