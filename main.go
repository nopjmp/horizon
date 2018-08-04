package main

import (
	"github.com/nopjmp/horizon/haystack"
	"io"
	"os"
)

func main() {
	hs, err := haystack.NewHaystack("data.db")
	if err != nil {
		panic(err)
		return
	}

	f, err := os.Open("test.txt")
	//f, err := os.Create("test2.txt")
	//n, err := hs.Find("test.txt")
	//io.Copy(f, n)
	stat, _ := f.Stat()
	n, err := hs.NewNeedle("test.txt", stat.Size())
	if err != nil {
		panic(err)
		return
	}
	io.Copy(n, f)
	n.Finalize()
}
