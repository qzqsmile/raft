package main

import (
	"bytes"
	"fmt"
	"labgob"
)

func main() {
	s := (2+3) / 2
	fmt.Printf("%v", s)
}
func write(s []int) []byte{
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(1)
	e.Encode(2)
	e.Encode(1)
	e.Encode(2)
	e.Encode(s)
	data := w.Bytes()
	return data
}