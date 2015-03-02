package main

import (
    "os"
    "io"
)

var HEAD = []byte(`
package main

import (
    "net/http"
)

func returnIndex(rw http.ResponseWriter, req *http.Request) {
    rw.Header().Set("Content-Type", "text/html")
    rw.Write([]byte(`)

var TAIL = []byte(`))
}`)

func main() {
    of,err := os.Create("static.go")
    if err!=nil {
        panic("??")
    }
    defer of.Close()
    src,err := os.Open("index.html")
    if err!=nil {
        panic("??")
    }
    defer src.Close()
    
    of.Write(HEAD)
    of.WriteString("`")
    io.Copy(of, src)
    of.WriteString("`")
    of.Write(TAIL)
    
}
