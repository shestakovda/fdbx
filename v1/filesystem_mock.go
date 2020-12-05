package fdbx

import "net/http"

type mockFileSystem struct {
	*MockConn
}

func (w *mockFileSystem) Open(name string) (_ http.File, err error) {
	return w.FileSystemOpen(name)
}

func (w *mockFileSystem) Save(name string, data []byte) (err error) {
	return w.FileSystemSave(name, data)
}
