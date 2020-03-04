package fdbx

import (
	"bytes"
	"io"
	"net/http"
	"os"
	"path"
	"strings"
	"time"

	"github.com/golang/glog"

	fbs "github.com/google/flatbuffers/go"
	"github.com/shestakovda/fdbx/models"
)

const (
	verFileV1     uint8 = 1
	verFileLatest uint8 = verFileV1

	fileCursorPage = 1000
)

func newV610FileSystem(conn *v610Conn, typeID uint16) *v610fileSystem {
	return &v610fileSystem{
		conn: conn,
		tpid: typeID,
	}
}

type v610fileSystem struct {
	tpid uint16
	conn *v610Conn
}

func (fs *v610fileSystem) clean(name string) string {
	return "/" + strings.Trim(path.Clean(name), "./")
}

func (fs *v610fileSystem) Open(name string) (_ http.File, err error) {
	var rec Record

	name = fs.clean(name)

	glog.Errorf("open name = %s", name)

	if rec, err = fs.newFile(verFileV1, name); err != nil {
		return
	}

	if err = fs.conn.Tx(func(db DB) (exp error) { return db.Load(nil, rec) }); err != nil {
		return
	}

	return rec.(http.File), nil
}

func (fs *v610fileSystem) Save(name string, data []byte) (err error) {
	name = fs.clean(name)
	return fs.conn.Tx(func(db DB) error {

		glog.Errorf("save name = %s", name)

		return db.Save(nil, &v610file{
			path:  name,
			data:  data,
			mtime: time.Now(),
		})
	})
}

func (fs *v610fileSystem) newFile(ver uint8, id string) (Record, error) {
	return &v610file{
		path:   id,
		system: fs,
	}, nil
}

type v610file struct {
	path   string
	data   []byte
	mtime  time.Time
	cursor Cursor
	stream *bytes.Reader
	system *v610fileSystem
}

// http.File -> io.Closer
func (f *v610file) Close() (err error) {
	if f.cursor == nil {
		return nil
	}

	return f.cursor.Close()
}

// http.File -> io.Reader
func (f *v610file) Read(p []byte) (n int, err error) { return f.stream.Read(p) }

// http.File -> io.Seeker
func (f *v610file) Seek(offset int64, whence int) (int64, error) { return f.stream.Seek(offset, whence) }

// http.File
func (f *v610file) Readdir(count int) (list []os.FileInfo, err error) {
	page := fileCursorPage
	conn := f.system.conn
	list = make([]os.FileInfo, 0, count)

	if count > 0 {
		page = count
	}

	// Refill the cursor if necessary
	if f.cursor != nil && f.cursor.Empty() {
		if err = f.cursor.Close(); err != nil {
			return
		}
		f.cursor = nil
	}

	if f.cursor == nil {
		query := f.system.clean(path.Dir(f.path)) + "/"

		if query == "//" {
			query = "/"
		}

		glog.Errorf("query name = %s", query)
		if f.cursor, err = conn.Cursor(f.FdbxType(), Query(S2B(query))); err != nil {
			return
		}
	}

	if err = f.cursor.ApplyOpts(Page(uint(page))); err != nil {
		return
	}

	load := func(cnt int) (wtf error) {
		recs := make([]Record, 0, cnt)

		if wtf = conn.Tx(func(db DB) (exp error) { recs, exp = f.cursor.Next(db, 0); return }); wtf != nil {
			return
		}

		for i := range recs {
			list = append(list, recs[i].(os.FileInfo))
		}

		return nil
	}

	if count > 0 {
		if err = load(count); err != nil {
			return
		}

		if f.cursor.Empty() {
			return list, io.EOF
		}

		return list, nil
	}

	for !f.cursor.Empty() {
		if err = load(fileCursorPage); err != nil {
			return
		}
	}

	return list, nil
}

// http.File
func (f *v610file) Stat() (os.FileInfo, error) { return f, nil }

// os.FileInfo
func (f *v610file) Name() string { return path.Base(f.path) }

// os.FileInfo
func (f *v610file) Size() int64 { return int64(len(f.data)) }

// os.FileInfo
func (f *v610file) Mode() os.FileMode { return os.FileMode(0644) }

// os.FileInfo
func (f *v610file) ModTime() time.Time { return f.mtime }

// os.FileInfo
func (f *v610file) IsDir() bool { return false }

// os.FileInfo
func (f *v610file) Sys() interface{} { return f.system }

func (f *v610file) FdbxID() string { return f.path }

func (f *v610file) FdbxType() RecordType {
	return RecordType{ID: FilesTypeID, Ver: verFileV1, New: f.system.newFile}
}

func (f *v610file) FdbxIndex(idx Indexer) error { return nil }

func (f *v610file) FdbxMarshal() ([]byte, error) {
	buf := fbs.NewBuilder(64 + len(f.path) + len(f.data))

	pathOffset := buf.CreateString(f.path)
	dataOffset := buf.CreateByteVector(f.data)

	models.FileStart(buf)
	models.FileAddPath(buf, pathOffset)
	models.FileAddData(buf, dataOffset)
	if !f.mtime.IsZero() {
		models.FileAddMTime(buf, uint64(f.mtime.UTC().UnixNano()))
	}
	buf.Finish(models.FileEnd(buf))
	return buf.FinishedBytes(), nil
}

func (f *v610file) FdbxUnmarshal(buf []byte) error {
	model := models.GetRootAsFile(buf, 0)

	f.path = B2S(model.Path())
	f.data = model.DataBytes()
	f.stream = bytes.NewReader(f.data)

	if mtime := model.MTime(); mtime > 0 {
		f.mtime = time.Unix(0, int64(mtime))
	}

	return nil
}
