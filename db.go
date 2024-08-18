package bolt

import (
	"io"
	"os"
	"path/filepath"

	"go.etcd.io/bbolt"
)

var (
	ErrBucketNotFound     = bbolt.ErrBucketNotFound
	ErrBucketExists       = bbolt.ErrBucketExists
	ErrBucketNameRequired = bbolt.ErrBucketNameRequired
	ErrKeyRequired        = bbolt.ErrKeyRequired
)

func Open(path string) DB {
	bolt, err := bbolt.Open(path, 0600, nil)
	return &boltDB{bolt: bolt, err: err}
}

func From(bolt *bbolt.DB) DB {
	return &boltDB{bolt: bolt}
}

type DB interface {
	View(fn func(tx Tx) error) error
	Update(fn func(tx Tx) error) error
	Batch(fn func(tx Tx) error) error
	Close() error
	Path() string

	Bolt() (*bbolt.DB, error)
	Copy(w io.Writer) (err error)
	CopyFile(path string, mode os.FileMode) error
}

var _ boltDB

type boltDB struct {
	bolt *bbolt.DB
	err  error
}

func (b *boltDB) Path() string {
	if b.bolt != nil {
		return filepath.ToSlash(b.bolt.Path())
	}
	return ""
}

func (b *boltDB) Copy(w io.Writer) (err error) {
	return b.bolt.View(func(tx *bbolt.Tx) (err error) { return tx.Copy(w) })
}

func (b *boltDB) CopyFile(path string, mode os.FileMode) error {
	return b.bolt.View(func(tx *bbolt.Tx) error { return tx.CopyFile(path, mode) })
}

func (b *boltDB) View(fn func(tx Tx) error) error {
	if b.err != nil {
		return b.err
	}
	return b.bolt.View(func(t *bbolt.Tx) error { return fn(&bucket{db: b, tx: t}) })
}

func (b *boltDB) Update(fn func(tx Tx) error) error {
	if b.err != nil {
		return b.err
	}
	return b.bolt.Update(func(t *bbolt.Tx) error { return fn(&bucket{db: b, tx: t}) })
}

func (b *boltDB) Batch(fn func(tx Tx) error) error {
	if b.err != nil {
		return b.err
	}
	return b.bolt.Batch(func(t *bbolt.Tx) error { return fn(&bucket{db: b, tx: t}) })
}

func (b *boltDB) Close() error {
	if b.err != nil {
		return b.bolt.Close()
	}
	return nil
}

func (b *boltDB) Bolt() (db *bbolt.DB, err error) {
	if err = b.err; err != nil {
		return
	}
	db = b.bolt
	return
}
