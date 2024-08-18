package bolt

import (
	"bytes"
	"io"
	"io/fs"

	"go.etcd.io/bbolt"
)

type Tx interface {
	CreateBucket(path [][]byte) (err error)
	Set(keyPath [][]byte, value []byte) (err error)
	Get(keyPath [][]byte) (value []byte, err error)
	Del(keyPath [][]byte) (err error)
	Scan(keyPath [][]byte, scan []byte, fn func(k, v []byte, bucket bool) (err error)) (err error)
	Err() error
}

type bucket struct {
	db  *boltDB
	tx  *bbolt.Tx
	err error
}

func (b *bucket) DB() DB { return b.db }

func (b *bucket) Err() error { return b.err }

func (b *bucket) getBucket(bucketPath [][]byte, create bool, err *error) (n *bbolt.Bucket) {
	if *err = b.err; *err != nil {
		return
	}

	if len(bucketPath) == 0 {
		*err = ErrBucketNameRequired
		return
	}

	for i, name := range bucketPath {
		if create {
			if i == 0 {
				n, *err = b.tx.CreateBucketIfNotExists(name)
			} else {
				n, *err = n.CreateBucketIfNotExists(name)
			}
		} else {
			if i == 0 {
				n = b.tx.Bucket(name)
			} else {
				n = n.Bucket(name)
			}
		}

		if *err != nil {
			return
		}

		if n == nil {
			*err = ErrBucketNotFound
			return
		}
	}

	return
}

func (b *bucket) bucketKeySplit(keyPath [][]byte, create bool, err *error) (bucket *bbolt.Bucket, key []byte) {
	bucketPath, key := keyPath[:len(keyPath)-1], keyPath[len(keyPath)-1]
	bucket = b.getBucket(bucketPath, create, err)
	return
}

func (b *bucket) CreateBucket(path [][]byte) (err error) {
	b.getBucket(path, true, &err)
	return
}

func (b *bucket) Set(keyPath [][]byte, value []byte) (err error) {
	if err = b.err; err != nil {
		return
	}

	switch len(keyPath) {
	case 0:
		err = ErrKeyRequired
	case 1:
		err = ErrBucketNameRequired
	default:
		if bucket, key := b.bucketKeySplit(keyPath, true, &err); err == nil {
			err = bucket.Put(key, value)
		}
	}
	return
}

func (b *bucket) Get(keyPath [][]byte) (value []byte, err error) {
	if err = b.err; err != nil {
		return
	}

	switch len(keyPath) {
	case 0:
		err = ErrKeyRequired
	case 1:
		err = ErrBucketNameRequired
	default:
		if bucket, key := b.bucketKeySplit(keyPath, false, &err); err == nil {
			value = bucket.Get(key)
		}
	}
	return
}

func (b *bucket) Del(keyPath [][]byte) (err error) {
	if err = b.err; err != nil {
		return
	}

	switch len(keyPath) {
	case 0:
		err = ErrKeyRequired
	case 1:
		err = b.tx.DeleteBucket(keyPath[0])
	default:
		if bucket, key := b.bucketKeySplit(keyPath, false, &err); err == nil {
			if bucket.Bucket(key) != nil {
				err = bucket.DeleteBucket(key)
			} else {
				err = bucket.Delete(key)
			}
		}
	}
	return
}

func (b *bucket) Scan(keyPath [][]byte, scan []byte, fn func(k, v []byte, bucket bool) error) (err error) {
	if err = b.err; err != nil {
		return
	}

	err = func() (err error) {
		if len(keyPath) == 0 {
			err = b.tx.ForEach(func(name []byte, b *bbolt.Bucket) error { return fn(name, nil, true) })
			return
		}

		bucket := b.getBucket(keyPath, false, &err)
		if err != nil {
			return
		}

		if len(scan) == 0 {
			if err = bucket.ForEachBucket(func(k []byte) error { return fn(k, nil, true) }); err != nil {
				return
			}
			err = bucket.ForEach(func(k, v []byte) error {
				if bucket.Bucket(k) == nil {
					return fn(k, v, bucket.Bucket(k) != nil)
				}
				return nil
			})
		} else {
			if err = bucket.ForEachBucket(func(k []byte) error {
				if bytes.HasPrefix(k, scan) {
					return fn(k, nil, true)
				}
				return nil
			}); err != nil {
				return
			}
			cur := bucket.Cursor()
			for k, v := cur.Seek(scan); k != nil; k, v = cur.Next() {
				if !bytes.HasPrefix(k, scan) {
					break
				}
				if err = fn(k, v, false); err != nil {
					break
				}
			}
		}
		return
	}()

	if err == io.EOF || err == fs.SkipAll || err == fs.SkipDir {
		err = nil
	}
	return
}
