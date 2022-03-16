
// Copyright 2019 ChainSafe Systems (ON) Corp.
// This file is part of gossamer.
//
// The gossamer library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The gossamer library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the gossamer library. If not, see <http://www.gnu.org/licenses/>.

package badger

import (
	"sync"
	"os"
	"fmt"
	"github.com/ethereum/go-ethereum/log"
  "github.com/ethereum/go-ethereum/ethdb"
	"github.com/dgraph-io/badger/v3"
)

// BadgerDB contains directory path to data and db instance
type BadgerDB struct {
	file   string
	db     *badger.DB
	lock   sync.RWMutex
}

type KVList = badger.KVList

// NewBadgerDB initializes badgerDB instance
func New(file string, cache int, handles int, namespace string, readonly bool) (*BadgerDB, error) {
	opts := badger.DefaultOptions(file)
	opts.ValueDir = file
	opts.Logger = nil

	if _, err := os.Stat(file); os.IsNotExist(err) {
		if err := os.MkdirAll(file, os.ModePerm); err != nil {
			return nil, err
			}
	}
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	return &BadgerDB{
		file: file,
		db:     db,
	}, nil
}

// Path returns the path to the database directory.
func (db *BadgerDB) Path() string {
	return db.file
}

func (db *BadgerDB) NewBatch() ethdb.Batch {
	return &BadgerBatch{db: db.db, b: db.db.NewWriteBatch(), keys: map[string]bool{}, keyvalue: map[string]string{}}
}

func (db *BadgerDB) NewBatchWithSize(size int) ethdb.Batch {
	return &BadgerBatch{db: db.db, b: db.db.NewWriteBatch(), keys: map[string]bool{}, keyvalue: map[string]string{}}
}

type BadgerBatch struct {
	db      *badger.DB
	b       *badger.WriteBatch
	size    int
	keys    map[string]bool
	keyvalue map[string]string
	discard bool
}

func (b *BadgerBatch) Put(key []byte, value []byte) error {
	if b.discard {
		b.b = b.db.NewWriteBatch()
		b.discard = false
		b.size = 0
		b.keyvalue = make(map[string]string)
	}
	err := b.b.SetEntry(badger.NewEntry(key, value).WithMeta(0))
	if err != nil {
		return err
	}
	b.size += len(value)
	b.keyvalue[string(key)] = string(value)
	return nil
}

func (b *BadgerBatch) Delete(key []byte) error {
	if b.discard {
		b.b = b.db.NewWriteBatch()
		b.discard = false
		b.size = 0
		b.keyvalue = make(map[string]string)
	}
	err := b.b.Delete(key)
	if err != nil {
		return err
	}
	b.size += len(key)
	delete(b.keyvalue, string(key))
	return nil
}

func (b *BadgerBatch) PutIfAbsent(key, value []byte) error {
	if b.discard {
		b.b = b.db.NewWriteBatch()
		b.discard = false
		b.size = 0
		b.keyvalue = make(map[string]string)
	}
	if !b.keys[string(key)] {
		err := b.b.SetEntry(badger.NewEntry(key, value).WithMeta(0))
		if err != nil {
			return err
		}
		b.size += len(value)
		b.keys[string(key)] = true
		return nil
	}
	return fmt.Errorf("duplicated key in batch, (HEX) %x", key)
}

func (b *BadgerBatch) Exist(key []byte) bool {
	return b.keys[string(key)]
}

func (b *BadgerBatch) Write() error {
	defer func() {
		b.b.Cancel()
		b.discard = true
	}()

	return b.b.Flush()
}

func (b *BadgerBatch) ValueSize() int {
	return b.size
}

func (b *BadgerBatch) Reset() {
	b.b = b.db.NewWriteBatch()
	b.keys = make(map[string]bool)
	b.keyvalue = make(map[string]string)
	b.size = 0
}

func (b *BadgerBatch) Replay(w ethdb.KeyValueWriter) error {
	if b.discard {
		b.b = b.db.NewWriteBatch()
		b.discard = false
		b.size = 0
		b.keyvalue = make(map[string]string)
	}
	for key, value := range b.keyvalue{
		if err := w.Put([]byte(key), []byte(value)); err!=nil{
			return err
		}
	}
	return nil
}


func (db *BadgerDB) Stat(property string) (string, error) {
	return "", nil
}


// Put puts the given key / value to the queue
func (db *BadgerDB) Put(key []byte, value []byte) error {
	db.lock.Lock()
	defer db.lock.Unlock()
	return db.db.Update(func(txn *badger.Txn) error {
		err := txn.Set(key, value)
		return err
	})
}

// Has checks the given key exists already; returning true or false
func (db *BadgerDB) Has(key []byte) (exists bool, err error) {
	db.lock.RLock()
	defer db.lock.RUnlock()
	err = db.db.View(func(txn *badger.Txn) error {
		item, errr := txn.Get(key)
		if item != nil {
			exists = true
		}
		if errr == badger.ErrKeyNotFound {
			exists = false
			errr = nil
		}
		return errr
	})
	return exists, err
}

// Get returns the given key
func (db *BadgerDB) Get(key []byte) (data []byte, err error) {
	db.lock.RLock()
	defer db.lock.RUnlock()
	err = db.db.View(func(txn *badger.Txn) error {
		item, e := txn.Get(key)
		if e != nil {
			return e
		}
		data, e = item.ValueCopy(nil)
		if e != nil {
			return e
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return data, nil
}

// Del removes the key from the queue and database
func (db *BadgerDB) Delete(key []byte) error {
	db.lock.Lock()
	defer db.lock.Unlock()
	return db.db.Update(func(txn *badger.Txn) error {
		err := txn.Delete(key)
		if err == badger.ErrKeyNotFound {
			err = nil
		}
		return err
	})
}

// Flush commits pending writes to disk
func (db *BadgerDB) Flush() error {
	return db.db.Sync()
}

// Close closes a DB
func (db *BadgerDB) Close() error {
	db.lock.Lock()
	defer db.lock.Unlock()
	if err := db.db.Close(); err != nil {
		return err
	}
	return nil
}

// ClearAll would delete all the data stored in DB.
func (db *BadgerDB) ClearAll() error {
	return db.db.DropAll()
}

// BadgerIterator struct contains a transaction, iterator and init.
type BadgerIterator struct {
	txn  *badger.Txn
	iter *badger.Iterator
	init bool
	lock sync.RWMutex
}

// NewIterator returns a new iterator within the Iterator struct along with a new transaction
func (db *BadgerDB) NewIterator(prefix []byte, start []byte) ethdb.Iterator {
	txn := db.db.NewTransaction(false)
	opts := badger.IteratorOptions{
	PrefetchValues: true,
	PrefetchSize:   100,
	Reverse:        false,
	AllVersions:    false,
	Prefix:         prefix,

}
	iter := txn.NewIterator(opts)
	return &BadgerIterator{
		txn:  txn,
		iter: iter,
	}
}

// Release closes the iterator and discards the created transaction.
func (i *BadgerIterator) Release() {
	i.lock.Lock()
	defer i.lock.Unlock()
	i.iter.Close()
	i.txn.Discard()
}

// Next rewinds the iterator to the zero-th position if uninitialized, and then will advance the iterator by one
// returns bool to ensure access to the item
func (i *BadgerIterator) Next() bool {
	i.lock.RLock()
	defer i.lock.RUnlock()

	if !i.init {
		i.iter.Rewind()
		i.init = true
		return i.iter.Valid()
	}

	if !i.iter.Valid() {
		return false
	}
	i.iter.Next()
	return i.iter.Valid()
}

// Seek will look for the provided key if present and go to that position. If
// absent, it would seek to the next smallest key
func (i *BadgerIterator) Seek(key []byte) {
	i.lock.RLock()
	defer i.lock.RUnlock()
	i.iter.Seek(key)
}

// Key returns an item key
func (i *BadgerIterator) Key() []byte {
	i.lock.RLock()
	defer i.lock.RUnlock()
	return i.iter.Item().Key()
}

// Value returns a copy of the value of the item
func (i *BadgerIterator) Value() []byte {
	i.lock.RLock()
	defer i.lock.RUnlock()
	val, err := i.iter.Item().ValueCopy(nil)
	if err != nil {
		log.Warn("value retrieval error ", "error", err)
	}
	return val
}

func (iter *BadgerIterator) Error() error {
	return nil
}

func (db *BadgerDB) Compact(start []byte, limit []byte) error {
	return nil
}
