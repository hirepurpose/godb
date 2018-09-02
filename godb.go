package godb

import (
  "fmt"
  "time"
  "net/url"
  "strings"
  "database/sql"
  
  "github.com/hirepurpose/godb/sync"
)

import (
	_ "github.com/lib/pq"
  
  "github.com/bww/go-lru"
  "github.com/bww/go-alert"
  "github.com/bww/go-upgrade"
  "github.com/bww/go-util/env"
  "github.com/bww/go-util/debug"
  "github.com/bww/go-upgrade/driver/postgres"
)

const (
  RESULT_COUNT_MAX            = 250
  RESULT_COUNT_DEFAULT        = 25
  CACHE_ELEMENTS_DEFAULT      = 1024
)

// A scannable object
type scannable interface {
  Scan(dest ...interface{})(error)
}

// A transaction handler
type TransactionHandler func(cxt Context)(error)

// The store client
type Database struct {
  db      *sql.DB
  cache   *lru.Cache
  dbname  string
}

// Create a new store
func New(uri string, migrate bool, syncer sync.Service) (*Database, error) {
  
  // parse the URL for the scheme
  u, err := url.Parse(uri)
  if err != nil {
    return nil, err
  }
  
  // note this here, since we don't want to log credentials
  fmt.Printf("-----> Connecting to %v at: %v%v\n", strings.Title(u.Scheme), u.Host, u.Path)
  // open our database connection
  db, err := sql.Open(u.Scheme, uri)
  if err != nil {
    return nil, fmt.Errorf("Could not open cache DB connection: %v", err)
  }
  
  // params
  db.SetMaxOpenConns(10)
  db.SetMaxIdleConns(10)
  
  // setup our store
  store := &Database{db, lru.New(CACHE_ELEMENTS_DEFAULT), u.Path}
  
  // run migrations if necessary
  if migrate {
    if syncer == nil {
      panic("search: Cannot migrate without a synchronization service!")
    }
    lock, err := syncer.Mutex(fmt.Sprintf("/godb/%s/db/postgres", env.Environ()))
    if err != nil {
      return nil, err
    }
    err = lock.Perform(store.migrate)
    if err != nil {
      return nil, err
    }
  }
  
  return store, nil
}

// Migrate
func (d *Database) migrate() error {
  
  p, err := postgres.NewWithDB(d.db)
  if err != nil {
    return err
  }
  
  u, err := upgrade.New(upgrade.Config{Resources:env.Etc("db"), Driver:p})
  if err != nil {
    return err
  }
  
  _, err = u.Upgrade()
  if err != nil {
    return err
  }
  
  return nil
}

// Obtain the underlying database
func (d *Database) Database() *sql.DB {
  return d.db
}

// Implement Context
func (d *Database) Exec(query string, args ...interface{}) (sql.Result, error) {
  return d.db.Exec(query, args...)
}

// Implement Context
func (d *Database) Query(query string, args ...interface{}) (*sql.Rows, error) {
  return d.db.Query(query, args...)
}

// Implement Context
func (d *Database) QueryRow(query string, args ...interface{}) *sql.Row {
  return d.db.QueryRow(query, args...)
}

// Begin a transaction
func (d *Database) Begin() (*sql.Tx, error) {
  return d.db.Begin()
}

// Execute in a new transaction and commit or roll-back as necessary on completion
// if the provided transation is nil. Otherwise, use the provided transaction and
// assume it is managed externally.
func (d *Database) Atomic(cxt Context, h TransactionHandler) error {
  if cxt == nil {
    return d.Transaction(h)
  }else{
    return h(cxt)
  }
}

// Execute in a transaction. A transaction is created and the handler is invoked.
// If the handler returns a non-nil error the transaction is rolled back, otherwise
// the transaction is committed.
func (d *Database) Transaction(h TransactionHandler) error {
  
  tx, err := d.Begin()
  if err != nil {
    return err
  }
  
  cxt := Context(tx)
  if debug.VERBOSE {
    cxt = NewDebugContextWithPrefix(" <txn>", cxt)
  }
  
  err = h(cxt)
  
  if err == nil {
    err = tx.Commit()
  }else if terr := tx.Rollback(); terr != nil {
    alt.Errorf("store: Could not rollback transaction: %v", terr)
  }
  
  return err
}

// Return the current time truncated to database precision (milliseconds)
func Now() time.Time {
  return time.Now().UTC().Truncate(time.Millisecond)
}
