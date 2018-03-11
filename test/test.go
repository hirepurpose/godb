package test

import (
  "os"
  "fmt"
  "sync"
  "net/url"
  "testing"
  
  "gdb"
  "gdb/sync/faux"
)

import (
  "github.com/bww/go-util/debug"
  "github.com/bww/go-upgrade/driver/postgres"
)

const sourceDB = "postgres"

func dburl(n string) string {
  return fmt.Sprintf("postgres://postgres@localhost/%s?sslmode=disable", url.PathEscape(n))
}

var sharedDB *gdb.Database
func DB() *gdb.Database {
  return sharedDB
}

var initOnce sync.Once
func Init(m *testing.M, n string) {
  initOnce.Do(func() {
    teardown(n)
    setup(n)
  })
}

func setup(name string) {
  syncer := faux.New()
  
  debug.DEBUG   = istrue(os.Getenv("GODB_DEBUG"))
  debug.VERBOSE = istrue(os.Getenv("GODB_VERBOSE"))
  debug.TRACE   = istrue(os.Getenv("GODB_TRACE"))
  
  err := postgres.CreateDatabase(dburl(sourceDB), name)
  if err != nil {
    panic(fmt.Errorf("Creating %s (from %s): %v", name, sourceDB, err))
  }
  sharedDB, err = gdb.New(dburl(name), true, syncer)
  if err != nil {
    panic(err)
  }
}

func teardown(name string) {
  err := teardownPostgres(name)
  if err != nil {
    panic(err)
  }
}

func teardownPostgres(name string) error {
  err := postgres.DropDatabase(dburl(sourceDB), name)
  if err != nil {
    return err
  }
  return nil
}

func istrue(s string) bool {
  return s == "true" || s == "t" || s == "yes" || s == "y"
}
