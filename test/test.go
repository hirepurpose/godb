package test

import (
  "os"
  "fmt"
  "sync"
  "net/url"
  
  "github.com/hirepurpose/godb"
  "github.com/hirepurpose/godb/sync/faux"
)

import (
  "github.com/bww/go-util/debug"
  "github.com/bww/go-upgrade/driver/postgres"
)

const sourceDB = "postgres"

func dburl(n string) string {
  return fmt.Sprintf("postgres://postgres@localhost/%s?sslmode=disable", url.PathEscape(n))
}

var sharedDB *godb.Database
func DB() *godb.Database {
  return sharedDB
}

var initOnce sync.Once
func Init(n string, m bool) {
  initOnce.Do(func() {
    teardown(n, m)
    setup(n, m)
  })
}

func setup(name string, migrate bool) {
  syncer := faux.New()
  
  debug.DEBUG   = istrue(os.Getenv("GODB_DEBUG"))
  debug.VERBOSE = istrue(os.Getenv("GODB_VERBOSE"))
  debug.TRACE   = istrue(os.Getenv("GODB_TRACE"))
  
  err := postgres.CreateDatabase(dburl(sourceDB), name)
  if err != nil {
    panic(fmt.Errorf("Creating %s (from %s): %v", name, sourceDB, err))
  }
  sharedDB, err = godb.New(dburl(name), migrate, syncer)
  if err != nil {
    panic(err)
  }
}

func teardown(name string, migrate bool) {
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
