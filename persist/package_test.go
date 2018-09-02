package persist

import (
  "os"
  "testing"
  
  "github.com/hirepurpose/godb/test"
)

const dbname = "hp_db_persist_test"
const table  = "hp_persist_test"

func TestMain(m *testing.M) {
  test.Init(dbname)
  os.Exit(m.Run())
}
