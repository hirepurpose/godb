package persist

import (
  "os"
  "fmt"
  "testing"
  
  "gdb"
  "gdb/uuid"
  "gdb/test"
)

import (
  "github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
  test.Init(m, "gdb_test_persist")
  os.Exit(m.Run())
}

var inc = 0
func newId() interface{} {
  inc++
  return fmt.Sprintf("%d", inc)
}

type entityTester struct {
  Id    string    `db:"id,pk"`
  Name  string    `db:"name"`
}

type entityPersister struct {
  Persister
}

func (e entityPersister) Table() string {
  return "gdb_persist_test" // defined in base but never created in production
}

func (t entityPersister) GenerateId(val interface{}, cxt gdb.Context) (interface{}, error) {
  return uuid.New().String(), nil
}

func (t entityPersister) IsTransient(val interface{}, cxt gdb.Context) (bool, error) {
  var n int
  id := val.(*entityTester).Id
  if id == "" {
    return true, nil
  }
  if cxt == nil {
    cxt = t.DefaultContext()
  }
  err := cxt.QueryRow(`SELECT COUNT(*) FROM gdb_persist_test WHERE id = $1`, id).Scan(&n)
  if err != nil {
    return false, err
  }
  return n == 0, nil
}

func (e entityPersister) StoreTesterEntity(v *entityTester, opts StoreOptions, cxt gdb.Context) error {
  return e.StoreEntity(e, v, opts, cxt)
}

func (e entityPersister) FetchTesterEntity(id string, opts FetchOptions, cxt gdb.Context) (*entityTester, error) {
  v := &entityTester{}
  err := e.FetchEntity(e, v, opts, cxt, `SELECT {*} FROM gdb_persist_test WHERE id = $1`, id)
  if err != nil {
    return nil, err
  }
  return v, nil
}

func (e entityPersister) FetchTesterEntities(limit gdb.Range, opts FetchOptions, cxt gdb.Context) ([]*entityTester, error) {
  var v []*entityTester
  err := e.FetchEntities(e, &v, opts, cxt, `SELECT {*} FROM gdb_persist_test ORDER BY created_at, id OFFSET $1 LIMIT $2`, limit.Location, limit.Length)
  if err != nil {
    return nil, err
  }
  return v, nil
}

func (e entityPersister) DeleteTesterEntity(v *entityTester, opts StoreOptions, cxt gdb.Context) error {
  return e.DeleteEntity(e, v, opts, cxt)
}

func TestIdentHandling(t *testing.T) {
  var err error
  
  cxt := test.DB()
  if !assert.NotNil(t, cxt) { return }
  p := &entityPersister{New(cxt)}
  
  _, err = cxt.Exec("DROP TABLE IF EXISTS gdb_persist_test;")
  if !assert.Nil(t, err) { return }
  _, err = cxt.Exec("CREATE TABLE gdb_persist_test (id uuid primary key, name varchar(128) not null, created_at timestamp with time zone not null default now());")
  if !assert.Nil(t, err) { return }
  
  e := &entityTester{"", "This is the name"}
  trans, err := p.IsTransient(e, cxt)
  if assert.Nil(t, err, fmt.Sprintf("%v", err)) {
    assert.Equal(t, true, trans)
  }
  
  e.Id = uuid.New().String()
  trans, err = p.IsTransient(e, cxt)
  if assert.Nil(t, err, fmt.Sprintf("%v", err)) {
    assert.Equal(t, true, trans)
  }
  
  err = p.StoreTesterEntity(e, 0, nil)
  assert.Nil(t, err, fmt.Sprintf("%v", err))
  
  c, err := p.FetchTesterEntity(e.Id, 0, nil)
  if assert.Nil(t, err, fmt.Sprintf("%v", err)) {
    assert.Equal(t, e, c)
  }
  
  trans, err = p.IsTransient(e, cxt)
  if assert.Nil(t, err, fmt.Sprintf("%v", err)) {
    assert.Equal(t, false, trans)
  }
  
  a, err := p.FetchTesterEntities(gdb.Range{0, 100}, 0, nil)
  if assert.Nil(t, err, fmt.Sprintf("%v", err)) {
    assert.Equal(t, []*entityTester{e}, a)
  }
  
  err = p.DeleteTesterEntity(e, 0, nil)
  assert.Nil(t, err, fmt.Sprintf("%v", err))
  
  trans, err = p.IsTransient(e, cxt)
  if assert.Nil(t, err, fmt.Sprintf("%v", err)) {
    assert.Equal(t, true, trans)
  }
}
