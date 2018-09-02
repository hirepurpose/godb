package persist

import (
  "fmt"
  "reflect"
  
  "github.com/hirepurpose/godb"
  "github.com/hirepurpose/godb/convert"
  "github.com/hirepurpose/godb/uuid"
)

var inc = 0
func newId() interface{} {
  inc++
  return fmt.Sprintf("%d", inc)
}

type foreignTester struct {
  Id      uuid.Ident  `db:"id,pk"`
  Value   string      `db:"value"`
}

func (f foreignTester) ForeignKey() interface{} {
  return f.Id
}

type foreignPersister struct {
  ORM
}

func (e foreignPersister) Table() string {
  return "hp_persist_test_foreign" // defined in base but never created in production
}

func (e foreignPersister) StoreTesterEntity(v *foreignTester, opts StoreOptions, cxt db.Context) error {
  return e.StoreEntity(e, v, opts, cxt)
}

func (e foreignPersister) FetchTesterEntity(id uuid.Ident, opts FetchOptions, cxt db.Context) (*foreignTester, error) {
  v := &foreignTester{}
  err := e.FetchEntity(e, v, opts, cxt, `SELECT {*} FROM hp_persist_test_foreign WHERE id = $1`, id)
  if err != nil {
    return nil, err
  }
  return v, nil
}

func (e foreignPersister) FetchTesterEntities(limit Range, opts FetchOptions, cxt db.Context) ([]*foreignTester, error) {
  var v []*foreignTester
  err := e.FetchEntities(e, &v, opts, cxt, `SELECT {*} FROM hp_persist_test_foreign ORDER BY id OFFSET $1 LIMIT $2`, limit.Location, limit.Length)
  if err != nil {
    return nil, err
  }
  return v, nil
}

func (e foreignPersister) DeleteTesterEntity(v *foreignTester, opts StoreOptions, cxt db.Context) error {
  return e.DeleteEntity(e, v, opts, cxt)
}

type namedInlineTester struct {
  A       bool                `db:"named_a"`
  B       string              `db:"named_b"`
}

type entityTester struct {
  Id      string              `db:"id,pk"`
  Name    string              `db:"name"`
  Foreign *foreignTester      `db:"foreign_id,fk"`
  Named   *namedInlineTester  `db:",inline"`
  Inline  struct {
    A     string              `db:"inline_a"`
    B     int                 `db:"inline_b"`
  }                           `db:",inline"`
}

type entityPersister struct {
  ORM
}

func (e entityPersister) Table() string {
  return "hp_persist_test" // defined in base but never created in production
}

func (t entityPersister) GenerateId(val interface{}, cxt db.Context) (interface{}, error) {
  return uuid.New().String(), nil
}

func (t entityPersister) IsTransient(val interface{}, cxt db.Context) (bool, error) {
  var n int
  id := val.(*entityTester).Id
  if id == "" {
    return true, nil
  }
  if cxt == nil {
    cxt = t.DefaultContext()
  }
  err := cxt.QueryRow(`SELECT COUNT(*) FROM hp_persist_test WHERE id = $1`, id).Scan(&n)
  if err != nil {
    return false, err
  }
  return n == 0, nil
}

func (e entityPersister) StoreTesterEntity(v *entityTester, opts StoreOptions, cxt db.Context) error {
  return e.StoreEntity(e, v, opts, cxt)
}

func (e entityPersister) FetchTesterEntity(id string, opts FetchOptions, cxt db.Context) (*entityTester, error) {
  v := &entityTester{}
  err := e.FetchEntity(e, v, opts, cxt, `SELECT {*} FROM hp_persist_test WHERE id = $1`, id)
  if err != nil {
    return nil, err
  }
  return v, nil
}

func (e entityPersister) FetchTesterEntities(limit Range, opts FetchOptions, cxt db.Context) ([]*entityTester, error) {
  var v []*entityTester
  err := e.FetchEntities(e, &v, opts, cxt, `SELECT {*} FROM hp_persist_test ORDER BY name OFFSET $1 LIMIT $2`, limit.Location, limit.Length)
  if err != nil {
    return nil, err
  }
  return v, nil
}

func (e entityPersister) IterTesterEntities(opts FetchOptions, cxt db.Context) (Iter, error) {
  return e.IterEntities(e, reflect.TypeOf((*entityTester)(nil)), opts, cxt, `SELECT {*} FROM hp_persist_test ORDER BY name`)
}

func (e entityPersister) DeleteTesterEntity(v *entityTester, opts StoreOptions, cxt db.Context) error {
  return e.DeleteEntity(e, v, opts, cxt)
}

func (e entityPersister) StoreRelated(v interface{}, opts StoreOptions, cxt db.Context) error {
  z := v.(*entityTester)
  if z.Foreign != nil {
    p := foreignPersister{New(cxt)}
    err := p.StoreTesterEntity(z.Foreign, opts, cxt)
    if err != nil {
      return err
    }
  }
  return nil
}

func (e entityPersister) StoreReferences(v interface{}, opts StoreOptions, cxt db.Context) error {
  return nil
}

func (e entityPersister) FetchRelatedExtra(v interface{}, extra Columns, opts FetchOptions, cxt db.Context) error {
  z := v.(*entityTester)
  if k, ok := extra["foreign_id"]; ok && k != nil {
    var id uuid.Ident
    err := convert.Assign(&id, k)
    if err != nil {
      return err
    }
    p := foreignPersister{New(cxt)}
    f, err := p.FetchTesterEntity(id, opts, cxt)
    if err != nil {
      return err
    }
    z.Foreign = f
  }
  return nil
}

func (e entityPersister) DeleteRelated(v interface{}, opts StoreOptions, cxt db.Context) error {
  return nil
}

func (e entityPersister) DeleteReferences(v interface{}, opts StoreOptions, cxt db.Context) error {
  return nil
}
