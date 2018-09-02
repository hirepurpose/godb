package persist

import (
  "fmt"
  "testing"
  
  "github.com/hirepurpose/godb/test"
  "github.com/hirepurpose/godb/uuid"
)

import (
  "github.com/stretchr/testify/assert"
)

func TestCRUD(t *testing.T) {
  cxt := test.DB()
  if !assert.NotNil(t, cxt) { return }
  pe := &entityPersister{New(cxt)}
  pf := &foreignPersister{New(cxt)}
  
  f := &foreignTester{Value:"Foreign value"}
  err := pf.StoreTesterEntity(f, 0, nil)
  assert.Nil(t, err, fmt.Sprintf("%v", err))
  
  e := &entityTester{Id: "", Name: "This is the name", Foreign: f, Named: &namedInlineTester{true, "Named inline struct B"}}
  e.Inline.A = "Anonymous inline struct A"
  e.Inline.B = 998877
  
  trans, err := pe.IsTransient(e, cxt)
  if assert.Nil(t, err, fmt.Sprintf("%v", err)) {
    assert.Equal(t, true, trans)
  }
  
  e.Id = uuid.New().String()
  trans, err = pe.IsTransient(e, cxt)
  if assert.Nil(t, err, fmt.Sprintf("%v", err)) {
    assert.Equal(t, true, trans)
  }
  
  err = pe.StoreTesterEntity(e, StoreOptionCascade, nil)
  assert.Nil(t, err, fmt.Sprintf("%v", err))
  
  c, err := pe.FetchTesterEntity(e.Id, FetchOptionCascade, nil)
  if assert.Nil(t, err, fmt.Sprintf("%v", err)) {
    assert.Equal(t, e, c)
  }
  
  trans, err = pe.IsTransient(e, cxt)
  if assert.Nil(t, err, fmt.Sprintf("%v", err)) {
    assert.Equal(t, false, trans)
  }
  
  a, err := pe.FetchTesterEntities(Range{0, 100}, FetchOptionCascade, nil)
  if assert.Nil(t, err, fmt.Sprintf("%v", err)) {
    assert.Equal(t, []*entityTester{e}, a)
  }
  
  err = pe.DeleteTesterEntity(e, 0, nil)
  assert.Nil(t, err, fmt.Sprintf("%v", err))
  
  trans, err = pe.IsTransient(e, cxt)
  if assert.Nil(t, err, fmt.Sprintf("%v", err)) {
    assert.Equal(t, true, trans)
  }
}

func TestFetchOne(t *testing.T) {
  cxt := test.DB()
  pe := &entityPersister{New(cxt)}
  n := 100
  
  _, err := cxt.Exec(fmt.Sprintf("DELETE FROM %s", table))
  if !assert.Nil(t, err, fmt.Sprintf("%v", err)) {
    return
  }
  
  for i := 0; i < n; i++ {
    e := &entityTester{Name: fmt.Sprintf("%04d This is the name", i), Named: &namedInlineTester{true, fmt.Sprintf("Named inline struct B #%d", i)}}
    e.Inline.A = fmt.Sprintf("Anonymous inline struct A #%d", i)
    e.Inline.B = i
    
    err := pe.StoreTesterEntity(e, StoreOptionCascade, nil)
    assert.Nil(t, err, fmt.Sprintf("%v", err))
    
    r, err := pe.FetchTesterEntity(e.Id, FetchOptionCascade, nil)
    if assert.Nil(t, err, fmt.Sprintf("%v", err)) {
      assert.Equal(t, e, r)
    }
  }
  
}

func TestFetchMany(t *testing.T) {
  cxt := test.DB()
  pe := &entityPersister{New(cxt)}
  n := 100
  
  _, err := cxt.Exec(fmt.Sprintf("DELETE FROM %s", table))
  if !assert.Nil(t, err, fmt.Sprintf("%v", err)) {
    return
  }
  
  check := make([]*entityTester, n)
  for i := 0; i < n; i++ {
    e := &entityTester{Name: fmt.Sprintf("%04d This is the name", i), Named: &namedInlineTester{true, fmt.Sprintf("Named inline struct B #%d", i)}}
    e.Inline.A = fmt.Sprintf("Anonymous inline struct A #%d", i)
    e.Inline.B = i
    
    err := pe.StoreTesterEntity(e, StoreOptionCascade, nil)
    assert.Nil(t, err, fmt.Sprintf("%v", err))
    check[i] = e
  }
  
  a, err := pe.FetchTesterEntities(Range{0, n}, FetchOptionCascade, nil)
  if assert.Nil(t, err, fmt.Sprintf("%v", err)) {
    assert.Equal(t, check, a)
  }
  
}

func TestFetchIter(t *testing.T) {
  cxt := test.DB()
  pe := &entityPersister{New(cxt)}
  n := 1000
  
  _, err := cxt.Exec(fmt.Sprintf("DELETE FROM %s", table))
  if !assert.Nil(t, err, fmt.Sprintf("%v", err)) {
    return
  }
  
  check := make([]*entityTester, n)
  for i := 0; i < n; i++ {
    e := &entityTester{Name: fmt.Sprintf("%04d This is the name", i), Named: &namedInlineTester{true, fmt.Sprintf("Named inline struct B #%d", i)}}
    e.Inline.A = fmt.Sprintf("Anonymous inline struct A #%d", i)
    e.Inline.B = i
    
    err := pe.StoreTesterEntity(e, StoreOptionCascade, nil)
    assert.Nil(t, err, fmt.Sprintf("%v", err))
    check[i] = e
  }
  
  it, err := pe.IterTesterEntities(FetchOptionCascade, nil)
  if assert.Nil(t, err, fmt.Sprintf("%v", err)) {
    i := 0
    for ; it.Next(); i++ {
      x := &entityTester{}
      err = it.Scan(x)
      if assert.Nil(t, err, fmt.Sprintf("%v", err)) {
        assert.Equal(t, check[i], x)
      }
    }
    assert.Equal(t, n, i)
  }
  
}
