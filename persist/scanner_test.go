package persist

import (
  "fmt"
  "sort"
  "reflect"
  "testing"
)

import (
  "github.com/stretchr/testify/assert"
)

type ident string
func (v ident) New() interface{} {
  return ident(newId().(string))
}

type validTester struct {
  A   ident     `db:"a,pk"`
  B   string    `db:"b"`
  C   int       `db:"c,ro"`
}

type embedTester struct {
  validTester
  D   bool      `db:"d"`
}

func sortedPrimaryKeys(s *scanner) []string {
  p := s.PrimaryKeys()
  sort.Strings(p)
  return p
}

func sortedProperties(s *scanner) []string {
  p := s.Properties()
  sort.Strings(p)
  return p
}

func TestMapping(t *testing.T) {
  
  v := &validTester{"~", "B", 123}
  s := Scanner(v)
  
  assert.Equal(t, []string{"a"},      sortedPrimaryKeys(s))
  assert.Equal(t, []string{"b","c"},  sortedProperties(s))
  
  err := s.SetId(ident("A"))
  if assert.Nil(t, err, fmt.Sprintf("%v", err)) {
    assert.Equal(t, &validTester{ident("A"),"B",123}, v)
    id, err := s.Id()
    if assert.Nil(t, err, fmt.Sprintf("%v", err)) {
      assert.Equal(t, v.A, id)
    }
  }
  
  x, err := s.NewId()
  if assert.Nil(t, err, fmt.Sprintf("%v", err)) {
    assert.Equal(t, ident("1"), x)
  }  
  
  l, err := s.Values(true, Read)
  if assert.Nil(t, err, fmt.Sprintf("%v", err)) {
    assert.Equal(t, map[string]interface{}{"a":ident("A"),"b":"B","c":123}, l)
  }
  
  l, err = s.Values(false, Write)
  if assert.Nil(t, err, fmt.Sprintf("%v", err)) {
    assert.Equal(t, map[string]interface{}{"b":"B"}, l)
  }
  
  d, err := s.Dests([]string{"a","b","c"})
  if assert.Nil(t, err, fmt.Sprintf("%v", err)) {
    reflect.ValueOf(d[0]).Elem().Set(reflect.ValueOf(ident("Y")))
    reflect.ValueOf(d[1]).Elem().SetString("Z")
    reflect.ValueOf(d[2]).Elem().SetInt(987)
    assert.Equal(t, &validTester{ident("Y"),"Z",987}, v)
  }
  
  // ---
  
  e := &embedTester{*v, false}
  s  = Scanner(e)
  
  assert.Equal(t, []string{"a"},          sortedPrimaryKeys(s))
  assert.Equal(t, []string{"b","c","d"},  sortedProperties(s))
  
  err = s.SetId(ident("Q"))
  if assert.Nil(t, err, fmt.Sprintf("%v", err)) {
    assert.Equal(t, &embedTester{validTester{ident("Q"),"Z",987}, false}, e)
    id, err := s.Id()
    if assert.Nil(t, err, fmt.Sprintf("%v", err)) {
      assert.Equal(t, e.A, id)
    }
  }
  
  x, err = s.NewId()
  if assert.Nil(t, err, fmt.Sprintf("%v", err)) {
    assert.Equal(t, ident("2"), x)
  }  
  
  l, err = s.Values(true, Read)
  if assert.Nil(t, err, fmt.Sprintf("%v", err)) {
    assert.Equal(t, map[string]interface{}{"a":ident("Q"),"b":"Z","c":987,"d":false}, l)
  }
  
  d, err = s.Dests([]string{"a","b","c","d"})
  if assert.Nil(t, err, fmt.Sprintf("%v", err)) {
    reflect.ValueOf(d[0]).Elem().Set(reflect.ValueOf(ident("A")))
    reflect.ValueOf(d[1]).Elem().SetString("B")
    reflect.ValueOf(d[2]).Elem().SetInt(123)
    reflect.ValueOf(d[3]).Elem().SetBool(true)
    assert.Equal(t, &embedTester{validTester{ident("A"),"B",123}, true}, e)
  }
  
}
