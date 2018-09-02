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
  A   ident             `db:"a,pk"`
  B   string            `db:"b"`
  C   int               `db:"c,ro"`
}

type embedTester struct {
  validTester
  D   bool              `db:"d"`
  E   *referenceTester  `db:"e,fk"`
  H   inlineTester      `db:",inline"`
}

type anotherTester struct {
  A   ident             `db:"a,pk"`
  B   *inlineTester     `db:"prefix_,inline"`
}

type referenceTester struct {
  F   ident             `db:"f,pk"`
  G   int               `db:"g"`
}

type inlineTester struct {
  HA  bool              `db:"h_a"`
  HB  string            `db:"h_b"`
}

type emptyNameTester struct {
  inlineTester          `db:"-"`
  A   ident             `db:"a,pk"`
  X   bool              `db:"-"`
  B   string            `db:"b"`
}

func (r referenceTester) ForeignKey() interface{} {
  return r.F
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
  
  t.Run("A", func(t *testing.T) {
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
      assert.Equal(t, Columns{"a":ident("A"),"b":"B","c":123}, l)
    }
    
    l, err = s.Values(false, Write)
    if assert.Nil(t, err, fmt.Sprintf("%v", err)) {
      assert.Equal(t, Columns{"b":"B"}, l)
    }
    
    d, _, err := s.Dests([]string{"a","b","c"})
    if assert.Nil(t, err, fmt.Sprintf("%v", err)) {
      reflect.ValueOf(d[0]).Elem().Set(reflect.ValueOf(ident("Y")))
      reflect.ValueOf(d[1]).Elem().SetString("Z")
      reflect.ValueOf(d[2]).Elem().SetInt(987)
      assert.Equal(t, &validTester{ident("Y"),"Z",987}, v)
    }
  })
  
  // ---
  
  t.Run("B", func(t *testing.T) {
    v := &validTester{"~", "Z", 987}
    e := &embedTester{*v, false, &referenceTester{ident("V"), 999}, inlineTester{true, "R"}}
    s := Scanner(e)
    
    assert.Equal(t, []string{"a"},                          sortedPrimaryKeys(s))
    assert.Equal(t, []string{"b","c","d","e","h_a","h_b"},  sortedProperties(s))
    
    err := s.SetId(ident("Q"))
    if assert.Nil(t, err, fmt.Sprintf("%v", err)) {
      assert.Equal(t, &embedTester{validTester{ident("Q"),"Z",987}, false, &referenceTester{ident("V"), 999}, inlineTester{true, "R"}}, e)
      id, err := s.Id()
      if assert.Nil(t, err, fmt.Sprintf("%v", err)) {
        assert.Equal(t, e.A, id)
      }
    }
    
    x, err := s.NewId()
    if assert.Nil(t, err, fmt.Sprintf("%v", err)) {
      assert.Equal(t, ident("2"), x)
    }  
    
    l, err := s.Values(true, Read)
    if assert.Nil(t, err, fmt.Sprintf("%v", err)) {
      assert.Equal(t, Columns{"a":ident("Q"),"b":"Z","c":987,"d":false,"h_a":true,"h_b":"R"}, l)
    }
    
    d, z, err := s.Dests([]string{"a","b","c","d","e","h_a","h_b"})
    if assert.Nil(t, err, fmt.Sprintf("%v", err)) {
      reflect.ValueOf(d[0]).Elem().Set(reflect.ValueOf(ident("A")))
      reflect.ValueOf(d[1]).Elem().SetString("B")
      reflect.ValueOf(d[2]).Elem().SetInt(123)
      reflect.ValueOf(d[3]).Elem().SetBool(true)
      assert.Equal(t, &embedTester{validTester{ident("A"),"B",123}, true, &referenceTester{ident("V"), 999}, inlineTester{true, "R"}}, e)
      reflect.ValueOf(z["e"]).Elem().Set(reflect.ValueOf(ident("X")))
      assert.Equal(t, reflect.ValueOf(z["e"]).Elem().Interface(), ident("X"))
    }
  })
  
  // ---
  
  t.Run("C", func(t *testing.T) {
    a := &anotherTester{ident("A"), &inlineTester{true, "Inline tester"}}
    s := Scanner(a)
    
    assert.Equal(t, []string{"a"},                        sortedPrimaryKeys(s))
    assert.Equal(t, []string{"prefix_h_a","prefix_h_b"},  sortedProperties(s))
    
    l, err := s.Values(true, Read)
    if assert.Nil(t, err, fmt.Sprintf("%v", err)) {
      assert.Equal(t, Columns{"a":ident("A"),"prefix_h_a":true,"prefix_h_b":"Inline tester"}, l)
    }
    
    d, _, err := s.Dests([]string{"a","prefix_h_a","prefix_h_b"})
    if assert.Nil(t, err, fmt.Sprintf("%v", err)) && assert.Len(t, d, 3) {
      reflect.ValueOf(d[0]).Elem().Set(reflect.ValueOf(ident("Q")))
      reflect.ValueOf(d[1]).Elem().SetBool(false)
      reflect.ValueOf(d[2]).Elem().SetString("Z")
      assert.Equal(t, &anotherTester{ident("Q"), &inlineTester{false, "Z"}}, a)
    }
  })
  
  // ---
  
  t.Run("D", func(t *testing.T) {
    a := &anotherTester{ident("A"), nil}
    s := Scanner(a)
    
    assert.Equal(t, []string{"a"},                        sortedPrimaryKeys(s))
    assert.Equal(t, []string{"prefix_h_a","prefix_h_b"},  sortedProperties(s))
    
    l, err := s.Values(true, Read)
    if assert.Nil(t, err, fmt.Sprintf("%v", err)) {
      assert.Equal(t, Columns{"a":ident("A")}, l)
    }
    
    d, _, err := s.Dests([]string{"a","prefix_h_a","prefix_h_b"})
    if assert.Nil(t, err, fmt.Sprintf("%v", err)) && assert.Len(t, d, 3) {
      reflect.ValueOf(d[0]).Elem().Set(reflect.ValueOf(ident("Q")))
      reflect.ValueOf(d[1]).Elem().SetBool(true)
      reflect.ValueOf(d[2]).Elem().SetString("A")
      assert.Equal(t, &anotherTester{ident("Q"), &inlineTester{true, "A"}}, a)
    }
  })
  
  // ---
  
  t.Run("E", func(t *testing.T) {
    a := &emptyNameTester{inlineTester{true, "HB"}, ident("A"), false, "B"}
    s := Scanner(a)
    
    assert.Equal(t, []string{"a"}, sortedPrimaryKeys(s))
    assert.Equal(t, []string{"b"}, sortedProperties(s))
    
    l, err := s.Values(true, Read)
    if assert.Nil(t, err, fmt.Sprintf("%v", err)) {
      assert.Equal(t, Columns{"a":ident("A"), "b":"B"}, l)
    }
    
    d, _, err := s.Dests([]string{"a","b"})
    if assert.Nil(t, err, fmt.Sprintf("%v", err)) && assert.Len(t, d, 2) {
      reflect.ValueOf(d[0]).Elem().Set(reflect.ValueOf(ident("Y")))
      reflect.ValueOf(d[1]).Elem().SetString("Z")
      assert.Equal(t, &emptyNameTester{inlineTester{true, "HB"}, ident("Y"), false, "Z"}, a)
    }
  })
  
}
