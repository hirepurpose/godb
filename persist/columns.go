package persist

import (
  "reflect"
)

type Columns map[string]interface{}

// Produce a new map of columns by dereferencing every value in the receiver, each of
// which is expected to be a pointer and will panic if not
func (c Columns) Deref() Columns {
  d := make(Columns)
  for k, v := range c {
    d[k] = reflect.ValueOf(v).Elem().Interface()
  }
  return d
}

// Produce slices of keys and values whose indices correspond to each other
func (c Columns) KeysVals() ([]string, []interface{}) {
  rk := make([]string, len(c))
  rv := make([]interface{}, len(c))
  var i int
  for k, v := range c {
    rk[i] = k
    rv[i] = v
    i++
  }
  return rk, rv
}
