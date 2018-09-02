package persist

import (
  "fmt"
  "strings"
  "reflect"
)

type multiError []error

func (e multiError) Error() string {
  s := &strings.Builder{}
  for i, v := range e {
    if i > 0 { s.WriteString("; ") }
    s.WriteString(v.Error())
  }
  return s.String()
}

type subfetchError struct {
  which   reflect.Type
  value   reflect.Value
  err     error
}

func newSubfetchError(t reflect.Type, v reflect.Value, e error) subfetchError {
  return subfetchError{t, v, e}
}

func (s subfetchError) Error() string {
  return fmt.Sprintf("Sub-fetch (%v) - %v", s.which, s.err)
}
