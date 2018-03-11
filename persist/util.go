package persist

import (
  "fmt"
  "time"
  "reflect"
  
  "gdb/uuid"
)

// Is this interface empty
func IsEmpty(v interface{}) bool {
  switch c := v.(type) {
    case nil:
      return true
    case uuid.UUID:
      return c == uuid.Zero
    case time.Time:
      return c.IsZero()
  }
  
  val := reflect.ValueOf(v)
  switch val.Kind() {
    case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
      return val.Int() == 0
    case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
      return val.Uint() == 0
    case reflect.Float32, reflect.Float64:
      return val.Float() == 0
    case reflect.Array, reflect.Chan, reflect.Map, reflect.Slice, reflect.String:
      return val.Len() == 0
    case reflect.Ptr, reflect.Func, reflect.Interface:
      return !val.IsValid() || val.IsNil()
  }
  
  return false
}

// Generate an argument list
func arglist(s, n int) string {
  var l string
  for i := 0; i < n; i++ {
    if i > 0 {
      l += fmt.Sprintf(", $%d", s + i)
    }else{
      l += fmt.Sprintf("$%d", s + i)
    }
  }
  return l
}
