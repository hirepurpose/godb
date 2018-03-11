package godb

import (
  "fmt"
  "encoding/json"
  
  "gdb/uuid"
)

// Generate a property list, optionally with a qualifier
func proplist(p string, a []string) string {
  var l string
  for i, e := range a {
    if i > 0 { l += ", " }
    if p != "" {
      l += p +"."+ e
    }else{
      l += e
    }
  }
  return l
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

// Generate an assignment list
func setlist(s int, p string, a []string) string {
  var l string
  for i, e := range a {
    if i > 0 {
      l += ", "
    }
    if p != "" {
      l += p +"."
    }
    l += fmt.Sprintf("%s = $%d", e, s + i)
  }
  return l
}

// Convert ids to a string list
func idlist(ids []uuid.UUID) []interface{} {
  s := make([]interface{}, len(ids))
  for i, e := range ids {
    s[i] = e.String()
  }
  return s
}

// Serialize a nullable value
func marshal(v interface{}) (*string, error) {
  var s *string
  if v != nil {
    data, err := json.Marshal(v)
    if err != nil {
      return nil, err
    }
    c := string(data)
    s = &c
  }
  return s, nil
}

// Deserialize a nullable value
func unmarshal(s *string, v interface{}) error {
  if s != nil && len(*s) > 0 {
    err := json.Unmarshal([]byte(*s), v)
    if err != nil {
      return err
    }
  }
  return nil
}
