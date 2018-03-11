package persist

import (
  "fmt"
  "sync"
  "strings"
  "reflect"
  
  "gdb/convert"
)

type Operation int
const (
  Read Operation = iota
  Write
)

// Mapping cache
type mappingCache struct {
  sync.RWMutex
  cache map[reflect.Type]*mapping
}

// Lookup a cached mapping
func (c *mappingCache) get(t reflect.Type) (*mapping, bool) {
  c.RLock()
  m, ok := c.cache[t]
  c.RUnlock()
  return m, ok
}

// Set a cached mapping
func (c *mappingCache) put(t reflect.Type, m *mapping) {
  c.Lock()
  c.cache[t] = m
  c.Unlock()
}

// Shared mapping cache
var sharedCache = &mappingCache{sync.RWMutex{}, make(map[reflect.Type]*mapping)}

// A field tag
type fieldTag struct {
  name        string
  primaryKey  bool
  readOnly    bool
}

// Parse a field tag
func newFieldTag(t string) (*fieldTag, error) {
  p := strings.Split(t, ",")
  if len(p) < 1 {
    return nil, fmt.Errorf("Invalid struct tag")
  }
  
  f := &fieldTag{name:strings.TrimSpace(p[0])}
  p  = p[1:]
  
  for _, e := range p {
    if strings.EqualFold(strings.TrimSpace(e), "pk") {
      f.primaryKey = true
    }else if strings.EqualFold(strings.TrimSpace(e), "ro") {
      f.readOnly = true
    }else{
      return nil, fmt.Errorf("Invalid struct tag: %s (%s)", t, e)
    }
  }
  return f, nil
}

// A field mapping
type fieldMapping struct {
  index     int
  field     reflect.StructField
  readOnly  bool
}

// A struct mapping
type mapping struct {
  reflect.Type
  primaryKeys map[string]fieldMapping
  properties  map[string]fieldMapping
  embeds      []fieldMapping
}

// Obtain a cached mapping or create a mapping
func Mapping(t reflect.Type) (*mapping, error) {
  var err error
  m, ok := sharedCache.get(t)
  if !ok {
    m, err = newMapping(t)
    if err != nil {
      return nil, err
    }
    sharedCache.put(t, m)
  }
  return m, nil
}

// Create a new mapping for the provided type
func newMapping(t reflect.Type) (*mapping, error) {
  for t.Kind() == reflect.Ptr {
    t = t.Elem()
  }
  if t.Kind() != reflect.Struct {
    return nil, fmt.Errorf("Mapped type must be a struct")
  }
  
  pk := make(map[string]fieldMapping)
  pv := make(map[string]fieldMapping)
  em := make([]fieldMapping, 0)
  
  n := t.NumField()
  for i := 0; i < n; i++ {
    f := t.Field(i)
    if f.Anonymous {
      em = append(em, fieldMapping{i, f, false})
    }else if v := f.Tag.Get("db"); v != "" {
      tag, err := newFieldTag(v)
      if err != nil {
        return nil, err
      }
      fm := fieldMapping{i, f, tag.readOnly}
      if tag.primaryKey {
        pk[tag.name] = fm
      }else{
        pv[tag.name] = fm
      }
    }
  }
  
  // if len(pk) != 1 {
  //   return nil, fmt.Errorf("Exactly one primary key is required for type %v; found %d", t, len(pk))
  // }
  // if len(pv) < 1 {
  //   return nil, fmt.Errorf("No properties are defined for type: %v", t)
  // }
  return &mapping{t, pk, pv, em}, nil
}

// Obtain a list of primary key columns
func (m *mapping) PrimaryKeys() []string {
  pk := make([]string, 0)
  
  for k, _ := range m.primaryKeys {
    pk = append(pk, k)
  }
  
  if len (m.embeds) > 0 {
    for _, e := range m.embeds {
      if e.field.Type == m.Type {
        panic(fmt.Errorf("Circular type embedding"))
      }
      s, err := Mapping(e.field.Type)
      if err != nil {
        panic(err)
      }
      pk = append(pk, s.PrimaryKeys()...)
    }
  }
  
  return pk
}

// Obtain a list of property columns
func (m *mapping) Properties() []string {
  pv := make([]string, 0)
  
  for k, _ := range m.properties {
    pv = append(pv, k)
  }
  
  if len (m.embeds) > 0 {
    for _, e := range m.embeds {
      if e.field.Type == m.Type {
        panic(fmt.Errorf("Circular type embedding"))
      }
      s, err := Mapping(e.field.Type)
      if err != nil {
        panic(err)
      }
      pv = append(pv, s.Properties()...)
    }
  }
  
  return  pv
}

// Obtain a list of identifier values
func (m *mapping) idValues(v reflect.Value) ([]reflect.Value, error) {
  pk := make([]reflect.Value, 0)
  
  v = reflect.Indirect(v)
  for _, e := range m.primaryKeys {
    pk = append(pk, v.Field(e.index))
  }
  
  if len (m.embeds) > 0 {
    for _, e := range m.embeds {
      if e.field.Type == m.Type {
        return nil, fmt.Errorf("Circular type embedding")
      }
      s, err := Mapping(e.field.Type)
      if err != nil {
        return nil, err
      }
      x, err := s.idValues(v.Field(e.index))
      if err != nil {
        return nil, err
      }
      pk = append(pk, x...)
    }
  }
  
  return pk, nil
}

// Obtain the single primary key identifier
func (m *mapping) Id(v reflect.Value) (interface{}, error) {
  ids, err := m.idValues(v)
  if err != nil {
    return nil, err
  }
  if len(ids) != 1 {
    return nil, fmt.Errorf("Invalid primary key count: %d != %d", len(ids), 1)
  }
  return ids[0].Interface(), nil
}

// Set the single primary key identifier
func (m *mapping) SetId(v reflect.Value, id interface{}) error {
  ids, err := m.idValues(v)
  if err != nil {
    return err
  }
  if len(ids) != 1 {
    return fmt.Errorf("Invalid primary key count: %d != %d", len(ids), 1)
  }
  f := ids[0]
  if f.Kind() != reflect.Ptr {
    f = f.Addr()
  }
  return convert.Assign(f.Interface(), id)
}

// Produce a new identifier
func (m *mapping) NewId(v reflect.Value) (interface{}, error) {
  ids, err := m.idValues(v)
  if err != nil {
    return nil, err
  }
  if len(ids) != 1 {
    return nil, fmt.Errorf("Invalid primary key count: %d != %d", len(ids), 1)
  }
  f := ids[0]
  if !f.Type().Implements(typeOfIdent) {
    return nil, fmt.Errorf("Identifier must implement %v", typeOfIdent)
  }
  return f.Interface().(Ident).New(), nil
}

// Obtain a map of property names to values
func (m *mapping) Values(v reflect.Value, pk bool, op Operation) (map[string]interface{}, error) {
  pv := make(map[string]interface{})
  v = reflect.Indirect(v)
  
  if len (m.embeds) > 0 {
    for _, e := range m.embeds {
      if e.field.Type == m.Type {
        return nil, fmt.Errorf("Circular type embedding")
      }
      s, err := Mapping(e.field.Type)
      if err != nil {
        return nil, err
      }
      x, err := s.Values(v.Field(e.index), pk, op)
      if err != nil {
        return nil, err
      }
      for k, y := range x {
        pv[k] = y
      }
    }
  }
  
  if pk {
    for n, e := range m.primaryKeys {
      pv[n] = v.Field(e.index).Interface()
    }
  }
  for n, e := range m.properties {
    if op == Read || !e.readOnly {
      pv[n] = v.Field(e.index).Interface()
    }
  }
  
  return pv, nil
}

// Obtain scanning destinations for the provided set of columns
func (m *mapping) dests(v reflect.Value, names []string) (map[string]interface{}, []string, error) {
  pv := make(map[string]interface{}, 0)
  v = reflect.Indirect(v)
  
  if len (m.embeds) > 0 {
    for _, e := range m.embeds {
      if e.field.Type == m.Type {
        return nil, nil, fmt.Errorf("Circular type embedding")
      }
      s, err := Mapping(e.field.Type)
      if err != nil {
        return nil, nil, err
      }
      var x map[string]interface{}
      x, names, err = s.dests(v.Field(e.index), names)
      if err != nil {
        return nil, nil, err
      }
      for k, z := range x {
        pv[k] = z
      }
    }
  }
  
  for i := len(names) - 1; i >= 0; i-- {
    f, ok := propSearch(names[i], m.primaryKeys, m.properties)
    if ok {
      d := v.Field(f.index).Addr()
      pv[names[i]] = d.Interface()
      names = append(names[:i], names[i+1:]...)
    }
  }
  
  return pv, names, nil
}

// Obtain scanning destinations for the provided set of columns
func (m *mapping) Dests(v reflect.Value, names []string) ([]interface{}, error) {
  n := make([]string, len(names))
  copy(n, names)
  
  d, r, err := m.dests(v, n)
  if err != nil {
    return nil, err
  }else if len(r) > 0 {
    return nil, fmt.Errorf("Unknown columns: %v", strings.Join(r, ", "))
  }
  
  x := make([]interface{}, len(d))
  for i, e := range names {
    x[i] = d[e]
  }
  
  return x, nil
}

// Search for a property in one or more maps
func propSearch(n string, in ...map[string]fieldMapping) (fieldMapping, bool) {
  for _, e := range in {
    if f, ok := e[n]; ok {
      return f, true
    }
  }
  return fieldMapping{}, false
}
