package persist

import (
  "fmt"
  "sync"
  "strings"
  "reflect"
  
  "github.com/hirepurpose/godb/convert"
)

import (
  "github.com/bww/go-util/debug"
)

const emptyName = "-"

type Operation int
const (
  Read Operation = iota
  Write
)

/**
 * Mapping cache
 */
type mappingCache struct {
  sync.RWMutex
  cache map[reflect.Type]*mapping
}

/**
 * Lookup a cached mapping
 */
func (c *mappingCache) get(t reflect.Type) (*mapping, bool) {
  c.RLock()
  m, ok := c.cache[t]
  c.RUnlock()
  return m, ok
}

/**
 * Set a cached mapping
 */
func (c *mappingCache) put(t reflect.Type, m *mapping) {
  c.Lock()
  c.cache[t] = m
  c.Unlock()
}

/**
 * Shared mapping cache
 */
var sharedCache = &mappingCache{sync.RWMutex{}, make(map[reflect.Type]*mapping)}

/**
 * A field tag
 */
type fieldTag struct {
  name        string
  primaryKey  bool
  foreignKey  bool
  readOnly    bool
  embedded    bool
}

/**
 * Parse a field tag
 */
func newFieldTag(t string) (fieldTag, error) {
  if t == "" {
    return fieldTag{}, nil
  }
  
  p := strings.Split(t, ",")
  if len(p) < 1 {
    return fieldTag{}, fmt.Errorf("Invalid struct tag")
  }
  
  f := fieldTag{name:strings.TrimSpace(p[0])}
  p  = p[1:]
  
  for _, e := range p {
    if strings.EqualFold(strings.TrimSpace(e), "pk") {
      f.primaryKey = true
    }else if strings.EqualFold(strings.TrimSpace(e), "fk") {
      f.foreignKey = true
    }else if strings.EqualFold(strings.TrimSpace(e), "ro") {
      f.readOnly = true
    }else if strings.EqualFold(strings.TrimSpace(e), "inline") {
      f.embedded = true
    }else{
      return fieldTag{}, fmt.Errorf("Unsupported struct tag argument '%s' in '%s'", e, t)
    }
  }
  return f, nil
}

/**
 * A field mapping
 */
type fieldMapping struct {
  index     int
  tag       fieldTag
  field     reflect.StructField
}

/**
 * A struct mapping
 */
type mapping struct {
  reflect.Type
  primaryKeys map[string]fieldMapping
  properties  map[string]fieldMapping
  embeds      []fieldMapping
}

/**
 * Obtain a cached mapping or create a mapping
 */
func Mapping(t reflect.Type) (*mapping, error) {
  var err error
  m, ok := sharedCache.get(t)
  if !ok { // this is a race
    m, err = newMapping(t)
    if err != nil {
      return nil, err
    }
    sharedCache.put(t, m)
  }
  return m, nil
}

/**
 * Create a new mapping for the provided type
 */
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
    
    var tag fieldTag
    var err error
    if v := f.Tag.Get("db"); v != "" {
      tag, err = newFieldTag(v)
      if err != nil {
        return nil, err
      }
    }
    
    if tag.name == emptyName {
      continue // explicitly skipped
    }
    
    if f.Anonymous {
      em = append(em, fieldMapping{i, fieldTag{}, f})
    } else {
      fm := fieldMapping{i, tag, f}
      if tag.embedded {
        em = append(em, fm)
      }else if tag.name != "" {
        if tag.primaryKey {
          pk[tag.name] = fm
        }else{
          pv[tag.name] = fm
        }
      }
    }
  }
  
  return &mapping{t, pk, pv, em}, nil
}

/**
 * Obtain a list of primary key columns
 */
func (m *mapping) PrimaryKeys() []string {
  pk := make([]string, 0)
  
  for k, _ := range m.primaryKeys {
    pk = append(pk, k)
  }
  
  for _, e := range m.embeds {
    if e.field.Type == m.Type {
      panic(fmt.Errorf("Circular type embedding for %v", m.Type))
    }
    s, err := Mapping(e.field.Type)
    if err != nil {
      panic(err)
    }
    pk = append(pk, s.PrimaryKeys()...)
  }
  
  return pk
}

// Obtain a list of property columns
func (m *mapping) Properties() []string {
  return m.props("")
}

// Obtain a list of property columns
func (m *mapping) props(prefix string) []string {
  pv := make([]string, 0)
  
  for k, _ := range m.properties {
    pv = append(pv, prefix + k)
  }
  
  for _, e := range m.embeds {
    if e.field.Type == m.Type {
      panic(fmt.Errorf("Circular type embedding for %v", m.Type))
    }
    s, err := Mapping(e.field.Type)
    if err != nil {
      panic(err)
    }
    px := prefix
    if e.tag.name != "" && e.tag.name != emptyName {
      px += e.tag.name
    }
    pv = append(pv, s.props(px)...)
  }
  
  return  pv
}

// Obtain a list of identifier values
func (m *mapping) idValues(v reflect.Value, top bool) ([]reflect.Value, error) {
  pk := make([]reflect.Value, 0)
  
  if !isValid(v) {
    if top {
      return nil, fmt.Errorf("Value of %v is nil", v.Type())
    }else{
      return pk, nil
    }
  }
  
  v = reflect.Indirect(v)
  if !v.IsValid() {
    return nil, fmt.Errorf("Value of %v is nil", v.Type())
  }
  
  for _, e := range m.primaryKeys {
    pk = append(pk, v.Field(e.index))
  }
  
  for _, e := range m.embeds {
    if e.field.Type == m.Type {
      return nil, fmt.Errorf("Circular type embedding for %v", v.Type())
    }
    s, err := Mapping(e.field.Type)
    if err != nil {
      return nil, err
    }
    x, err := s.idValues(v.Field(e.index), false)
    if err != nil {
      return nil, err
    }
    pk = append(pk, x...)
  }
  
  return pk, nil
}

// Obtain the single primary key identifier
func (m *mapping) Id(v reflect.Value) (interface{}, error) {
  ids, err := m.idValues(v, true)
  if err != nil {
    return nil, err
  }
  if len(ids) != 1 {
    return nil, fmt.Errorf("Invalid primary key count for %v: %d != %d (%s)", v.Type(), len(ids), 1, strings.Join(keys(m.primaryKeys), ", "))
  }
  return ids[0].Interface(), nil
}

// Set the single primary key identifier
func (m *mapping) SetId(v reflect.Value, id interface{}) error {
  ids, err := m.idValues(v, true)
  if err != nil {
    return err
  }
  if len(ids) != 1 {
    return fmt.Errorf("Invalid primary key count for %v: %d != %d (%s)", v.Type(), len(ids), 1, strings.Join(keys(m.primaryKeys), ", "))
  }
  f := ids[0]
  if f.Kind() != reflect.Ptr {
    f = f.Addr()
  }
  return convert.Assign(f.Interface(), id)
}

// Produce a new identifier
func (m *mapping) NewId(v reflect.Value) (interface{}, error) {
  ids, err := m.idValues(v, true)
  if err != nil {
    return nil, err
  }
  if len(ids) != 1 {
    return nil, fmt.Errorf("Invalid primary key count for %v: %d != %d (%s)", v.Type(), len(ids), 1, strings.Join(keys(m.primaryKeys), ", "))
  }
  f := ids[0]
  if !f.Type().Implements(typeOfIdent) {
    return nil, fmt.Errorf("Identifier of %v must implement %v", v.Type(), typeOfIdent)
  }
  return f.Interface().(Ident).New(), nil
}

// Obtain a map of property names to values
func (m *mapping) Values(v reflect.Value, pk bool, op Operation) (Columns, error) {
  return m.values(v, "", pk, op)
}

// Obtain a map of property names to values
func (m *mapping) values(v reflect.Value, prefix string, pk bool, op Operation) (Columns, error) {
  pv := make(Columns)
  
  if !isValid(v) {
    return pv, nil
  }
  v = reflect.Indirect(v)
  if !v.IsValid() {
    return pv, nil
  }
  
  for _, e := range m.embeds {
    if e.field.Type == m.Type {
      return nil, fmt.Errorf("Circular type embedding for %v", v.Type())
    }
    s, err := Mapping(e.field.Type)
    if err != nil {
      return nil, err
    }
    px := prefix
    if e.tag.name != "" && e.tag.name != emptyName {
      px += e.tag.name
    }
    x, err := s.values(v.Field(e.index), px, pk, op)
    if err != nil {
      return nil, err
    }
    for k, y := range x {
      pv[k] = y
    }
  }
  
  if pk {
    for n, e := range m.primaryKeys {
      pv[n] = v.Field(e.index).Interface()
    }
  }
  
  for n, e := range m.properties {
    if e.tag.foreignKey {
      f := v.Field(e.index)
      if !f.IsNil() {
        z, err := foreignKey(f)
        if err != nil {
          return nil, err
        }
        if op == Write && z != nil { // don't write nil, this can cause unintentinoal overwrites
          pv[prefix + n] = z
        }
      }
    }else if op == Read || !e.tag.readOnly {
      f := v.Field(e.index)
      if f.IsValid() {
        pv[prefix + n] = f.Interface()
      }
    }
  }
  
  return pv, nil
}

// Obtain scanning destinations for the provided set of columns
func (m *mapping) dests(p reflect.Value, q fieldMapping, v reflect.Value, prefix string, names []string) (Columns, Columns, []string, error) {
  v = reflect.Indirect(v)
  pv := make(Columns)
  px := make(Columns)
  
  for _, e := range m.embeds {
    if e.field.Type == m.Type {
      return nil, nil, nil, fmt.Errorf("Circular type embedding for %v", v.Type())
    }
    s, err := Mapping(e.field.Type)
    if err != nil {
      return nil, nil, nil, err
    }
    pf := prefix
    if e.tag.name != "" && e.tag.name != emptyName {
      pf += e.tag.name
    }
    var sv, sx map[string]interface{}
    sv, sx, names, err = s.dests(v, e, v.Field(e.index), pf, names)
    if err != nil {
      return nil, nil, nil, err
    }
    for k, z := range sv {
      pv[k] = z
    }
    for k, z := range sx {
      px[k] = z
    }
  }
  
  rem := make(map[string]struct{})
  for _, e := range names {
    rem[e] = struct{}{}
  }
  
  for _, e := range names {
    f, ok := searchProps(e, prefix, m.primaryKeys, m.properties)
    if ok {
      if !v.IsValid() {
        v = p.Field(q.index)
        if v.Kind() == reflect.Ptr {
          v.Set(reflect.New(v.Type().Elem()))
          v = reflect.Indirect(v)
        }
      }
      d := v.Field(f.index).Addr()
      if f.tag.foreignKey {
        px[e] = new(interface{})
      }else{
        pv[e] = d.Interface()
      }
      delete(rem, e)
    }
  }
  
  var i int
  mer := make([]string, len(rem))
  for k, _ := range rem {
    mer[i] = k
    i++
  }
  
  return pv, px, mer, nil
}

// Obtain scanning destinations for the provided set of columns
func (m *mapping) Dests(v reflect.Value, names []string) ([]interface{}, Columns, error) {
  
  pv, px, r, err := m.dests(reflect.Value{}, fieldMapping{}, v, "", names)
  if err != nil {
    return nil, nil, err
  }else if len(r) > 0 {
    return nil, nil, fmt.Errorf("Unknown columns for %v: %v", v.Type(), strings.Join(r, ", "))
  }
  
  d := make([]interface{}, len(names))
  for i, e := range names {
    if v, ok := pv[e]; ok {
      d[i] = v
    }else if v, ok = px[e]; ok {
      d[i] = v
    }
  }
  
  return d, px, nil
}

// Derive a foreign key from a foreign entity
func foreignKey(e reflect.Value) (interface{}, error) {
  if debug.TRACE {
    fmt.Println("persist:", e)
  }
  if e.Type().Implements(typeOfForeignEntity) {
    v := e.Interface().(ForeignEntity)
    if v == nil {
      return nil, nil
    }
    return v.ForeignKey(), nil
  }else{
    s, err := Mapping(e.Type())
    if err != nil {
      return nil, err
    }
    return s.Id(e)
  }
}

// Search for a property in one or more maps
func searchProps(n, p string, in ...map[string]fieldMapping) (fieldMapping, bool) {
  for _, e := range in {
    if f, ok := e[n]; ok {
      return f, true
    }
  }
  if l := len(p); l > 0 && strings.HasPrefix(n, p) {
    return searchProps(n[l:], "", in...)
  }
  return fieldMapping{}, false
}

// Determine if a value is valid and non-nil
func isValid(v reflect.Value) bool {
  switch v.Kind() {
    case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
      return v.IsValid() && !v.IsNil()
    default:
      return v.IsValid()
  }
}

// Get the keys
func keys(v map[string]fieldMapping) []string {
  s := make([]string, len(v))
  i := 0
  for k, _ := range v {
    s[i] = k
    i++
  }
  return s
}
