package persist

import (
  "fmt"
  "time"
  "sync"
  "reflect"
  "database/sql"
  
  "gdb"
  "gdb/pql"
)

import (
  "github.com/bww/go-util/text"
  "github.com/bww/go-util/trace"
  "github.com/bww/go-util/debug"
  "github.com/rcrowley/go-metrics"
)

// Metrics
var (
  storeDurationMetric metrics.Timer
  insertDurationMetric metrics.Timer
  updateDurationMetric metrics.Timer
  deleteDurationMetric metrics.Timer
  fetchOneDurationMetric metrics.Timer
  fetchManyDurationMetric metrics.Timer
)

// Setup metrics
func init() {
  storeDurationMetric = metrics.NewTimer()
  metrics.Register("gdb.persist.store", storeDurationMetric)
  insertDurationMetric = metrics.NewTimer()
  metrics.Register("gdb.persist.store.insert", insertDurationMetric)
  updateDurationMetric = metrics.NewTimer()
  metrics.Register("gdb.persist.store.update", updateDurationMetric)
  deleteDurationMetric = metrics.NewTimer()
  metrics.Register("gdb.persist.delete", deleteDurationMetric)
  fetchOneDurationMetric = metrics.NewTimer()
  metrics.Register("gdb.persist.fetch.one", fetchOneDurationMetric)
  fetchManyDurationMetric = metrics.NewTimer()
  metrics.Register("gdb.persist.fetch.many", fetchManyDurationMetric)
}

// Store options
type StoreOptions uint32
const (
  StoreOptionNone             = StoreOptions(0)
  StoreOptionStoreReferences  = StoreOptions(1 << 0)
  StoreOptionStoreRelated     = StoreOptions(1 << 1) | StoreOptionStoreReferences
  StoreOptionDeleteReferences = StoreOptions(1 << 2)
  StoreOptionDeleteOrphans    = StoreOptions(1 << 3) | StoreOptionDeleteReferences
  StoreOptionCascade          = StoreOptionStoreReferences | StoreOptionStoreRelated | StoreOptionDeleteReferences | StoreOptionDeleteOrphans
)

// Fetch options
type FetchOptions uint32
const (
  FetchOptionNone             = FetchOptions(0)
  FetchOptionFetchRelated     = FetchOptions(1 << 0)
  FetchOptionCascade          = FetchOptionFetchRelated
  FetchOptionConcurrent       = FetchOptions(1 << 1) // sub-fetches may be performed concurrently
)

// Persistent values
type Values map[string]interface{}

// Ident type
var typeOfIdent = reflect.TypeOf((*Ident)(nil)).Elem()

// Identifier type
type Ident interface {
  New()(interface{})
}

// Persistent type
var typeOfPersistent = reflect.TypeOf((*Persistent)(nil)).Elem()

// Implemented by persistent/persistable types
type Persistent interface {
  // Obtain the persistent table name.
  Table()(string)
}

// Mapping type
var typeOfPersistentMapping = reflect.TypeOf((*PersistentMapping)(nil)).Elem()

// Implemented by persistent/persistable types that define explicit mappings
type PersistentMapping interface {
  // Obtain the entity's primary key column names.
  PrimaryKeys()([]string)
  // Obtain the entity's column names.
  Columns()([]string)
  // Obtain the persistent identifier for an entity. If this identifier is nil or empty the entity is considered transient.
  PersistentId(interface{})(interface{})
  // Set the persistent identifier for an entity, e.g., when inserting.
  SetPersistentId(interface{}, interface{})(error)
  // Generate a new persistent identifier.
  NewPersistentId(interface{})(interface{})
  // Obtain a relational column-to-value mapping of this entity's values, EXCLUDING the primary key, which is the persistent id.
  PersistentValues(interface{})(map[string]interface{}, error)
  // Obtain scanning destinaions for the provided ordered relational columns INCLUDING the primary key. This weird interface is an artifact of sql.Rows.Scan().
  ValueDestinations(interface{}, []string)([]interface{}, error)
}

// Implemented by persistent types that explicitly generate identifiers
type GeneratesIdentifiers interface {
  // Generate and set identifiers as necessary for the provided entity
  GenerateId(interface{}, gdb.Context)(interface{}, error)
  // Determine if this entity is transient or not
  IsTransient(interface{}, gdb.Context)(bool, error)
}

// Implemented by persistent types with relationships
type StoresRelationships interface {
  // Persist dependent entities
  StoreRelated(interface{}, StoreOptions, gdb.Context)(error)
  // Persist relationships for dependent entities, but not the entities
  StoreReferences(interface{}, StoreOptions, gdb.Context)(error)
}

// Implemented by persistent types with relationships
type FetchesRelationships interface {
  // Fetch dependent entities
  FetchRelated(interface{}, FetchOptions, gdb.Context)(error)
}

// Implemented by persistent types with relationships
type DeletesRelationships interface {
  // Delete dependent entity relationships, but not the entities
  DeleteReferences(interface{}, StoreOptions, gdb.Context)(error)
  // Delete dependent entities
  DeleteRelated(interface{}, StoreOptions, gdb.Context)(error)
}

// Persister
type Persister interface {
  DefaultContext()(gdb.Context)
  
  StoreEntity(Persistent, interface{}, StoreOptions, gdb.Context)(error)
  CountEntities(Persistent, string, ...interface{})(int, error)
  FetchEntity(Persistent, interface{}, FetchOptions, gdb.Context, string, ...interface{})(error)
  FetchEntities(Persistent, interface{}, FetchOptions, gdb.Context, string, ...interface{})(error)
  DeleteEntity(Persistent, interface{}, StoreOptions, gdb.Context)(error)

  StoreRelated(Persistent, interface{}, StoreOptions, gdb.Context)(error)
  FetchRelated(Persistent, interface{}, FetchOptions, gdb.Context)(error)
  StoreReferences(Persistent, interface{}, StoreOptions, gdb.Context)(error)
  DeleteRelationships(Persistent, interface{}, StoreOptions, gdb.Context)(error)
}

// Concrete persister
type persister struct {
  cxt gdb.Context
}

// Create a persister
func New(cxt gdb.Context) Persister {
  if debug.VERBOSE {
    cxt = gdb.NewDebugContext(cxt)
  }
  return &persister{cxt}
}

// Obtain the default execution context
func (d *persister) DefaultContext() gdb.Context {
  return d.cxt
}

// Store related
func (d *persister) StoreRelated(p Persistent, v interface{}, opts StoreOptions, cxt gdb.Context) error {
  if rel, ok := p.(StoresRelationships); ok {
    if (opts & StoreOptionStoreRelated) == StoreOptionStoreRelated {
      err := rel.StoreRelated(v, opts, cxt)
      if err != nil {
        return err
      }
    }
  }
  return nil
}

// Store relationships
func (d *persister) StoreReferences(p Persistent, v interface{}, opts StoreOptions, cxt gdb.Context) error {
  if rel, ok := p.(StoresRelationships); ok {
    if (opts & StoreOptionStoreRelated) == StoreOptionStoreRelated {
      err := rel.StoreReferences(v, opts, cxt)
      if err != nil {
        return err
      }
    }
  }
  return nil
}

// Fetch relationships
func (d *persister) FetchRelated(p Persistent, v interface{}, opts FetchOptions, cxt gdb.Context) error {
  if rel, ok := p.(FetchesRelationships); ok {
    if (opts & FetchOptionFetchRelated) == FetchOptionFetchRelated {
      err := rel.FetchRelated(v, opts, cxt)
      if err != nil {
        return err
      }
    }
  }
  return nil
}

// Delete relationships
func (d *persister) DeleteRelationships(p Persistent, v interface{}, opts StoreOptions, cxt gdb.Context) error {
  if rel, ok := p.(DeletesRelationships); ok {
    if (opts & StoreOptionDeleteReferences) == StoreOptionDeleteReferences {
      err := rel.DeleteReferences(v, opts, cxt)
      if err != nil {
        return err
      }
    }
    if (opts & StoreOptionDeleteOrphans) == StoreOptionDeleteOrphans {
      err := rel.DeleteRelated(v, opts, cxt)
      if err != nil {
        return err
      }
    }
  }
  return nil
}

// Store a single persistent entity. The entity is either updated or inserted as needed.
func (d *persister) StoreEntity(p Persistent, v interface{}, opts StoreOptions, cxt gdb.Context) error {
  start := time.Now()
  defer func() { storeDurationMetric.Update(time.Since(start)) }()
  if cxt == nil {
    cxt = d.DefaultContext()
  }
  
  var m PersistentMapping
  if x, ok := p.(PersistentMapping); ok {
    m = x
  }else{
    m = newMappingEntity(v)
  }
  
  rel, relok := p.(StoresRelationships)
  if relok {
    if (opts & StoreOptionStoreRelated) == StoreOptionStoreRelated {
      err := rel.StoreRelated(v, opts, cxt)
      if err != nil {
        return err
      }
    }
  }
  
  var trans bool
  pkid := m.PersistentId(v)
  gen, genok := p.(GeneratesIdentifiers)
  if !genok {
    trans = IsEmpty(pkid)
  }else{
    var err error
    trans, err = gen.IsTransient(v, cxt)
    if err != nil {
      return err
    }
  }
  
  pvals, err := m.PersistentValues(v)
  if err != nil {
    return err
  }
  
  var kc int
  var q, kl string
  var vals []interface{}
  if trans {
    defer func() { insertDurationMetric.Update(time.Since(start)) }()
    kl, kc, vals = keyList("", pvals)
    if !genok {
      pkid = m.NewPersistentId(v)
    }else if IsEmpty(pkid) {
      pkid, err = gen.GenerateId(v, cxt)
      if err != nil {
        return err
      }
    }
    if kc > 0 { kl += ", " }; kl += "id"
    vals = append(vals, pkid)
    q = fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", p.Table(), kl, arglist(1, len(vals)))
  }else{
    defer func() { updateDurationMetric.Update(time.Since(start)) }()
    kl, kc, vals = keyValueList("", pvals)
    vals = append(vals, pkid)
    q = fmt.Sprintf("UPDATE %s SET %s WHERE id = $%d", p.Table(), kl, kc + 1)
  }
  
  _, err = cxt.Exec(q, vals...)
  if err != nil {
    return err
  }
  
  if trans { // this has to happen before we persist relationships
    err := m.SetPersistentId(v, pkid)
    if err != nil {
      return err
    }
  }
  
  if relok {
    if (opts & StoreOptionStoreReferences) == StoreOptionStoreReferences {
      err := rel.StoreReferences(v, opts, cxt)
      if err != nil {
        return err
      }
    }
  }
  
  return nil
}

// Count persistent entities.
func (d *persister) CountEntities(p Persistent, q string, v ...interface{}) (int, error) {
  var n int
  err := d.DefaultContext().QueryRow(q, v...).Scan(&n)
  if err != nil {
    return -1, err
  }
  return n, nil
}

// Fetch a single persistent entity.
func (d *persister) FetchEntity(p Persistent, v interface{}, opts FetchOptions, cxt gdb.Context, src string, args ...interface{}) error {
  start := time.Now()
  defer func() { fetchOneDurationMetric.Update(time.Since(start)) }()
  if cxt == nil {
    cxt = d.DefaultContext()
  }
  
  var m PersistentMapping
  if x, ok := p.(PersistentMapping); ok {
    m = x
  }else{
    m = newMappingEntity(v)
  }
  
  q, err := pql.Parse(src, append(m.PrimaryKeys(), m.Columns()...))
  if err != nil {
    return err
  }
  
  dest, err := m.ValueDestinations(v, q.Columns)
  if err != nil {
    return err
  }
  
  err = cxt.QueryRow(q.SQL, args...).Scan(dest...)
  if err == sql.ErrNoRows {
    return gdb.ErrNotFound
  }else if err != nil {
    return err
  }
  
  if rel, ok := p.(FetchesRelationships); ok {
    if (opts & FetchOptionFetchRelated) == FetchOptionFetchRelated {
      err = rel.FetchRelated(v, opts, cxt)
      if err != nil {
        return err
      }
    }
  }
  
  return nil
}

// Fetch many persistent entities.
func (d *persister) FetchEntities(p Persistent, r interface{}, opts FetchOptions, cxt gdb.Context, src string, args ...interface{}) error {
  tr := trace.New("trace: db/fetch/n: "+ text.CollapseSpaces(src)).Warn(time.Millisecond)
  defer tr.Finish()
  var sp *trace.Span
  
  start := time.Now()
  defer func() { fetchManyDurationMetric.Update(time.Since(start)) }()
  if cxt == nil {
    cxt = d.DefaultContext()
  }
  
  sp = tr.Start("Lookup entity type")
  var isptr bool
  rval := reflect.ValueOf(r)
  if rval.Kind() == reflect.Ptr {
    r = rval.Elem().Interface()
    isptr = true
  }else{
    fmt.Println("persist: Non-pointer value destination provided; this probably will not do what you expect.")
  }
  
  stype := reflect.TypeOf(r)
  if stype.Kind() != reflect.Slice {
    return fmt.Errorf("Argument must be a slice %T", stype)
  }
  
  btype, _ := derefType(stype.Elem())
  if btype.Kind() != reflect.Struct {
    return fmt.Errorf("Array element type must be a struct")
  }
  
  sval := reflect.ValueOf(r)
  sp.Finish()
  
  sp = tr.Start("Get or create mapping")
  var m PersistentMapping
  if x, ok := p.(PersistentMapping); ok {
    m = x
  }else{
    m = newMappingEntityForType(stype.Elem())
  }
  sp.Finish()
  
  sp = tr.Start("Parse PQL query")
  q, err := pql.Parse(src, append(m.PrimaryKeys(), m.Columns()...))
  if err != nil {
    return err
  }
  sp.Finish()
  
  sp = tr.Start("Execute query")
  rows, err := cxt.Query(q.SQL, args...)
  if err != nil {
    return err
  }
  sp.Finish()
  
  ccount := -1
  cnames, err := rows.Columns()
  if err != nil {
    return err
  }
  
  var waiter sync.WaitGroup
  sem := make(chan struct{}, 10)
  var i int
  
  spres := tr.Start("Process results")
  var discard []interface{} // discard columns, if we have extraneous fields
  defer rows.Close()
  for rows.Next() {
    v := reflect.New(btype)
    e := v.Interface()
    
    sp = spres.Start("Map destinations")
    dest, err := m.ValueDestinations(e, q.Columns)
    if err != nil {
      return err
    }
    sp.Finish()
    
    if ccount < 0 {
      ccount = len(dest)
      if n := len(cnames) - ccount; n > 0 {
        for i := 0; i < n; i++ {
          var v interface{}
          discard = append(discard, &v)
        }
      }
    }
    if discard != nil {
      dest = append(dest, discard...)
    }
    
    sp = spres.Start("Scan destinations")
    err = rows.Scan(dest...)
    if err != nil {
      return err
    }
    sp.Finish()
    
    sval = reflect.Append(sval, v) // expand, placeholder
    sem <- struct{}{}
    if err != nil {
      break // error in a previous iteration, bail out
    }
    
    waiter.Add(1)
    go func(i int, v reflect.Value, e interface{}){
      defer func(){ <-sem; waiter.Done() }()
      sp = spres.Start("Sub-fetch relationships")
      if rel, ok := p.(FetchesRelationships); ok {
        if (opts & FetchOptionFetchRelated) == FetchOptionFetchRelated {
          serr := rel.FetchRelated(e, opts, cxt)
          if serr != nil {
            err = serr; return
          }
        }
      }
      sval.Index(i).Set(v)
      sp.Finish()
    }(i, v, e)
    i++
    
  }
  waiter.Wait()
  if err != nil {
    return err // from sub-fetches
  }
  if err = rows.Err(); err != nil {
    return err
  }
  spres.Finish()
  
  if isptr {
    rval.Elem().Set(sval)
  }
  return nil
}

// Delete a persistent entity.
func (d *persister) DeleteEntity(p Persistent, v interface{}, opts StoreOptions, cxt gdb.Context) error {
  start := time.Now()
  defer func() { deleteDurationMetric.Update(time.Since(start)) }()
  if cxt == nil {
    cxt = d.DefaultContext()
  }
  
  var m PersistentMapping
  if x, ok := p.(PersistentMapping); ok {
    m = x
  }else{
    m = newMappingEntity(v)
  }
  
  pkid := m.PersistentId(v)
  if IsEmpty(pkid) {
    return gdb.ErrTransient
  }
  
  pk := m.PrimaryKeys()
  if l := len(pk); l != 1 {
    return fmt.Errorf("Invalid primary key count: %v != %v", l, 1)
  }
  kv, _, args := keyValueList("", map[string]interface{}{
    pk[0]: pkid,
  })
  
  if rel, ok := p.(DeletesRelationships); ok {
    if (opts & StoreOptionDeleteReferences) == StoreOptionDeleteReferences {
      err := rel.DeleteReferences(v, opts, cxt)
      if err != nil {
        return err
      }
    }
    if (opts & StoreOptionDeleteOrphans) == StoreOptionDeleteOrphans {
      err := rel.DeleteRelated(v, opts, cxt)
      if err != nil {
        return err
      }
    }
  }
  
  q := fmt.Sprintf("DELETE FROM %s WHERE %s", p.Table(), kv)
  _, err := cxt.Exec(q, args...)
  if err != nil {
    return err
  }
  
  return nil
}

// A list of keys, values and ordered values
func keyList(p string, e Values) (string, int, []interface{}) {
  var o []interface{}
  var l string
  
  i := 0
  for k, v := range e {
    if i > 0 { l += ", " }
    if p != "" {
      l += p +"."+ k
    }else{
      l += k
    }
    o = append(o, v)
    i++
  }
  
  return l, i, o
}

// A list of keys to values and ordered values
func keyValueList(p string, e Values) (string, int, []interface{}) {
  var o []interface{}
  var l string
  
  i := 0
  for k, v := range e {
    if i > 0 { l += ", " }
    if p != "" {
      l += p +"."+ k
    }else{
      l += k
    }
    l += fmt.Sprintf(" = $%d", i + 1)
    o = append(o, v)
    i++
  }
  
  return l, i, o
}

// Deref a type
func derefType(e reflect.Type) (reflect.Type, int) {
  var r int
  for e.Kind() == reflect.Ptr {
    e = e.Elem()
    r++
  }
  return e, r
}

// Indirect a value some number of times
func indirects(v reflect.Value, n int) reflect.Value {
  for i := 0; i < n; i++ {
    v = reflect.Indirect(v)
  }
  return v
}
