package persist

import (
  "fmt"
  "time"
  "reflect"
  
  "github.com/hirepurpose/godb"
  "github.com/hirepurpose/godb/pql"
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
  iterDurationMetric metrics.Timer
)

// Setup metrics
func init() {
  storeDurationMetric = metrics.NewTimer()
  metrics.Register("godb.persist.store", storeDurationMetric)
  insertDurationMetric = metrics.NewTimer()
  metrics.Register("godb.persist.store.insert", insertDurationMetric)
  updateDurationMetric = metrics.NewTimer()
  metrics.Register("godb.persist.store.update", updateDurationMetric)
  deleteDurationMetric = metrics.NewTimer()
  metrics.Register("godb.persist.delete", deleteDurationMetric)
  fetchOneDurationMetric = metrics.NewTimer()
  metrics.Register("godb.persist.fetch.one", fetchOneDurationMetric)
  fetchManyDurationMetric = metrics.NewTimer()
  metrics.Register("godb.persist.fetch.many", fetchManyDurationMetric)
  iterDurationMetric = metrics.NewTimer()
  metrics.Register("godb.persist.iter", iterDurationMetric)
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
  StoreOptionReserved         = StoreOptions(0xffff)  // bits reserved to the `hp/db/persist` package
  StoreUserOption             = 17                    // base for user options
)

// Fetch options
type FetchOptions uint32
const (
  FetchOptionNone             = FetchOptions(0)
  FetchOptionFetchRelated     = FetchOptions(1 << 0)
  FetchOptionCascade          = FetchOptionFetchRelated
  FetchOptionConcurrent       = FetchOptions(1 << 1)  // sub-fetches may be performed concurrently
  FetchOptionReserved         = FetchOptions(0xffff)  // bits reserved to the `hp/db/persist` package
  FetchUserOption             = 17                    // base for user options
)

// Ident type
var typeOfIdent = reflect.TypeOf((*Ident)(nil)).Elem()

// Identifier type
type Ident interface {
  // Produce a new, unique identifier
  New()(interface{})
}

// Foreign entity type
var typeOfForeignEntity = reflect.TypeOf((*ForeignEntity)(nil)).Elem()

// Implemented by foreign entities
type ForeignEntity interface {
  // Obtain the foreign key that identifies this entity
  ForeignKey()(interface{})
}

// Persistent type
var typeOfPersister = reflect.TypeOf((*Persister)(nil)).Elem()

// Implemented by persisters
type Persister interface {
  // Obtain the persistent table name.
  Table()(string)
}

// Mapping type
var typeOfPersistentMapping = reflect.TypeOf((*PersistentMapping)(nil)).Elem()

// Implemented by entities that define explicit mappings
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
  PersistentValues(interface{})(Columns, error)
  // Obtain scanning destinaions for the provided ordered relational columns INCLUDING the primary key. This weird interface is an artifact of sql.Rows.Scan().
  ValueDestinations(interface{}, []string)([]interface{}, Columns, error)
}

// Implemented by persisters that explicitly generate identifiers
type GeneratesIdentifiers interface {
  // Determine if this entity is transient or not. Within the scope of a single store operation, this method
  // will only be called by the ORM before calling GenerateId (if necessary) and setting the generated identifier.
  IsTransient(interface{}, godb.Context)(bool, error)
  // Generate and set identifiers as necessary for the provided entity
  GenerateId(interface{}, godb.Context)(interface{}, error)
}

// Implemented by persisters with relationships
type StoresRelated interface {
  // Persist dependent entities
  StoreRelated(interface{}, StoreOptions, godb.Context)(error)
}
type StoresReferences interface {
  // Persist relationships for dependent entities, but not the entities
  StoreReferences(interface{}, StoreOptions, godb.Context)(error)
}

// Implemented by persisters with relationships
type FetchesRelated interface {
  // Fetch dependent entities
  FetchRelated(interface{}, FetchOptions, godb.Context)(error)
}

// Implemented by persisters with relationships that support extra properties (e.g., for foreign keys)
type FetchesRelatedExtra interface {
  // Fetch dependent entities
  FetchRelatedExtra(interface{}, Columns, FetchOptions, godb.Context)(error)
}

// Implemented by persisters with relationships
type DeletesRelated interface {
  // Delete dependent entities
  DeleteRelated(interface{}, StoreOptions, godb.Context)(error)
}
type DeletesReferences interface {
  // Delete dependent entity relationships, but not the entities
  DeleteReferences(interface{}, StoreOptions, godb.Context)(error)
}

// An ORM
type ORM interface {
  Context(godb.Context)(godb.Context)
  DefaultContext()(godb.Context)
  
  StoreEntity(Persister, interface{}, StoreOptions, godb.Context)(error)
  CountEntities(Persister, godb.Context, string, ...interface{})(int, error)
  FetchEntity(Persister, interface{}, FetchOptions, godb.Context, string, ...interface{})(error)
  FetchEntities(Persister, interface{}, FetchOptions, godb.Context, string, ...interface{})(error)
  IterEntities(Persister, reflect.Type, FetchOptions, godb.Context, string, ...interface{})(*iter, error)
  DeleteEntity(Persister, interface{}, StoreOptions, godb.Context)(error)
  
  StoreRelated(Persister, interface{}, StoreOptions, godb.Context)(error)
  FetchRelated(Persister, interface{}, Columns, FetchOptions, godb.Context)(error)
  StoreReferences(Persister, interface{}, StoreOptions, godb.Context)(error)
  DeleteRelated(Persister, interface{}, StoreOptions, godb.Context)(error)
  DeleteReferences(Persister, interface{}, StoreOptions, godb.Context)(error)
}

// Concrete persister
type orm struct {
  cxt godb.Context
}

// Create a persister
func New(cxt godb.Context) ORM {
  if debug.VERBOSE {
    cxt = godb.NewDebugContext(cxt)
  }
  return &orm{cxt}
}

// Obtain the default execution context
func (d *orm) DefaultContext() godb.Context {
  return d.cxt
}

// Resolve the execution context
func (d *orm) Context(c godb.Context) godb.Context {
  if c != nil {
    return c
  }else{
    return d.DefaultContext()
  }
}

// Store related
func (d *orm) StoreRelated(p Persister, v interface{}, opts StoreOptions, cxt godb.Context) error {
  if (opts & StoreOptionStoreRelated) == StoreOptionStoreRelated {
    if rel, ok := p.(StoresRelated); ok {
      err := rel.StoreRelated(v, opts, cxt)
      if err != nil {
        return err
      }
    }
  }
  return nil
}

// Store relationships
func (d *orm) StoreReferences(p Persister, v interface{}, opts StoreOptions, cxt godb.Context) error {
  if (opts & StoreOptionStoreReferences) == StoreOptionStoreReferences {
    if rel, ok := p.(StoresReferences); ok {
      err := rel.StoreReferences(v, opts, cxt)
      if err != nil {
        return err
      }
    }
  }
  return nil
}

// Fetch relationships
func (d *orm) FetchRelated(p Persister, v interface{}, extra Columns, opts FetchOptions, cxt godb.Context) error {
  if (opts & FetchOptionFetchRelated) == FetchOptionFetchRelated {
    if rel, ok := p.(FetchesRelatedExtra); ok {
      err := rel.FetchRelatedExtra(v, extra, opts, cxt)
      if err != nil {
        return err
      }
    }else if rel, ok := p.(FetchesRelated); ok {
      err := rel.FetchRelated(v, opts, cxt)
      if err != nil {
        return err
      }
    }
  }
  return nil
}

// Delete relationships
func (d *orm) DeleteReferences(p Persister, v interface{}, opts StoreOptions, cxt godb.Context) error {
  if (opts & StoreOptionDeleteReferences) == StoreOptionDeleteReferences {
    if rel, ok := p.(DeletesReferences); ok {
      err := rel.DeleteReferences(v, opts, cxt)
      if err != nil {
        return err
      }
    }
  }
  return nil
}

// Delete relationships
func (d *orm) DeleteRelated(p Persister, v interface{}, opts StoreOptions, cxt godb.Context) error {
  if (opts & StoreOptionDeleteOrphans) == StoreOptionDeleteOrphans {
    if rel, ok := p.(DeletesRelated); ok {
      err := rel.DeleteRelated(v, opts, cxt)
      if err != nil {
        return err
      }
    }
  }
  return nil
}

// Store a single persistent entity. The entity is either updated or inserted as needed.
func (d *orm) StoreEntity(p Persister, v interface{}, opts StoreOptions, cxt godb.Context) error {
  start := time.Now()
  defer func() { storeDurationMetric.Update(time.Since(start)) }()
  cxt = d.Context(cxt)
  
  var tr *trace.Trace
  var sp *trace.Span
  if debug.TRACE {
    tr = trace.New(fmt.Sprintf("trace: db/store/1: (%T) %v", v, v)).Warn(time.Millisecond)
    defer tr.Finish()
  }
  
  sp = tr.Start(fmt.Sprintf("%T: Get or create mapping", v))
  var m PersistentMapping
  if x, ok := p.(PersistentMapping); ok {
    m = x
  }else{
    m = newMappingEntity(v)
  }
  sp.Finish()
  
  sp = tr.Start(fmt.Sprintf("%T: Store related entities", v))
  err := d.StoreRelated(p, v, opts, cxt)
  if err != nil {
    return err
  }
  sp.Finish()
  
  sp = tr.Start(fmt.Sprintf("%T: Resolve identifiers: %v", v, v))
  var trans bool
  pkid := m.PersistentId(v)
  gen, genok := p.(GeneratesIdentifiers)
  if !genok {
    trans = IsEmpty(pkid)
  }else{
    var err error
    trans, err = gen.IsTransient(v, cxt) // IsTransient must never be called AFTER GenerateId is called, below
    if err != nil {
      return err
    }
  }
  sp.Finish()
  
  sp = tr.Start(fmt.Sprintf("%T: Map persistent values", v))
  pvals, err := m.PersistentValues(v)
  if err != nil {
    return err
  }
  sp.Finish()
  
  sp = tr.Start(fmt.Sprintf("%T: Build query", v))
  pks := m.PrimaryKeys()
  if l := len(pks); l != 1 {
    return fmt.Errorf("Primary key count is invalid: %d != %d", l, 1)
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
    if kc > 0 { kl += ", " }; kl += pks[0]
    vals = append(vals, pkid)
    if debug.TRACE {
      names, vals := pvals.KeysVals()
      dumpMapping(v, names, vals)
    }
    q = fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", p.Table(), kl, arglist(1, len(vals)))
  }else{
    defer func() { updateDurationMetric.Update(time.Since(start)) }()
    kl, kc, vals = keyValueList("", pvals)
    vals = append(vals, pkid)
    if debug.TRACE {
      names, vals := pvals.KeysVals()
      dumpMapping(v, names, vals)
    }
    q = fmt.Sprintf("UPDATE %s SET %s WHERE %s = $%d", p.Table(), kl, pks[0], kc + 1)
  }
  sp.Finish()
  
  sp = tr.Start(fmt.Sprintf("%T: Execute query (%s)", v, q))
  _, err = cxt.Exec(q, vals...)
  if err != nil {
    return err
  }
  sp.Finish()
  
  if trans { // this has to happen before we persist relationships
    sp = tr.Start(fmt.Sprintf("%T: Set persistent ident", v))
    err := m.SetPersistentId(v, pkid)
    if err != nil {
      return err
    }
    sp.Finish()
  }
  
  sp = tr.Start(fmt.Sprintf("%T: Store references", v))
  err = d.StoreReferences(p, v, opts, cxt)
  if err != nil {
    return err
  }
  sp.Finish()
  
  return nil
}

// Count persistent entities.
func (d *orm) CountEntities(p Persister, cxt godb.Context, q string, v ...interface{}) (int, error) {
  cxt = d.Context(cxt)
  var n int
  err := cxt.QueryRow(q, v...).Scan(&n)
  if err != nil {
    return -1, err
  }
  return n, nil
}

// Fetch a single persistent entity.
func (d *orm) FetchEntity(p Persister, v interface{}, opts FetchOptions, cxt godb.Context, src string, args ...interface{}) error {
  var tr *trace.Trace
  var sp *trace.Span
  if debug.TRACE {
    tr = trace.New("trace: db/fetch/1: "+ text.CollapseSpaces(src)).Warn(time.Millisecond)
    defer tr.Finish()
  }
  
  start := time.Now()
  defer func() { fetchOneDurationMetric.Update(time.Since(start)) }()
  cxt = d.Context(cxt)
  
  sp = tr.Start("Get or create mapping")
  var m PersistentMapping
  if c, ok := p.(PersistentMapping); ok {
    m = c
  }else{
    m = newMappingEntity(v)
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
    if debug.VERBOSE {
      return fmt.Errorf("persist: Could not query row for %T (%s): %v", v, q.SQL, err)
    }else{
      return fmt.Errorf("persist: Could not query row for %T: %v", v, err)
    }
  }
  sp.Finish()
  
  it := newIter(rows, d, opts, cxt, m, p, q, tr)
  defer func() {
    if it != nil {
      it.Close()
    }
  }()
  
  if !it.Next() {
    return godb.ErrNotFound
  }
  
  err = it.Scan(v)
  if err != nil {
    return err
  }
  
  err = it.Close(); it = nil
  if err != nil {
    return err
  }
  
  return nil
}

// Fetch many persistent entities.
func (d *orm) FetchEntities(p Persister, r interface{}, opts FetchOptions, cxt godb.Context, src string, args ...interface{}) error {
  var tr *trace.Trace
  var sp *trace.Span
  if debug.TRACE {
    tr = trace.New("trace: db/fetch/n: "+ text.CollapseSpaces(src)).Warn(time.Millisecond)
    defer tr.Finish()
  }
  
  start := time.Now()
  defer func() { fetchManyDurationMetric.Update(time.Since(start)) }()
  cxt = d.Context(cxt)
  
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
  
  it := newIter(rows, d, opts, cxt, m, p, q, tr)
  defer func() {
    if it != nil {
      it.Close()
    }
  }()
  
  spres := tr.Start("Process results")
  for it.Next() {
    v := reflect.New(btype)
    e := v.Interface()
    
    err = it.Scan(e)
    if err != nil {
      return err
    }
    
    sval = reflect.Append(sval, v) // expand, placeholder
  }
  spres.Finish()
  
  err = it.Close(); it = nil
  if err != nil {
    return err
  }
  
  if isptr {
    rval.Elem().Set(sval)
  }
  return nil
}

// Fetch many persistent entities.
func (d *orm) IterEntities(p Persister, t reflect.Type, opts FetchOptions, cxt godb.Context, src string, args ...interface{}) (*iter, error) {
  var tr *trace.Trace
  var sp *trace.Span
  if debug.TRACE {
    tr = trace.New("trace: db/iter: "+ text.CollapseSpaces(src)).Warn(time.Millisecond)
    defer tr.Finish()
  }
  
  start := time.Now()
  defer func() { iterDurationMetric.Update(time.Since(start)) }()
  cxt = d.Context(cxt)
  
  sp = tr.Start("Resolve entity type")
  btype, _ := derefType(t)
  if btype.Kind() != reflect.Struct {
    return nil, fmt.Errorf("Entity must be a struct")
  }
  sp.Finish()
  
  sp = tr.Start("Get or create mapping")
  var m PersistentMapping
  if x, ok := p.(PersistentMapping); ok {
    m = x
  }else{
    m = newMappingEntityForType(btype)
  }
  sp.Finish()
  
  sp = tr.Start("Parse PQL query")
  q, err := pql.Parse(src, append(m.PrimaryKeys(), m.Columns()...))
  if err != nil {
    return nil, err
  }
  sp.Finish()
  
  sp = tr.Start("Execute query")
  rows, err := cxt.Query(q.SQL, args...)
  if err != nil {
    return nil, err
  }
  sp.Finish()
  
  return newIter(rows, d, opts, cxt, m, p, q, tr), nil
}

// Delete a persistent entity.
func (d *orm) DeleteEntity(p Persister, v interface{}, opts StoreOptions, cxt godb.Context) error {
  start := time.Now()
  defer func() { deleteDurationMetric.Update(time.Since(start)) }()
  cxt = d.Context(cxt)
  
  var m PersistentMapping
  if x, ok := p.(PersistentMapping); ok {
    m = x
  }else{
    m = newMappingEntity(v)
  }
  
  pkid := m.PersistentId(v)
  if IsEmpty(pkid) {
    return godb.ErrTransient
  }
  
  pk := m.PrimaryKeys()
  if l := len(pk); l != 1 {
    return fmt.Errorf("Invalid primary key count: %v != %v", l, 1)
  }
  kv, _, args := keyValueList("", map[string]interface{}{
    pk[0]: pkid,
  })
  
  err := d.DeleteReferences(p, v, opts, cxt)
  if err != nil {
    return err
  }
  
  err = d.DeleteRelated(p, v, opts, cxt)
  if err != nil {
    return err
  }
  
  q := fmt.Sprintf("DELETE FROM %s WHERE %s", p.Table(), kv)
  _, err = cxt.Exec(q, args...)
  if err != nil {
    return err
  }
  
  return nil
}

// A list of keys, values and ordered values
func keyList(p string, e Columns) (string, int, []interface{}) {
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
func keyValueList(p string, e Columns) (string, int, []interface{}) {
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

// Dump a mapping
func dumpMapping(v interface{}, names []string, dests []interface{}) {
  if len(names) != len(dests) {
    panic(fmt.Sprintf("persist: names and dests must be of equal lengths, but %d != %d", len(names), len(dests)))
  }
  fmt.Printf("<M> %T %v\n", v, v)
  var dlen int
  for _, e := range names {
    if l := len(e); l > dlen {
      dlen = l
    }
  }
  dfmt := fmt.Sprintf("    %% 3d: %%%ds -> (%%T) %%+v\n", dlen)
  for i, e := range dests {
    fmt.Printf(dfmt, i, names[i], e, e)
  }
}
