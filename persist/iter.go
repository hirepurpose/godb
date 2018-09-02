package persist

import (
  "fmt"
  "database/sql"
  
  "github.com/hirepurpose/godb"
  "github.com/hirepurpose/godb/pql"
)

import (
  "github.com/bww/go-util/text"
  "github.com/bww/go-util/trace"
  "github.com/bww/go-util/debug"
)

// An iterator
type Iter interface {
  Next()(bool)
  Scan(interface{})(error)
  Close()(error)
}

// Concreate iterator
type iter struct {
  *sql.Rows
  orm     ORM
  opts    FetchOptions
  cxt     godb.Context
  m       PersistentMapping
  p       Persister
  q       *pql.Query
  tr      *trace.Trace
  n, cols int
  discard []interface{} // discard columns, if we have extraneous fields
}

// Create an iterator
func newIter(i *sql.Rows, o ORM, f FetchOptions, c godb.Context, m PersistentMapping, p Persister, q *pql.Query, t *trace.Trace) *iter {
  return &iter{i, o, f, c, m, p, q, t, 0, -1, nil}
}

// Determine if there is a next element, and if so advance to it
func (x *iter) Next() bool {
  return x.Rows.Next()
}

// Close this iterator. This method may be called on a nil pointer without
// effect. This is just to simplify the convention:
//   defer it.Close()
// wherein `it` may be nil because it has already been cleaned up.
func (x *iter) Close() error {
  if x != nil {
    return x.Rows.Close()
  }else{
    return nil
  }
}

// Scan an element
func (x *iter) Scan(v interface{}) error {
  if v == nil {
    return fmt.Errorf("persist: Scan target is nil")
  }
  
  defer func(){ x.n++ }()
  var sp *trace.Span
  
  sp = x.tr.Start("Map destinations")
  dest, extra, err := x.m.ValueDestinations(v, x.q.Columns)
  if err != nil {
    return err
  }
  sp.Finish()
  
  if x.n == 0 { // first iteration, setup discard columns
    if debug.TRACE {
      dumpMapping(v, x.q.Columns, dest)
    }
    sp = x.tr.Start("Resolve discard columns")
    cnames, err := x.Rows.Columns()
    if err != nil {
      return err
    }
    x.cols = len(dest)
    if n := len(cnames) - x.cols; n > 0 {
      for i := 0; i < n; i++ {
        var v interface{}
        x.discard = append(x.discard, &v)
      }
    }
    sp.Finish()
  }
  if x.discard != nil {
    dest = append(dest, x.discard...)
  }
  
  sp = x.tr.Start("Scan fields")
  err = x.Rows.Scan(dest...)
  if err != nil {
    if debug.VERBOSE {
      return fmt.Errorf("persist: Could not query rows for %T w/ %T(%v) (%s): %v", v, x.cxt, x.cxt, text.CollapseSpaces(x.q.SQL), err)
    }else{
      return fmt.Errorf("persist: Could not query rows for %T: %v", v, err)
    }
  }
  sp.Finish()
  
  sp = x.tr.Start("Fetch related")
  err = x.orm.FetchRelated(x.p, v, extra.Deref(), x.opts, x.cxt)
  if err != nil {
    if debug.VERBOSE {
      return fmt.Errorf("persist: Could not fetch related for %T w/ %T(%v) (%s): %v", v, x.cxt, x.cxt, text.CollapseSpaces(x.q.SQL), err)
    }else{
      return fmt.Errorf("persist: Could not fetch related for %T: %v", v, err)
    }
  }
  sp.Finish()
  
  return nil
}
