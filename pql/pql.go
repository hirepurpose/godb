package pql

import (
  "fmt"
)

import (
  "github.com/bww/go-util/debug"
)

// An identifier
type Ident struct {
  Name      string
  Wildcard  bool
}

// A property
type Property struct {
  Left  *Ident
  Right *Ident
}

// Is this a wildcard?
func (p Property) Wildcard() bool {
  if p.Right != nil {
    return p.Right.Wildcard
  }else{
    return p.Left.Wildcard
  }
}

// Obtain a property base (unprefixed) name
func (p Property) Base() string {
  if p.Right != nil {
    return p.Right.Name
  }else{
    return p.Left.Name
  }
}

// Obtain a property qualified name
func (p Property) String() string {
  if p.Right != nil {
    return p.Left.Name +"."+ p.Right.Name
  }else{
    return p.Left.Name
  }
}

// A query
type Query struct {
  SQL     string
  Columns []string
}

// Parse an entity SQL query
func Parse(q string, avail []string) (*Query, error) {
  var i, esc, open, tail int
  var c, seq rune
  var out string
  
  if debug.TRACE {
    fmt.Println("<Q>", q)
  }
  
  var cols []string
  for i, c = range q {
    switch c {
      
      case '\\':
        esc++
        
      case '\'', '"':
        if seq == 0 {
          if esc % 2 == 0 {
            if seq == 0 {
              seq = c
            }else{
              seq = 0
            }
          }
        }else if seq == c {
          if esc % 2 == 0 {
            seq = 0
          }
        }
        esc = 0
        
      case '{':
        if seq == 0 && esc % 2 == 0 {
          if open != 0 {
            return nil, fmt.Errorf("Meta sequence opened more than once")
          }
          open = i
        }
        esc = 0
        
      case '}':
        if seq == 0 && esc % 2 == 0 {
          if open == 0 {
            return nil, fmt.Errorf("Meta sequence closed but never opened")
          }
          x, err := parse(q[open+1:i])
          if err != nil {
            return nil, err
          }
          s, e, err := emitPropertyList(x, avail)
          if err != nil {
            return nil, err
          }
          out = q[tail:open] + s
          cols = append(cols, e...)
          open = 0
          tail = i + 1
        }
        esc = 0
        
    }
  }
  
  if seq != 0 {
    return nil, fmt.Errorf("Quote sequence never closed")
  }
  if i - tail > 0 {
    out += q[tail:]
  }
  
  if debug.TRACE {
    fmt.Println("<R>", out)
  }
  
  return &Query{out, cols}, nil
}

// Parse PQL. This is super simple now, so we just lex inline. If it gets
// much more elaborate we'll have to build a proper lexer.
// 
// Valid PQL is currently:
// 
//   pql :=
//     property_list
// 
//   property_list :=
//     property
//   | property ',' property_list
// 
//   property := 
//     ident
//   | ident '.' ident
//   
//   ident :=
//     '*'
//   | [a-zA-Z_][a-zA-Z0-9_]*
//  
func parse(q string) ([]*Property, error) {
  p := newParser(newScanner(q))
  e, err := parsePropertyList(p)
  if err != nil {
    return nil, err
  }else{
    return e, nil
  }
}

// Parse a property list
func parsePropertyList(p *parser) ([]*Property, error) {
  props := make([]*Property, 0)
  
  for {
    
    e, err := parseProperty(p)
    if err != nil {
      return nil, err
    }
    
    props = append(props, e)
    
    t := p.peek(0)
    if t.which == tokenEOF {
      break
    }else if t.which == tokenComma {
      p.next()
    }else{
      return nil, invalidTokenError(t, tokenComma, tokenEOF)
    }
    
  }
  
  return props, nil
}

// Parse a property expression
func parseProperty(p *parser) (*Property, error) {
  
  l, err := parseIdent(p)
  if err != nil {
    return nil, err
  }
  
  v := &Property{Left:l}
  
  t := p.peek(0)
  if t.which == tokenDot {
    if l.Wildcard {
      return nil, parserErrorf(t, "Cannot dereference wildcard identifier")
    }
    p.next()
    r, err := parseIdent(p)
    if err != nil {
      return nil, err
    }
    v.Right = r
  }
  
  return v, nil
}

// Parse an ident expression
func parseIdent(p *parser) (*Ident, error) {
  
  t := p.peek(0)
  if t.which == tokenStar {
    p.next()
    return &Ident{"*", true}, nil
  }
  
  t, err := p.nextAssert(tokenIdent)
  if err != nil {
    return nil, err
  }
  
  return &Ident{t.value.(string), false}, nil
}

// Emit a property list
func emitPropertyList(p []*Property, avail []string) (string, []string, error) {
  var q string
  
  if len(p) < 1 {
    return "", nil, nil
  }
  
  if len(p) == 1 {
    s := p[0]
    if s.Left.Wildcard || (s.Right != nil && s.Right.Wildcard) {
      base := append([]string{}, avail...)
      var x *Ident
      if s.Right != nil {
        x = s.Left
      }
      p = make([]*Property, len(base))
      for i, e := range base {
        if x != nil {
          p[i] = &Property{Left:x, Right:&Ident{Name:e}}
        }else{
          p[i] = &Property{Left:&Ident{Name:e}}
        }
      }
    }
  }
  
  cols := make([]string, len(p))
  for i, e := range p {
    if e.Wildcard() {
      return "", nil, fmt.Errorf("Invalid property: unexpected wildcard")
    }
    if i > 0 {
      q += ", "
    }
    q += e.String()
    cols[i] = e.Base()
  }
  
  return q, cols, nil
}

