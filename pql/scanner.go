// Borrowed and modified from EPL, another little language. 
// 
// Copyright (c) 2014 Brian William Wolter, All rights reserved.
// EPL - A little Embeddable Predicate Language
// 
// Redistribution and use in source and binary forms, with or without modification,
// are permitted provided that the following conditions are met:
// 
//   * Redistributions of source code must retain the above copyright notice, this
//     list of conditions and the following disclaimer.
// 
//   * Redistributions in binary form must reproduce the above copyright notice,
//     this list of conditions and the following disclaimer in the documentation
//     and/or other materials provided with the distribution.
//     
//   * Neither the names of Brian William Wolter, Wolter Group New York, nor the
//     names of its contributors may be used to endorse or promote products derived
//     from this software without specific prior written permission.
//     
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
// ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
// WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
// IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
// INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
// BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
// LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
// OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
// OF THE POSSIBILITY OF SUCH DAMAGE.
// 
// --
// 
// This scanner incorporates routines from the Go package text/scanner:
// http://golang.org/src/pkg/text/scanner/scanner.go
// 
// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
// 
// http://golang.org/LICENSE
// 

package pql

import (
  "fmt"
  "math"
  "strings"
  "strconv"
  "unicode"
  "unicode/utf8"
)

// A text span
type span struct {
  text      string
  offset    int
  length    int
}

// Span (unquoted) excerpt
func (s span) excerpt() string {
  max := float64(len(s.text))
  return s.text[int(math.Max(0, math.Min(max, float64(s.offset)))):int(math.Min(max, float64(s.offset+s.length)))]
}

// Span (quoted) excerpt
func (s span) String() string {
  return strconv.Quote(s.excerpt())
}

// Create a new span that encompasses all the provided spans. The underlying text is taken from the first span.
func encompass(a ...span) span {
  var t string
  min, max := 0, 0
  for i, e := range a {
    if i == 0 {
      min, max = e.offset, e.offset + e.length
      t = e.text
    }else{
      if e.offset < min {
        min = e.offset
      }
      if e.offset + e.length > max {
        max = e.offset + e.length
      }
    }
  }
  return span{t, min, max - min}
}

// Token type
type tokenType int

// Token stuff
const (
  eof = -1
)

// A token
type token struct {
  span      span
  which     tokenType
  value     interface{}
}

// Stringer
func (t token) String() string {
  switch t.which {
    case tokenError:
      return fmt.Sprintf("<%v %v %v>", t.which, t.span, t.value)
    default:
      return fmt.Sprintf("<%v %v>", t.which, t.span)
  }
}

// A scanner action
type scannerAction func(*scanner) scannerAction

// A scanner
type scanner struct {
  text      string
  index     int
  width     int // current rune width
  start     int // token start position
  depth     int // expression depth
  tokens    chan token
  state     scannerAction
}

// Create a scanner
func newScanner(text string) *scanner {
  t := make(chan token, 5 /* several tokens may be produced in one iteration */)
  return &scanner{text, 0, 0, 0, 0, t, entryAction}
}

// Scan and produce a token
func (s *scanner) scan() token {
  for {
    select {
      case t := <- s.tokens:
        return t
      default:
        s.state = s.state(s)
    }
  }
}

// Create an error
func (s *scanner) errorf(where span, cause error, format string, args ...interface{}) *scannerError {
  return &scannerError{fmt.Sprintf(format, args...), where, cause}
}

// Emit a token
func (s *scanner) emit(t token) {
  s.tokens <- t
  s.start = t.span.offset + t.span.length
}

// Emit an error and return a nil action
func (s *scanner) error(err *scannerError) scannerAction {
  s.tokens <- token{err.span, tokenError, err}
  return nil
}

// Obtain the next rune from input without consuming it
func (s *scanner) peek() rune {
  r := s.next()
  s.backup()
  return r
}

// Consume the next rune from input
func (s *scanner) next() rune {
  
  if s.index >= len(s.text) {
    s.width = 0
    return eof
  }
  
  r, w := utf8.DecodeRuneInString(s.text[s.index:])
  s.index += w
  s.width  = w
  
  return r
}

// Match ahead
func (s *scanner) match(text string) bool {
  return s.matchAt(s.index, text)
}

// Match ahead
func (s *scanner) matchAt(index int, text string) bool {
  i := index
  
  if i < 0 {
    return false
  }
  
  for n := 0; n < len(text); {
    
    if i >= len(s.text) {
      return false
    }
    
    r, w := utf8.DecodeRuneInString(s.text[i:])
    i += w
    c, z := utf8.DecodeRuneInString(text[n:])
    n += z
    
    if r != c {
      return false
    }
    
  }
  
  return true
}

// Match ahead. The shortest matching string in the set will succeed.
func (s *scanner) matchAny(texts ...string) (bool, string) {
  return s.matchAnyAt(s.index, texts...)
}

// Match ahead. The shortest matching string in the set will succeed.
func (s *scanner) matchAnyAt(index int, texts ...string) (bool, string) {
  i := index
  m := 0
  
  if i < 0 {
    return false, ""
  }
  
  for _, v := range texts {
    w := len(v)
    if w > m {
      m = w
    }
  }
  
  for n := 0; n < m; {
    for _, text := range texts {
      
      if i >= len(s.text) {
        return false, ""
      }
      if n >= len(text) {
        continue
      }
      
      r, w := utf8.DecodeRuneInString(s.text[i:])
      i += w
      c, z := utf8.DecodeRuneInString(text[n:])
      n += z
      
      if r != c {
        continue
      }
      if n >= len(text) {
        return true, text
      }
      
    }
  }
  
  return false, ""
}

// Find the next occurance of any character in the specified string
func (s *scanner) findFrom(index int, any string, invert bool) int {
  i := index
  if !invert {
    return strings.IndexAny(s.text[i:], any)
  }else{
    for {
      
      if i >= len(s.text) {
        return -1
      }
      
      r, w := utf8.DecodeRuneInString(s.text[i:])
      
      if !strings.ContainsRune(any, r) {
        return i
      }else{
        i += w
      }
      
    }
  }
}

// Shuffle the token start to the current index
func (s *scanner) ignore() {
  s.start = s.index
}

// Unconsume the previous rune from input (this can be called only once
// per invocation of next())
func (s *scanner) backup() {
  s.index -= s.width
}

// Skip past a rune that was previously peeked
func (s *scanner) skip() {
  s.index += s.width
}

// Skip past a rune that was previously peeked and ignore it
func (s *scanner) skipAndIgnore() {
  s.skip()
  s.ignore()
}

// Scan an identifier
func (s *scanner) scanIdentifier() (string, error) {
  start := s.index
  for r := s.next(); r == '_' || unicode.IsLetter(r) || unicode.IsDigit(r); {
    r = s.next()
  }
  s.backup() // unget the last character
  return s.text[start:s.index], nil
}

// A scanner error
type scannerError struct {
  message   string
  span      span
  cause     error
}

// Error
func (s *scannerError) Error() string {
  if s.cause != nil {
    return fmt.Sprintf("%s: %v\n%v", s.message, s.cause, excerptCallout.FormatExcerpt(s.span))
  }else{
    return fmt.Sprintf("%s\n%v", s.message, excerptCallout.FormatExcerpt(s.span))
  }
}
