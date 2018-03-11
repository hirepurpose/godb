// Borrowed and modified from EPL, another little language.
// 
// Copyright (c) 2015 Brian William Wolter, All rights reserved.
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

package pql

import (
  "fmt"
)

// A parser
type parser struct {
  scanner   *scanner
  la        []token
}

// Create a parser
func newParser(s *scanner) *parser {
  return &parser{s, make([]token, 0, 2)}
}

// Obtain a look-ahead token without consuming it
func (p *parser) peek(n int) token {
  var t token
  
  l := len(p.la)
  if n < l {
    return p.la[n]
  }else if n + 1 > cap(p.la) {
    panic(fmt.Errorf("Look-ahead overrun: %d >= %d", n + 1, cap(p.la)))
  }
  
  p.la = p.la[:n+1]
  for i := l; i < n + 1; i++ {
    t = p.scanner.scan()
    p.la[i] = t
  }
  
  return t
}

// Consume the next token
func (p *parser) next() token {
  l := len(p.la)
  if l < 1 {
    return p.scanner.scan()
  }else{
    t := p.la[0]
    for i := 1; i < l; i++ { p.la[i-1] = p.la[i] }
    p.la = p.la[:l-1]
    return t
  }
}

// Consume the next token asserting that it is one of the provided token types
func (p *parser) nextAssert(valid ...tokenType) (token, error) {
  t := p.next()
  switch t.which {
    case tokenError:
      return token{}, fmt.Errorf("Error: %v", t)
  }
  for _, v := range valid {
    if t.which == v {
      return t, nil
    }
  }
  return token{}, invalidTokenError(t, valid...)
}

// A parser error
type parserError struct {
  message   string
  span      span
  cause     error
}

// Create a parser error
func parserErrorf(t token, f string, a ...interface{}) *parserError {
  return &parserError{span:t.span, message:fmt.Sprintf(f, a...)}
}

// Error
func (e parserError) Error() string {
  if e.cause != nil {
    return fmt.Sprintf("%s: %v\n%v", e.message, e.cause, excerptCallout.FormatExcerpt(e.span))
  }else{
    return fmt.Sprintf("%s\n%v", e.message, excerptCallout.FormatExcerpt(e.span))
  }
}

// Invalid token error
func invalidTokenError(t token, e ...tokenType) error {
  
  m := fmt.Sprintf("Invalid token: %v", t.which)
  if e != nil && len(e) > 0 {
    m += " (expected: "
    for i, t := range e {
      if i > 0 { m += ", " }
      m += fmt.Sprintf("%v", t)
    }
    m += ")"
  }
  
  return &parserError{m, t.span, nil}
}
