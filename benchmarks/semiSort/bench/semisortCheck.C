// This code is part of the Problem Based Benchmark Suite (PBBS)
// Copyright (c) 2011 Guy Blelloch and the PBBS team
//
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights (to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to
// permit persons to whom the Software is furnished to do so, subject to
// the following conditions:
//
// The above copyright notice and this permission notice shall be included
// in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
// LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
// OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
// WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

#include <iostream>
#include <algorithm>
#include <cstring>
#include <map>
#include "parlay/parallel.h"
#include "parlay/primitives.h"
#include "common/sequenceIO.h"
#include "common/atomics.h"
#include "common/parse_command_line.h"
using namespace std;
using namespace benchIO;

template <class T, class LESS>
void checkSort(sequence<sequence<char>> In,
	       sequence<sequence<char>> Out,
	       LESS less) {
  sequence<T> in_vals = parseElements<T>(In.cut(1, In.size()));
  sequence<T> out_vals = parseElements<T>(Out.cut(1, In.size()));
  size_t n = in_vals.size();
  // Create frequency map to check output against
  map<T, size_t> frequency;
  for (uint i = 0; i < n; i++) {
    if (frequency.count(in_vals[i]) == 0) {
      frequency[in_vals[i]] = 0;
    }
    frequency[in_vals[i]]++;
  }
  // Check output against frequency table
  assert(n == out_vals.size());
  uint i = 0;
  while (i < n) {
    T key = out_vals[i];
    if (frequency.count(key) == 0) {
      cout << "semisort: checked failed at location i= " << parlay::to_chars(i) << ".\n"
           << "Key found in out_vals and not input: " << parlay::to_chars(key) << endl;
      abort();
    }
    size_t freq = frequency[key];
    if (freq == 0) {
      cout << "semisort: check failed at location i= " << parlay::to_chars(i) << ".\n"
           << "Found the key '" << parlay::to_chars(key) << "' but was already found before" << endl;
      abort();
    }
    size_t end_index = i + freq;
    assert(end_index < n);
    while (i < end_index) {
      if (out_vals[i] != key) {
        cout << "semisort: check failed at location i= " << parlay::to_chars(i) << ".\n"
             << "Expected key: " << parlay::to_chars(key) << " \t but got: " << parlay::to_chars(out_vals[i]) << endl;
        abort();
      }
      i++;
    }
    frequency[key] = 0;
    i++;
  }
}

int main(int argc, char* argv[]) {
  commandLine P(argc,argv,"<inFile> <outFile>");
  pair<char*,char*> fnames = P.IOFileNames();
  char* infile = fnames.first;
  char* outfile = fnames.second;
  
  auto In = get_tokens(infile);
  elementType in_type = elementTypeFromHeader(In[0]);
  size_t in_n = In.size() - 1;

  auto Out = get_tokens(outfile);
  elementType out_type = elementTypeFromHeader(Out[0]);
  size_t out_n = In.size() - 1;

  if (in_type != out_type) {
    cout << argv[0] << ": in and out types don't match" << endl;
    return(1);
  }
  
  if (in_n != out_n) {
    cout << argv[0] << ": in and out lengths don't match" << endl;
    return(1);
  }

  auto less = [&] (uint a, uint b) {return a < b;};
  auto lessp = [&] (uintPair a, uintPair b) {return a.first < b.first;};
  
  switch (in_type) {
  case intType: 
    checkSort<uint>(In, Out, less);
    break; 
  case intPairT: 
    checkSort<uintPair>(In, Out, lessp);
    break; 
  default:
    cout << argv[0] << ": input files not of right type" << endl;
    return(1);
  }
}
