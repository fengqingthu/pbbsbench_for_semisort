// This code is part of the Problem Based Benchmark Suite (PBBS)
// Copyright (c) 2010 Guy Blelloch and the PBBS team
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

#include "parlay/parallel.h"
#include "parlay/primitives.h"
#include "common/time_loop.h"
#include "common/parse_command_line.h"
#include "common/sequenceIO.h"
#include "../parallelSemiSort/semiSort.h"
#include <iostream>
#include <algorithm>
using namespace std;
using namespace benchIO;

template <class T>
void timeSemiSort(sequence<sequence<char>> In, int rounds, char* outFile) {
  auto in_vals = parseElements<T>(In.cut(1, In.size()));
  size_t n = in_vals.size();
  parlay::sequence<record<string, T>> int_keys(n);
  const float HASH_RANGE_K = constants::HASH_RANGE_K;
  uint64_t k = pow(n, HASH_RANGE_K);
  for (int i = 0; i < n; i++) {
    record<string, T> a = {"object_" + to_string(i), in_vals[i], static_cast<int>(parlay::hash64(in_vals[i]) % k)};
    int_keys[i] = a;
  }
  sequence<record<string, T>> R;
  time_loop(rounds, 1.0,
       [&] () {
          parlay::parallel_for(0, n, [&](size_t i) {
            R[i] = int_keys[i];
          });
       },
       [&] () {semi_sort<string, T>(R);}, 
       [] () {});
  // if (outFile != NULL) writeSequenceToFile(R, outFile);
}

int main(int argc, char* argv[]) {
  commandLine P(argc,argv,"[-o <outFile>] [-r <rounds>] <inFile>");
  char* iFile = P.getArgument(0);
  char* oFile = P.getOptionValue("-o");
  int rounds = P.getOptionIntValue("-r",1);
  int bits = P.getOptionIntValue("-b", 0);

  auto In = get_tokens(iFile);
  elementType in_type = elementTypeFromHeader(In[0]);

  timeSemiSort<long>(In, rounds, oFile);
}

