#include "common/sequenceIO.h"
#include "common/parse_command_line.h"
#include <random>

using namespace benchIO;
using parlay::parallel_for;

// Thanks to https://cse.usf.edu/~kchriste/tools/toolpage.html and Masoud Kazemi
//===========================================================================
//----- Include files -------------------------------------------------------
#include <assert.h>             // Needed for assert() macro
#include <stdio.h>              // Needed for printf()
#include <stdlib.h>             // Needed for exit() and ato*()
#include <math.h>               // Needed for pow()

//----- Constants -----------------------------------------------------------
#define  FALSE          0       // Boolean false
#define  TRUE           1       // Boolean true

//----- Function prototypes -------------------------------------------------
int     zipf(double alpha, int n);  // Returns a Zipf random variable
double   rand_val(int seed);         // Jain's RNG

// Helper function for zipfian distribution
int zipf(double alpha, int n)
{
  static int first = TRUE;      // Static first time flag
  static double c = 0;          // Normalization constant
  static double* sum_probs;     // Pre-calculated sum of probabilities
  double z;                     // Uniform random number (0 < z < 1)
  int zipf_value = 0;               // Computed exponential value to be returned
  int    i;                     // Loop counter
  int low, high, mid;           // Binary-search bounds

  // Compute normalization constant on first call only
  if (first == TRUE)
  {
    for (i=1; i<=n; i++)
      c = c + (1.0 / pow((double) i, alpha));
    c = 1.0 / c;

    sum_probs = (double *) malloc((n+1)*sizeof(*sum_probs));
    sum_probs[0] = 0;
    for (i=1; i<=n; i++) {
      sum_probs[i] = sum_probs[i-1] + c / pow((double) i, alpha);
    }
    first = FALSE;
  }

  // Pull a uniform random number (0 < z < 1)
  do
  {
    z = rand_val(0);
  }
  while ((z == 0) || (z == 1));

  // Map z to the value
  low = 1, high = n;
  do {
    mid = floor((low+high)/2);
    if (sum_probs[mid] >= z && sum_probs[mid-1] < z) {
      zipf_value = mid;
      break;
    } else if (sum_probs[mid] >= z) {
      high = mid-1;
    } else {
      low = mid+1;
    }
  } while (low <= high);

  // Assert that zipf_value is between 1 and N
  assert((zipf_value >=1) && (zipf_value <= n));

  return(zipf_value);
}

//=========================================================================
//= Multiplicative LCG for generating uniform(0.0, 1.0) random numbers    =
//=   - x_n = 7^5*x_(n-1)mod(2^31 - 1)                                    =
//=   - With x seeded to 1 the 10000th x value should be 1043618065       =
//=   - From R. Jain, "The Art of Computer Systems Performance Analysis," =
//=     John Wiley & Sons, 1991. (Page 443, Figure 26.2)                  =
//=========================================================================
double rand_val(int seed)
{
  const long  a =      16807;  // Multiplier
  const long  m = 2147483647;  // Modulus
  const long  q =     127773;  // m div a
  const long  r =       2836;  // m mod a
  static long x;               // Random int value
  long        x_div_q;         // x divided by q
  long        x_mod_q;         // x modulo q
  long        x_new;           // New x value

  // Set the seed if argument is non-zero and then return zero
  if (seed > 0)
  {
    x = seed;
    return(0.0);
  }

  // RNG using integer arithmetic
  x_div_q = x / q;
  x_mod_q = x % q;
  x_new = (a * x_mod_q) - (r * x_div_q);
  if (x_new > 0)
    x = x_new;
  else
    x = x_new + m;

  // Return a random value between 0.0 and 1.0
  return((double) x / m);
}

int main(int argc, char* argv[]) {

    rand_val(1);
    commandLine P(argc,argv,"[-r <range>] [-t {int}] <para> <size> <outfile>");
    pair<size_t,char*> in = P.sizeAndFileName();
    size_t para = std::atoi(P.getArgument(2));
    // element type is fixed to int, which is not included in the elementTypeFromString function return value
    size_t n = in.first;
    char* fname = in.second;

    parlay::sequence<int> arr(n);

    parallel_for(0, arr.size(), [&](size_t i) {
        // according to section 5.1: "the i-th number in this range has a probability 1/(iM-) of being chosen..."
        arr[i] = zipf(1.0, para);
    });
    writeSequenceToFile(arr,fname);
}