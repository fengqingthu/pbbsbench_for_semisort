#include "common/sequenceIO.h"
#include "common/parse_command_line.h"
#include <random>

using namespace benchIO;
using parlay::parallel_for;

int main(int argc, char* argv[]) {
    
    commandLine P(argc,argv,"[-r <range>] [-t {uint64_t}] <size> <outfile>");
    pair<size_t,char*> in = P.sizeAndFileName();
    size_t para = std::atoi(P.getArgument(2));
    // element type is fixed to uint64_t, which is not included in the elementTypeFromString function return value
    size_t n = in.first;
    char* fname = in.second;

    parlay::sequence<uint64_t> arr(n);
    std::default_random_engine generator;
    std::uniform_int_distribution<uint64_t> distribution(0, para);

    parallel_for (size_t i = 0; i < n; i++) {
        arr[i] = distribution(generator);
    }

    return writeSequenceToFile(arr, fname);
}