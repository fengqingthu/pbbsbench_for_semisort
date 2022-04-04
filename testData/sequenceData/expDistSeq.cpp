#include "common/sequenceIO.h"
#include "common/parse_command_line.h"
#include <random>

using namespace benchIO;
using parlay::parallel_for;

int main(int argc, char* argv[]) {
    
    commandLine P(argc,argv,"[-r <range>] [-t {unsigned long}] <size> <outfile>");
    pair<size_t,char*> in = P.sizeAndFileName();
    size_t para = std::atoi(P.getArgument(2));
    // element type is fixed to unsigned long, which is not included in the elementTypeFromString function return value
    size_t n = in.first;
    char* fname = in.second;

    parlay::sequence<unsigned long> arr(n);
    std::default_random_engine generator;
    std::exponential_distribution<double> distribution(para);

    parallel_for(0, n, [&](size_t i) {
        arr[i] = static_cast<unsigned long>(n * distribution(generator));
    });


    writeSeqToFile("unsigned long", arr, fname);
}