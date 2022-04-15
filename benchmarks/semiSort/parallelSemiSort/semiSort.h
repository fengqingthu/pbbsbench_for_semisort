#pragma once
#include <stdio.h>
#include <iostream>
#include <functional>
#include <unordered_map>
#include <unordered_set>
#include <random>

#include "parlay/utilities.h"
#include "parlay/primitives.h"
#include "parlay/parallel.h"
#include "parlay/sequence.h"
#include "parlay/hash_table.h"

template <class A, class B>
struct record
{
  A obj;
  B key;
  long hashed_key;

  inline bool isEmpty() {
    return hashed_key == 0;
  }

  inline bool operator!=(record a) {
    return a.hashed_key != hashed_key || a.obj != obj || a.key != key;
  }

  inline bool operator==(record a) {
    return a.hashed_key == hashed_key && a.obj == obj && a.key == key;
  }
};

struct Bucket
{
  unsigned long long bucket_id;
  unsigned int offset;
  unsigned int size : 31;
  bool isHeavy;

  inline bool operator!=(Bucket a) {
    return a.bucket_id != bucket_id || a.offset != offset || a.size != size || a.isHeavy != isHeavy;
  }

  inline bool operator==(Bucket a)
  {
    return a.bucket_id == bucket_id && a.size == size && a.offset == offset && a.isHeavy == isHeavy;
  }
};

struct hash_buckets
{
  using eType = Bucket;
  using kType = unsigned long long;
  eType empty() { return {0, 0, 0, 0}; }
  kType getKey(eType v) { return v.bucket_id; }
  size_t hash(kType v) { return static_cast<size_t>(parlay::hash64(v)); }
  int cmp(kType v, kType b) { return (v > b) ? 1 : ((v == b) ? 0 : -1); }
  bool replaceQ(eType, eType) { return 0; }
  eType update(eType v, eType) { return v; }
  bool cas(eType *p, eType o, eType n)
  {
    auto pb = reinterpret_cast<std::atomic<unsigned long long> *>(&(p->bucket_id));
    if (std::atomic_compare_exchange_strong_explicit(pb, &(o.bucket_id), n.bucket_id, std::memory_order_relaxed, std::memory_order_relaxed)) {
      *p = n;
      return true;
    }
    return false;
  }
};

unsigned int size_func(unsigned int num_records, double p, unsigned int n, double c) {
  double lnn = (double)log((double) n);
  double clnn = c * lnn;

  double fs = (num_records + clnn + sqrt(clnn * clnn + 2 * num_records * c * clnn)) / p;
  double array_size = 1.1 * fs;

  return (unsigned int) pow(2, ceil(log(array_size) / log(2)));
}

template <class eType>
bool bucket_cas(eType *p, eType o, eType n)
{
  return std::atomic_compare_exchange_strong_explicit(
      reinterpret_cast<std::atomic<eType> *>(p), &o, n, std::memory_order_relaxed, std::memory_order_relaxed);
}

// round n down to nearest multiple of m
uint64_t round_down(uint64_t n, uint64_t m) {
  return n >= 0 ? (n / m) * m : ((n - m + 1) / m) * m;
}

// ----------------------- DECLARATION -------------------------
namespace constants {
  const float HASH_RANGE_K = 2.25;
  const float SAMPLE_PROBABILITY_CONSTANT = 1;
  const float DELTA_THRESHOLD = 1;
  const float F_C = 1.25;
  const float LIGHT_KEY_BUCKET_CONSTANT = 2;
}

using namespace std;
using parlay::parallel_for;

// #define DEBUG 1
const float HASH_RANGE_K = constants::HASH_RANGE_K;
const float SAMPLE_PROBABILITY_CONSTANT = constants::SAMPLE_PROBABILITY_CONSTANT;
const float DELTA_THRESHOLD = constants::DELTA_THRESHOLD;
const float F_C = constants::F_C;
const float LIGHT_KEY_BUCKET_CONSTANT = constants::LIGHT_KEY_BUCKET_CONSTANT;

template <class Object, class Key>
void semi_sort_with_hash(parlay::sequence<record<Object, Key> > &arr)
{
  hash<Key> hash_fn;
  uint64_t k = pow(arr.size(), HASH_RANGE_K);

  // Hash every key in parallel
  parallel_for(0, arr.size(), [&](size_t i)
               { arr[i].hashed_key = hash_fn(arr[i].key) % k + 1; });

#ifdef DEBUG
  cout << "Original Records w/ Hashed Keys: \n";
  for (uint32_t i = 0; i < arr.size(); i++)
  {
    cout << arr[i].obj << " " << arr[i].key << " " << arr[i].hashed_key << endl;
  }
#endif

  // Call the semisort function on the hashed keys
  semi_sort(arr);
}



template <class Object, class Key>
void semi_sort(parlay::sequence<record<Object, Key> > &arr)
{
  // Create a frequency map for step 4
  size_t n = arr.size();

  // Step 2
  double logn = log2((double)n);
  double p = SAMPLE_PROBABILITY_CONSTANT / logn; // this is theta(1 / log n) so we can autotune later

  uint32_t num_samples = ceil(1 / p);
  assert(num_samples != 0);

#ifdef DEBUG
  cout << "p: " << p << endl;
  cout << "cp: " << num_samples << endl;
#endif

  // Sample array
  parlay::sequence<bool> sample_index(n);

  // Choose which items to sample
  parallel_for(0, num_samples, [&](size_t i)
               { sample_index[(uint32_t)(rand() % num_samples + i / p)] = true; });

  // Pack sampled elements into smaller vector
  parlay::sequence<record<Object, Key> > sample = parlay::pack(arr, sample_index);

#ifdef DEBUG
  cout << "Sample Objects:" << endl;
  for (uint32_t i = 0; i < num_samples; i++)
  {
    cout << sample[i].obj << " " << sample[i].key << " " << sample[i].hashed_key << endl;
  }
#endif

  // Step 3 sort samples so we can more easily determine offsets
  auto comp = [&](record<Object, Key> x)
  { return x.hashed_key; };
  parlay::internal::integer_sort(parlay::make_slice(sample.begin(), sample.end()), comp, sizeof(uint64_t));

  // Step 4
  uint32_t gamma = DELTA_THRESHOLD * log(n);

#ifdef DEBUG
  cout << "Gamma: " << gamma << endl;
#endif

  parlay::sequence<uint32_t> differences(num_samples);
  // get array differecnes
  parallel_for(0, num_samples, [&](size_t i)
               {
        if (sample[i].hashed_key != sample[i+1].hashed_key){
            differences[i] = i+1;
        } });
  differences[num_samples - 1] = num_samples;

  // get offsets of differences in sorted array
  auto offset_filter = [&](uint32_t x)
  { return x != 0; };
  parlay::sequence<uint32_t> offsets = parlay::filter(differences, offset_filter);

  size_t num_unique_in_sample = offsets.size();
  parlay::sequence<uint32_t> counts(num_unique_in_sample);
  parlay::sequence<uint64_t> unique_hashed_keys(num_unique_in_sample);

  // save the unique hashed keys into an array for future use
  parallel_for(0, num_unique_in_sample, [&](size_t i)
               { unique_hashed_keys[i] = sample[offsets[i] - 1].hashed_key; });

#ifdef DEBUG
  cout << "differences, offsets, uniques" << endl;
  for (uint32_t i = 0; i < num_samples; i++)
  {
    cout << differences[i] << ", ";
  }
  cout << endl;
  for (uint32_t i = 0; i < num_unique_in_sample; i++)
  {
    cout << offsets[i] << ", ";
  }
  cout << endl;
  for (uint32_t i = 0; i < num_unique_in_sample; i++)
  {
    cout << unique_hashed_keys[i] << ", ";
  }
  cout << endl;
#endif

  // get size of heavy key buckets
  counts[0] = offsets[0];
  parallel_for(1, num_unique_in_sample, [&](size_t i)
               { counts[i] = offsets[i] - offsets[i - 1]; });

#ifdef DEBUG
  cout << "counts to bucket sizes" << endl;
  for (uint32_t i = 0; i < num_unique_in_sample; i++)
  {
    cout << counts[i] << " : " << size_func(counts[i], p, n, F_C) << endl;
  }
#endif

  // hash table T
  parlay::hashtable<hash_buckets> hash_table(n, hash_buckets());

  // calculate the number of light buckets we want
  uint32_t num_buckets = LIGHT_KEY_BUCKET_CONSTANT * ((double)n / logn / logn + 1);
  parlay::sequence<uint32_t> light_key_bucket_sample_counts(num_buckets);
  parlay::sequence<Bucket> heavy_key_buckets;

  // add heavy buckets and count number of light items in light buckets
  uint32_t current_bucket_offset = 0;
  for (uint32_t i = 0; i < num_unique_in_sample; i++)
  {
    if (counts[i] > gamma)
    {
      uint32_t bucket_size = (uint32_t)size_func(counts[i], p, n, F_C);
      Bucket new_heavy_bucket = {unique_hashed_keys[i], current_bucket_offset, bucket_size, true};
      heavy_key_buckets.push_back(new_heavy_bucket);
      current_bucket_offset += bucket_size;
    }
    else
    {
      // determine how big we should make the buckets
      uint64_t bucket_num = unique_hashed_keys[i] / num_buckets;
      light_key_bucket_sample_counts[bucket_num] += counts[i];
    }
  }

  // insert buckets into table in parallel
  parallel_for(0, heavy_key_buckets.size(), [&](size_t i)
               { hash_table.insert(heavy_key_buckets[i]); });

  // partition and create arrays for light keys here
  // 7a
  size_t nk = pow(arr.size(), HASH_RANGE_K);
  uint64_t bucket_range = (double)nk / (double)num_buckets;
  parlay::sequence<Bucket> light_buckets(num_buckets);

  for (uint32_t i = 0; i < num_buckets; i++)
  {
    uint32_t bucket_size = size_func(light_key_bucket_sample_counts[i], p, n, F_C);
    Bucket new_light_bucket = {i * bucket_range, current_bucket_offset, bucket_size, false};
    light_buckets[i] = new_light_bucket;
    current_bucket_offset += bucket_size;
  }

  parallel_for(0, num_buckets, [&](uint64_t i)
               { hash_table.insert(light_buckets[i]); });

#ifdef DEBUG
  cout << "buckets" << endl;
  parlay::sequence<Bucket> entries = hash_table.entries();
  for (uint32_t i = 0; i < entries.size(); i++)
  {
    cout << entries[i].bucket_id << " " << entries[i].offset << " " << entries[i].size << " " << entries[i].isHeavy << " " << endl;
  }
#endif

  // A' in the paper
  parlay::sequence<record<Object, Key> > buckets(current_bucket_offset + n);

  // scatter heavy keys
  uint32_t num_partitions = (int)((double)n / logn);
  parallel_for(0, num_partitions + 1, [&](size_t partition)
               {
        uint32_t end_partition = (uint32_t)((partition + 1) * logn);
        uint32_t end_state = (end_partition > n) ? n : end_partition;
        for(uint32_t i = partition * logn; i < end_state; i++) {
            Bucket entry = hash_table.find(arr[i].hashed_key);
            if (entry == (Bucket){0, 0, 0, 0}) // continue if it is not a heavy key
                continue;

            uint32_t insert_index = entry.offset + rand() % entry.size;
            while (true) {
                record<Object, Key> c = buckets[insert_index];
                if (c.isEmpty()) {
                    if (bucket_cas(&buckets[insert_index].hashed_key, 0L, arr[i].hashed_key)) {
                        buckets[insert_index] = arr[i];
                        break;
                    }
                }
                insert_index++;
            }
        } });

  // 7b
  // scatter light keys
  parallel_for(0, num_partitions + 1, [&](size_t partition)
               {
        uint32_t end_partition = (uint32_t)((partition + 1) * logn);
        uint32_t end_state = (end_partition > n) ? n : end_partition;
        for(uint32_t i = partition * logn; i < end_state; i++) {
            uint64_t rounded_down_key = round_down(arr[i].hashed_key, bucket_range);
            if (hash_table.find(arr[i].hashed_key) != (Bucket){0, 0, 0, 0}) // perhaps we can remove this somehow
                continue;

            Bucket entry = hash_table.find(rounded_down_key);
            if (entry == (Bucket){0, 0, 0, 0}) 
                continue;

            uint32_t insert_index = entry.offset + rand() % entry.size;
            while (true) {
                record<Object, Key> c = buckets[insert_index];
                if (c.isEmpty()) {
                    if (bucket_cas(&buckets[insert_index].hashed_key, 0L, arr[i].hashed_key)) {
                        buckets[insert_index] = arr[i];
                        break;
                    }
                }
                insert_index++;
            }
        } });

  // Step 7b, 7c
  auto light_key_filter = [&](record<Object, Key> x)
  { return x.hashed_key != 0; };
  auto light_key_comparison = [&](record<Object, Key> a, record<Object, Key> b)
  { return a.hashed_key < b.hashed_key; };
  parallel_for(0, num_buckets, [&](size_t i)
               {
        // sort here
        uint32_t start_range = light_buckets[i].offset;
        uint32_t end_range = light_buckets[i].offset + light_buckets[i].size;
        parlay::sort_inplace(buckets.cut(start_range, end_range), light_key_comparison); // sort light buckets

        auto out_cut = buckets.cut(start_range, end_range); // is this packing correct?
        parlay::sequence<record<Object, Key>> filtered = parlay::filter(
            buckets.cut(start_range, end_range),
            light_key_filter
        ); 
        parallel_for(start_range, end_range, [&](size_t j) {
            if (j - start_range < filtered.size()) {
                buckets[j] = filtered[j-start_range];
            } else{
                buckets[j] = (record<Object, Key>){};
                buckets[j].hashed_key = 0;
            }
        }); });

#ifdef DEBUG
  cout << "bucket" << endl;
  for (uint32_t i = 0; i < buckets.size(); i++)
  {
    cout << i << " " << buckets[i].obj << " " << buckets[i].key << " " << buckets[i].hashed_key << endl;
  }
#endif

  // step 8
  uint32_t num_partitions_step8 = min((uint32_t)1000, (uint32_t)buckets.size());
  parlay::sequence<int> interval_length(num_partitions_step8);
  parlay::sequence<int> interval_prefix_sum(num_partitions_step8);
  parallel_for(0, num_partitions_step8, [&](size_t partition) { // leq or lt?
    uint32_t chunk_length = ceil((double)buckets.size() / num_partitions_step8);
    uint32_t start_range = chunk_length * partition;
    uint32_t cur_chunk_pointer = 0;

    for (uint32_t i = 0; i < chunk_length; i++)
    {
      if (buckets[start_range + i].hashed_key != 0)
      {
        buckets[start_range + cur_chunk_pointer] = buckets[start_range + i];
        cur_chunk_pointer++;
      }
    }
    interval_length[partition] = cur_chunk_pointer;
  });

#ifdef DEBUG
  cout << "bucket after pack" << endl;
  for (uint32_t i = 0; i < buckets.size(); i++)
  {
    cout << i << " " << buckets[i].obj << " " << buckets[i].key << " " << buckets[i].hashed_key << endl;
  }
#endif

  for (uint32_t i = 1; i < num_partitions_step8; i++)
  {
    interval_prefix_sum[i] = interval_length[i-1] + interval_prefix_sum[i - 1];
  }

#ifdef DEBUG
  cout << "interval_prefix_sum" << endl;
  for (uint32_t i = 0; i < num_partitions_step8; i++)
  {
    cout << interval_prefix_sum[i] << " ";
  }
  cout << endl;
#endif

  parallel_for(0, num_partitions_step8, [&](size_t partition)
               {
        uint32_t chunk_length = ceil((double)buckets.size() / num_partitions_step8);
        uint32_t start_range = interval_prefix_sum[partition];
        for(uint32_t i = 0; i < interval_length[partition]; i++) {
            arr[start_range + i] = buckets[chunk_length * partition + i];
        } });

#ifdef DEBUG
  cout << "result" << endl;
  for (uint32_t i = 0; i < arr.size(); i++)
  {
    cout << i << " " << arr[i].obj << " " << arr[i].key << " " << arr[i].hashed_key << endl;
  }
#endif
}