#include <functional>
#include <string>
#include <fstream>
#include <iostream>
#include <algorithm>
#include <vector>
#include "Reducer.h"

struct Record {
  Record() = default;
  Record(std::string key, std::string value) : key(key), value(value) {}
  bool operator<(const Record& other) const {
    return std::lexicographical_compare(key.begin(), key.end(), other.key.begin(), other.key.end());
  }
  std::string key;
  std::string value;
};

void shuffle_data(int world_rank, int reducers_count) {
  if (world_rank == 0) {
    return;
  }
  std::string in_filename = std::to_string(world_rank) + ".out";
  std::ifstream in(in_filename);
  std::ofstream out[reducers_count];
  for (int i = 0; i < reducers_count; i++) {
    out[i].open(std::to_string(1 + i) + "." + in_filename, std::ios_base::out);
  }
  auto my_hash = std::hash<std::string>();
  while (in.good()) {
    std::string key, value;
    in >> key >> value;
    //std::cout << key << ' ' << value << std::endl;
    int bucket = my_hash(key) % reducers_count;
    out[bucket] << key << '\t' << value << std::endl;
  }

  for (int i = 0; i < reducers_count; i++) {
    out[i].close();
  }
  in.close();
}

void sort_data(int world_size, int world_rank, int reducers_count, const std::string& out_filename) {
  if (!world_rank || world_rank > reducers_count) {
    return;
  }
  std::ofstream out(out_filename);
  std::vector<Record> data;
  int bucket_num = world_rank;
  for (int i = 1; i < world_size; i++) {
    std::ifstream in(std::to_string(bucket_num) + "." + std::to_string(i) + ".out");
    std::string key, value;
    while (in.good()) {
      in >> key >> value;
      data.push_back(Record(key, value));
    }
  }
  std::sort(data.begin(), data.end());
  for (int i = 0; i < data.size(); i++) {
    out << data[i].key << '\t' << data[i].value << std::endl;
  }
  out.close();
}

ReduceInput* get_reducer_data(int world_rank, int world_size, int reducers_count) {
  ReduceInput* reduce_input = nullptr;
  shuffle_data(world_rank, reducers_count);
  MPI_Barrier(MPI_COMM_WORLD);
  std::string in_filename = std::to_string(world_rank) + "_reducer_data.in";
  sort_data(world_size, world_rank, reducers_count, in_filename);
  if (!world_rank || world_rank > reducers_count) {
    return nullptr;
  }
  return new ReduceInput(in_filename);
}