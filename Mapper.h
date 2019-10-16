#pragma once

#include <string>
#include <iostream>
#include <fstream>
#include "mpi.h"

struct Block {
  Block() = default;
  Block(int file_num, int offset, int size) : file_num(file_num), offset(offset), size(size) {}
  int file_num;
  int offset;
  int size;
};

char* read_block_data(const Block& block, const std::vector<std::string>& filenames) {
  FILE* file;
  file = fopen(filenames[block.file_num].c_str(), "r");
  fseek(file, block.offset, SEEK_SET);
  char* content = new char[block.size + 1];
  int read_size;
  read_size = fread(content, sizeof(char), block.size, file);
  content[read_size] = '\0';
  fclose(file);
  return content;
}

class MapInput {
 public:
  MapInput(std::vector<Block>&& blocks, const std::vector<std::string>& filenames)
  : blocks_(blocks), filenames_(filenames), cur_block_(0) {}

  char* value() {
    return read_block_data(blocks_[cur_block_], filenames_);
  };

  void next_value() {
    cur_block_++;
  }

  bool done() {
    return cur_block_ == blocks_.size();
  }

 private:
  std::vector<Block> blocks_;
  int cur_block_;
  const std::vector<std::string>& filenames_;
};

class Mapper {
 public:
  Mapper() = default;

  virtual void Map(MapInput &input) = 0;
  virtual void Emit(const std::string& key, const std::string& value) {
    keys_.push_back(key);
    values_.push_back(value);
  }
  virtual void Write() {
    int world_rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
    std::string out_filename = std::to_string(world_rank) + ".out";
    std::ofstream out(out_filename, std::ofstream::out);

    for (int i = 0; i < keys_.size(); i++) {
      out << keys_[i] << '\t' << values_[i] << '\n';
    }
    out.close();
  }

  virtual ~Mapper() = default;
 private:
  std::vector<std::string> keys_;
  std::vector<std::string> values_;
};
