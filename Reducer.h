#pragma once

#include <string>
#include <vector>

class ReduceInput {
 public:
  ReduceInput(const std::string& filename) : in_(filename) {
    if (!done_keys()) {
      next_value();
    }
  }

  std::string key() {
    return cur_key_;
  }

  std::string value() {
    new_key_ = false;
    return cur_value_;
  }

  void next_value() {
    in_ >> next_key_ >> next_value_;
    if (next_key_ != cur_key_) {
      new_key_ = true;
    }
    else {
      new_key_ = false;
      cur_key_ = next_key_;
      cur_value_ = next_value_;
    }
  }

  void next_key() {
    new_key_ = false;
    cur_key_ = next_key_;
    cur_value_ = next_value_;
  }

  bool done() {
    return !in_.good() || new_key_;
  }
  bool done_keys() {
    return !in_.good();
  }
 private:
  std::ifstream in_;
  std::string cur_key_;
  std::string next_key_;
  std::string cur_value_;
  std::string next_value_;
  bool new_key_;
};

class Reducer {
 public:
  Reducer(const std::string& out_folder) : out_folder_(out_folder) {}

  virtual void Reduce(ReduceInput* input) = 0;

  virtual void Emit(const std::string& key, const std::string& value) {
    keys_.push_back(key);
    values_.push_back(value);
  }

  virtual void Write() {
    int world_rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
    std::string out_filename = out_folder_ + "/" + std::to_string(world_rank) + ".out";
    std::ofstream out(out_filename, std::ofstream::out);

    for (int i = 0; i < keys_.size(); i++) {
      out << keys_[i] << '\t' << values_[i] << '\n';
    }
    out.close();
  }

  virtual ~Reducer() = default;
 private:
  const std::string& out_folder_;
  std::vector<std::string> keys_;
  std::vector<std::string> values_;
};
