#include <mpi.h>
#include <iostream>
#include <string>
#include <vector>
#include <fcntl.h>
#include <stdlib.h>
#include "Reader.h"
#include "MapReduce.h"


class WordCounter : public Mapper {
 public:
  virtual void Map(MapInput& input) {
    char* raw_text = input.value();
    std::string text(raw_text);
    int n = text.length();
    for (int i = 0; i < n; i++) {
      while ((i < n) && isspace(text[i])) {
        i++;
      }

      int start = i;
      while ((i < n) && !isspace(text[i])) {
        i++;
      }

      if (start < i) {
        std::string word = text.substr(start, i - start);
        Emit(word, "1");
      }
    }
    delete[] raw_text;
  }
};

class Adder : public Reducer {
 public:
  Adder(const std::string& out_folder) : Reducer(out_folder) {}
  void Reduce(ReduceInput* input) override {
    int value = 0;
    while (!input->done()) {
      value += std::stoi(input->value());
      input->next_value();
    }
    Emit(input->key(), std::to_string(value));
  }
};


int main(int argc, char* argv[]) {
  std::string filenames[] = {
      "/Users/let4ik/CLionProjects/mvs/1-task/heh.txt",
      "/Users/let4ik/CLionProjects/mvs/1-task/kek.txt",
  };
  int filenum = 2;
  std::string out_folder = "/Users/let4ik/CLionProjects/mvs/1-task/bin/out";

  MapReduceParams params;
  params.block_size = 30;
  for (int i = 0; i < filenum; i++) {
    params.filenames.push_back(filenames[i]);
  }
  params.mapper = new WordCounter();
  params.reducer = new Adder(out_folder);
  params.reducers_count = 3;
  MapReduce(params);

  return 0;
}