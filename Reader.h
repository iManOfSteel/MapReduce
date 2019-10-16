#pragma once
#include <string>
#include <vector>
#include <algorithm>
#include "Mapper.h"


Block* get_blocks(const std::vector<std::string>& filenames, int block_size, int* blocks_count) {
  std::vector<Block> blocks;
  for (int j = 0; j < filenames.size(); j++) {
    const char* filename = filenames[j].c_str();
    FILE *file = fopen(filename, "r");
    int file_size = 0;
    fseek(file, 0L, SEEK_END);
    file_size = ftell(file);
    fclose(file);
    int block_count = file_size / block_size + (file_size % block_size > 0);
    for (int i = 0; i < block_count; i++) {
      int cur_size = (i != block_count - 1) ? block_size : file_size - block_size * i;
      blocks.push_back(Block(j, i * block_size, cur_size));
    }
  }
  auto* res = new Block[blocks.size()];
  std::copy(blocks.begin(), blocks.end(), res);
  *blocks_count = blocks.size();
  return res;
}

MPI_Datatype mpi_block_type;

void create_my_datatype() {
  const int nitems = 3;
  int blocklengths[3] = {1, 1, 1};
  MPI_Datatype types[3] = {MPI_INT, MPI_INT, MPI_INT};
  MPI_Aint offsets[3];
  offsets[0] = offsetof(Block, file_num);
  offsets[1] = offsetof(Block, offset);
  offsets[2] = offsetof(Block, size);
  MPI_Type_create_struct(nitems, blocklengths, offsets, types, &mpi_block_type);
  MPI_Type_commit(&mpi_block_type);
}

MapInput distribute_data(int block_size, const std::vector<std::string>& filenames) {
  int world_size;
  MPI_Comm_size(MPI_COMM_WORLD, &world_size);
  int world_rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);

  int mapper_num = world_size - 1;
  create_my_datatype();

  int blocks_count = 0;
  Block* sendbuf = nullptr;
  int* scounts = nullptr;
  int* displs = nullptr;
  int rcount = 0;

  if (world_rank == 0) {
    scounts = new int[world_size];
    displs = new int[world_size];
    sendbuf = get_blocks(filenames, block_size, &blocks_count);
    std::cout << "Total blocks: " << blocks_count << std::endl;
    displs[0] = rcount = scounts[0] = 0;
    for (int i = 1; i < world_size; i++) {
      scounts[i] = blocks_count / mapper_num + (i <= blocks_count % mapper_num);
      displs[i] = displs[i - 1] + scounts[i - 1];
      MPI_Send(&scounts[i], 1, MPI_INT, i, 0, MPI_COMM_WORLD);
    }
  }
  else {
    MPI_Recv(&rcount, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
  }

  std::vector<Block> recvbuf_(rcount);

  MPI_Scatterv(sendbuf, scounts, displs, mpi_block_type, recvbuf_.data(), rcount, mpi_block_type, 0, MPI_COMM_WORLD);
  if (world_rank == 0) {
    delete[] scounts;
    delete[] displs;
    delete[] sendbuf;
  }
  return MapInput(std::move(recvbuf_), filenames);
}
