#include <iostream>
#include <string>
#include <mpi.h>
#include "Mapper.h"
#include "Reader.h"
#include "Reducer.h"
#include "Shuffler.h"

struct MapReduceParams {
  int block_size;
  std::vector<std::string> filenames;
  int reducers_count;
  Mapper* mapper;
  Reducer* reducer;
};


void MapReduce(const MapReduceParams& params) {
  MPI_Init(NULL, NULL);
  int world_size;
  MPI_Comm_size(MPI_COMM_WORLD, &world_size);
  int world_rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);

  MapInput map_input = distribute_data(params.block_size, params.filenames);
  if (world_rank != 0) {
    Mapper* mapper = params.mapper;
    while (!map_input.done()) {
      mapper->Map(map_input);
      map_input.next_value();
    }
    mapper->Write();
  }

  MPI_Barrier(MPI_COMM_WORLD);
  ReduceInput* reduce_input = get_reducer_data(world_rank, world_size, params.reducers_count);
  if (world_rank != 0 && world_rank <= params.reducers_count) {
    Reducer* reducer = params.reducer;
    while (!reduce_input->done_keys()) {
      reduce_input->next_key();
      reducer->Reduce(reduce_input);
    }
    reducer->Write();
  }

  delete params.mapper;
  delete params.reducer;
  MPI_Finalize();
}
