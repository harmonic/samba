#ifndef HEADER_fd_src_disco_pack_fd_pack_tile_private_h
#define HEADER_fd_src_disco_pack_fd_pack_tile_private_h

/* Runtime-configurable pack tile state updated by fdctl set-strategy.
   Must be the first member of fd_pack_ctx_t in fd_pack_tile.c. */

struct fd_pack_runtime_cfg {
  int harmonic_strategy; /* block_engine_SchedulingStrategy value for crank memo */
};

typedef struct fd_pack_runtime_cfg fd_pack_runtime_cfg_t;

#endif /* HEADER_fd_src_disco_pack_fd_pack_tile_private_h */
