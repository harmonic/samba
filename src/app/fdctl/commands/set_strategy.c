#define _GNU_SOURCE
#include "../../shared/fd_config.h"
#include "../../shared/fd_action.h"
#include "../../../disco/bundle/fd_bundle_tile_private.h"
#include "../../../disco/pack/fd_pack_tile_private.h"
#include "../../../disco/bundle/proto/block_engine.pb.h"
#include "../../../util/fd_util.h"

#include <stdlib.h>
#include <string.h>

static int
strategy_is_known( int strategy ) {
  switch( strategy ) {
  case block_engine_SchedulingStrategy_SCHEDULING_STRATEGY_FBA:
  case block_engine_SchedulingStrategy_SCHEDULING_STRATEGY_MREV:
  case block_engine_SchedulingStrategy_SCHEDULING_STRATEGY_FIFO:
    return 1;
  default:
    return 0;
  }
}

static char const *
strategy_cstr( int strategy ) {
  switch( strategy ) {
  case block_engine_SchedulingStrategy_SCHEDULING_STRATEGY_FBA:  return "fba";
  case block_engine_SchedulingStrategy_SCHEDULING_STRATEGY_MREV: return "mrev";
  case block_engine_SchedulingStrategy_SCHEDULING_STRATEGY_FIFO: return "fifo";
  default:                                                     return "unknown";
  }
}

static int
parse_strategy( char const * strategy_str ) {
  if(      !strcmp( strategy_str, "fba"  ) ) return block_engine_SchedulingStrategy_SCHEDULING_STRATEGY_FBA;
  else if( !strcmp( strategy_str, "mrev" ) ) return block_engine_SchedulingStrategy_SCHEDULING_STRATEGY_MREV;
  else if( !strcmp( strategy_str, "fifo" ) ) return block_engine_SchedulingStrategy_SCHEDULING_STRATEGY_FIFO;
  FD_LOG_ERR(( "unrecognized strategy `%s` (expected fifo, fba, or mrev)", strategy_str ));
}

static void
set_strategy_cmd_args( int *    pargc,
                       char *** pargv,
                       args_t * args ) {
  char const * strategy_str = fd_env_strip_cmdline_cstr( pargc, pargv, "--strategy", NULL, NULL );
  if( FD_UNLIKELY( !strategy_str ) ) {
    FD_LOG_ERR(( "missing --strategy argument (fifo, fba, or mrev)" ));
  }

  args->set_strategy.strategy_enum = parse_strategy( strategy_str );
  fd_cstr_ncpy( args->set_strategy.strategy, strategy_str, sizeof(args->set_strategy.strategy) );
}

static void
set_strategy_cmd_fn( args_t *   args,
                     config_t * config ) {
  ulong bundle_tile_idx = fd_topo_find_tile( &config->topo, "bundle", 0UL );
  if( FD_UNLIKELY( bundle_tile_idx==ULONG_MAX ) ) {
    FD_LOG_ERR(( "Bundle tile not found in topology (is [tiles.bundle.enabled] set?)" ));
  }

  fd_topo_tile_t const * bundle_tile = &config->topo.tiles[ bundle_tile_idx ];
  if( FD_UNLIKELY( bundle_tile->tile_obj_id==ULONG_MAX ) ) {
    FD_LOG_ERR(( "Bundle tile object not found" ));
  }

  fd_topo_obj_t const * bundle_obj = &config->topo.objs[ bundle_tile->tile_obj_id ];
  fd_topo_join_workspace( &config->topo, &config->topo.workspaces[ bundle_obj->wksp_id ], FD_SHMEM_JOIN_MODE_READ_WRITE, 0 );

  fd_bundle_tile_t * bundle_ctx = fd_topo_obj_laddr( &config->topo, bundle_tile->tile_obj_id );
  if( FD_UNLIKELY( !bundle_ctx ) ) {
    fd_topo_leave_workspaces( &config->topo );
    FD_LOG_ERR(( "Failed to access bundle tile object" ));
  }

  int const old_strategy = bundle_ctx->strategy;
  int const new_strategy = args->set_strategy.strategy_enum;

  if( FD_UNLIKELY( !strategy_is_known( old_strategy ) ) ) {
    fd_topo_leave_workspaces( &config->topo );
    FD_LOG_ERR(( "harmonic scheduling strategy in bundle tile is unknown (%d)", old_strategy ));
  }

  char const * const old_cstr = strategy_cstr( old_strategy );
  char const * const new_cstr = args->set_strategy.strategy;

  if( FD_LIKELY( old_strategy==new_strategy ) ) {
    FD_LOG_NOTICE(( "harmonic scheduling strategy: already %s (no change)", old_cstr ));
    fd_topo_leave_workspaces( &config->topo );
    return;
  }

  FD_COMPILER_MFENCE();
  bundle_ctx->strategy    = new_strategy;
  bundle_ctx->defer_reset = 1;
  FD_COMPILER_MFENCE();

  ulong pack_tile_idx = fd_topo_find_tile( &config->topo, "pack", 0UL );
  if( FD_LIKELY( pack_tile_idx!=ULONG_MAX ) ) {
    fd_topo_tile_t const * pack_tile = &config->topo.tiles[ pack_tile_idx ];
    if( FD_LIKELY( pack_tile->tile_obj_id!=ULONG_MAX ) ) {
      fd_topo_obj_t const * pack_obj = &config->topo.objs[ pack_tile->tile_obj_id ];
      fd_topo_join_workspace( &config->topo, &config->topo.workspaces[ pack_obj->wksp_id ], FD_SHMEM_JOIN_MODE_READ_WRITE, 0 );
      fd_pack_runtime_cfg_t * pack_rt = fd_topo_obj_laddr( &config->topo, pack_tile->tile_obj_id );
      if( FD_LIKELY( pack_rt ) ) {
        FD_COMPILER_MFENCE();
        pack_rt->harmonic_strategy = new_strategy;
        FD_COMPILER_MFENCE();
      }
    }
  }

  FD_LOG_NOTICE(( "harmonic scheduling strategy: %s -> %s (connection reset requested)",
                  old_cstr, new_cstr ));

  fd_topo_leave_workspaces( &config->topo );
}

action_t fd_action_set_strategy = {
  .name           = "set-strategy",
  .args           = set_strategy_cmd_args,
  .fn             = set_strategy_cmd_fn,
  .require_config = 1,
  .perm           = NULL,
  .description    = "Change harmonic scheduling strategy in the running bundle and pack tiles",
  .is_diagnostic  = 1,
};
