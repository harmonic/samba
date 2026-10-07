#define _GNU_SOURCE
#include "../../shared/fd_config.h"
#include "../../shared/fd_action.h"
#include "../../shared/fd_bootinfo.h"
#include "../../../disco/bundle/fd_bundle_tile_private.h"
#include "../../../disco/pack/fd_pack_tile_private.h"
#include "../../../util/fd_util.h"

static void
set_strategy_cmd_args( int *    pargc,
                       char *** pargv,
                       args_t * args ) {
  if( FD_UNLIKELY( *pargc<1 ) ) {
    FD_LOG_ERR(( "missing strategy argument (fifo, fba, or mrev)" ));
  }

  char const * strategy_str = (*pargv)[ 0 ];
  (*pargc)--;
  (*pargv)++;

  args->set_strategy.strategy_enum = fd_bundle_strategy_parse( strategy_str );
  if( FD_UNLIKELY( args->set_strategy.strategy_enum<0 ) ) FD_LOG_ERR(( "unrecognized strategy `%s` (expected fifo, fba, or mrev)", strategy_str ));
}

static void
set_strategy_cmd_fn( args_t *   args,
                     config_t * config ) {
  fd_bootinfo_adopt( config );
  fd_bootinfo_check_layout( config );
  ulong bundle_tile_idx = fd_topo_find_tile( &config->topo, "bundle", 0UL );
  if( FD_UNLIKELY( bundle_tile_idx==ULONG_MAX ) ) {
    FD_LOG_ERR(( "Bundle tile not found in topology (is [tiles.bundle.enabled] set?)" ));
  }

  fd_topo_tile_t const * bundle_tile = &config->topo.tiles[ bundle_tile_idx ];
  if( FD_UNLIKELY( bundle_tile->tile_obj_id==ULONG_MAX ) ) {
    FD_LOG_ERR(( "Bundle tile object not found" ));
  }

  fd_topo_obj_t const * bundle_obj = &config->topo.objs[ bundle_tile->tile_obj_id ];
  fd_topo_join_workspace( &config->topo, &config->topo.workspaces[ bundle_obj->wksp_id ], FD_SHMEM_JOIN_MODE_READ_WRITE, FD_TOPO_CORE_DUMP_LEVEL_DISABLED );

  fd_bundle_tile_t * bundle_ctx = fd_topo_obj_laddr( &config->topo, bundle_tile->tile_obj_id );
  if( FD_UNLIKELY( !bundle_ctx ) ) {
    fd_topo_leave_workspaces( &config->topo );
    FD_LOG_ERR(( "Failed to access bundle tile object" ));
  }

  /* Only the Harmonic block engine takes a strategy; elsewhere the
     reconnect would be for nothing */
  if( FD_UNLIKELY( !bundle_ctx->harmonic_block_mode ) ) {
    fd_topo_leave_workspaces( &config->topo );
    FD_LOG_ERR(( "set-strategy requires [tiles.bundle.harmonic_block_mode]" ));
  }

  int const old_strategy = bundle_ctx->strategy;
  int const new_strategy = args->set_strategy.strategy_enum;

  char const * const old_cstr = fd_bundle_strategy_cstr( old_strategy );
  char const * const new_cstr = fd_bundle_strategy_cstr( new_strategy );
  if( FD_UNLIKELY( !old_cstr ) ) {
    fd_topo_leave_workspaces( &config->topo );
    FD_LOG_ERR(( "harmonic scheduling strategy in bundle tile is unknown (%d)", old_strategy ));
  }

  if( FD_LIKELY( old_strategy==new_strategy ) ) {
    FD_LOG_NOTICE(( "harmonic scheduling strategy: already %s (no change)", old_cstr ));
    fd_topo_leave_workspaces( &config->topo );
    return;
  }

  /* The tile owns defer_reset (a bitfield), so signal it via strategy_seq */
  FD_COMPILER_MFENCE();
  FD_VOLATILE( bundle_ctx->strategy ) = new_strategy;
  FD_COMPILER_MFENCE();
  FD_VOLATILE( bundle_ctx->strategy_seq ) = FD_VOLATILE_CONST( bundle_ctx->strategy_seq )+1UL;
  FD_COMPILER_MFENCE();

  ulong pack_tile_idx = fd_topo_find_tile( &config->topo, "pack", 0UL );
  if( FD_LIKELY( pack_tile_idx!=ULONG_MAX ) ) {
    fd_topo_tile_t const * pack_tile = &config->topo.tiles[ pack_tile_idx ];
    if( FD_LIKELY( pack_tile->tile_obj_id!=ULONG_MAX ) ) {
      fd_topo_obj_t const * pack_obj = &config->topo.objs[ pack_tile->tile_obj_id ];
      fd_topo_join_workspace( &config->topo, &config->topo.workspaces[ pack_obj->wksp_id ], FD_SHMEM_JOIN_MODE_READ_WRITE, FD_TOPO_CORE_DUMP_LEVEL_DISABLED );
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

static void
set_strategy_args_help( fd_action_help_t * help ) {
  fd_action_help_arg( help, "<fifo|fba|mrev>", NULL, "Scheduling strategy to switch the block engine to" );
}

action_t fd_action_set_strategy = {
  .name           = "set-strategy",
  .args           = set_strategy_cmd_args,
  .fn             = set_strategy_cmd_fn,
  .require_config = 0, /* fd_bootinfo_adopt uses the running validator's published config */
  .perm           = NULL,
  .description    = "Change harmonic scheduling strategy in the running bundle and pack tiles",
  .detail         = "Overwrites the scheduling strategy in the running bundle and pack tiles,\n"
                    "then asks the bundle tile to drop its block engine connection.  The new\n"
                    "strategy is sent to the block engine with the SetStrategy RPC when the\n"
                    "connection is re-established.  The pack tile is updated so the on-chain\n"
                    "crank memo tag matches the new strategy.\n"
                    "\n"
                    "This change is not persisted.  Restarting the validator reverts to\n"
                    "[tiles.bundle.strategy] from the configuration file.\n",
  .usage          = "set-strategy <fifo|fba|mrev>",
  .args_help      = set_strategy_args_help,
  .is_diagnostic  = 1,
};
