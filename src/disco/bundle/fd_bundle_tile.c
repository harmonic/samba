#define _GNU_SOURCE
#include "fd_bundle_tile_private.h"
#include "fd_bundle_tile.h"
#include "fd_bundle_tpu.h"
#include "../fd_disco_base.h"
#include "../fd_txn_m.h"
#include "../metrics/fd_metrics.h"
#include "../topo/fd_topo.h"
#include "../keyguard/fd_keyload.h"
#include "../waker/fd_waker.h"
#include "../../waltz/http/fd_url.h"
#include "../../ballet/hex/fd_hex.h"
#include <errno.h>

#include <dirent.h> /* opendir */
#include <stdio.h> /* snprintf */
#include <fcntl.h> /* F_SETFL */
#include <unistd.h> /* close */
#include <sys/mman.h> /* PROT_READ (seccomp) */
#include <sys/uio.h> /* writev */
#include <netinet/in.h> /* AF_INET */
#include <netinet/tcp.h> /* TCP_FASTOPEN_CONNECT (seccomp) */
#include "../../waltz/resolv/fd_netdb.h"
#include "../../discof/replay/fd_replay_tile.h"

#include <linux/futex.h>
#include "generated/fd_bundle_tile_seccomp.h"

#define IN_KIND_REPLAY_OUT (1)

#define STEM_BURST (5UL)
FD_STATIC_ASSERT( FD_BUNDLE_CLIENT_MAX_TXN_PER_BUNDLE<=STEM_BURST, stem_burst );

/* hysteresis thresholds to avoid bouncing (e.g. during forks) */
#define FD_BUNDLE_SLEEP_THRESHOLD_SLOTS   (450UL)
#define FD_BUNDLE_WAKE_THRESHOLD_SLOTS    (400UL)
#define FD_BUNDLE_SLEEP_CHECK_INTERVAL_NS ((long)5e9)

FD_FN_CONST static ulong
scratch_align( void ) {
  return fd_ulong_max( fd_ulong_max( alignof(fd_bundle_tile_t), fd_grpc_client_align() ), pending_txn_align() );
}

FD_FN_CONST static ulong
scratch_footprint( fd_topo_tile_t const * tile ) {
  ulong pending_max = tile->bundle.out_depth;
  ulong l = FD_LAYOUT_INIT;
  l = FD_LAYOUT_APPEND( l, alignof(fd_bundle_tile_t), sizeof(fd_bundle_tile_t)                        );
  l = FD_LAYOUT_APPEND( l, fd_grpc_client_align(),    fd_grpc_client_footprint( tile->bundle.buf_sz ) );
  /* harmonic: second gRPC client for TPU endpoint */
  l = FD_LAYOUT_APPEND( l, fd_grpc_client_align(),    fd_grpc_client_footprint( tile->bundle.buf_sz ) );
  l = FD_LAYOUT_APPEND( l, pending_txn_align(),       pending_txn_footprint( pending_max )            );
  /* Harmonic staging: parallel buffer for harmonic block txns.  Sized
     to out_depth so a single fd_h2_rx pass (bounded by the gRPC client
     rx buf_sz) cannot produce more decoded txns than we can stage. */
  l = FD_LAYOUT_APPEND( l, alignof(fd_bundle_harmonic_staged_txn_t),
                           sizeof(fd_bundle_harmonic_staged_txn_t) * pending_max );
  return FD_LAYOUT_FINI( l, scratch_align() );
}

static void
fd_bundle_tile_maybe_sleep( fd_bundle_tile_t * ctx, long now_ns ) {
  /* Harmonic treats block engine connection liveness as a validator
     health signal, so never drop it between leader windows. */
  if( FD_UNLIKELY( ctx->harmonic_block_mode ) ) return;
  if( FD_UNLIKELY( !ctx->replay_in.mem ) ) return;
  if( FD_LIKELY( now_ns < ctx->sleep_check_ns ) ) return;
  ctx->sleep_check_ns = now_ns + FD_BUNDLE_SLEEP_CHECK_INTERVAL_NS;

  ulong next_leader_slot = ctx->next_leader_slot;
  ulong reset_slot       = ctx->reset_slot;

  /* Either don't know the leader schedule yet or have no upcoming
     leader slots.  Sleep. */
  if( FD_UNLIKELY( next_leader_slot==ULONG_MAX || reset_slot==ULONG_MAX ) ) {
    if( !ctx->sleep_mode ) {
      ctx->sleep_mode = 1;
      FD_LOG_INFO(( "Bundle tile entering sleep mode: no upcoming leader slots" ));
    }
    return;
  }

  ulong slots_until_leader = fd_ulong_sat_sub( next_leader_slot, reset_slot );

  if( ctx->sleep_mode ) {
    if( slots_until_leader <= FD_BUNDLE_WAKE_THRESHOLD_SLOTS ) {
      ctx->sleep_mode = 0;
      ctx->next_step_deadline = 0L;
      ctx->last_bundle_status_log_nanos = now_ns;
      FD_LOG_INFO(( "Bundle tile waking up: next leader slot %lu (~%lu slots away)", next_leader_slot, slots_until_leader ));
    }
  } else {
    /* reset_slot stays at/near next_leader_slot throughout a leader
       rotation and only jumps to the next rotation after leadership
       ends, so this cannot trigger mid-rotation. */
    if( slots_until_leader > FD_BUNDLE_SLEEP_THRESHOLD_SLOTS ) {
      ctx->sleep_mode = 1;
      FD_LOG_INFO(( "Bundle tile entering sleep mode: next leader slot %lu (~%lu slots away)", next_leader_slot, slots_until_leader ));
    }
  }
}

static inline void
metrics_write( fd_bundle_tile_t * ctx ) {
  FD_MCNT_SET( BUNDLE, TXN_RX,                 ctx->metrics.txn_received_cnt          );
  FD_MCNT_SET( BUNDLE, BUNDLE_RX,              ctx->metrics.bundle_received_cnt       );
  FD_MCNT_SET( BUNDLE, PKT_RX,                 ctx->metrics.packet_received_cnt       );
  FD_MCNT_SET( BUNDLE, PROTOBUF_RX_BYTES,         ctx->metrics.proto_received_bytes      );
  FD_MCNT_SET( BUNDLE, SHREDSTREAM_HEARTBEAT_SENT, ctx->metrics.shredstream_heartbeat_cnt );
  FD_MCNT_SET( BUNDLE, PING_ACKED,        ctx->metrics.ping_ack_cnt              );
  FD_MCNT_SET( BUNDLE, CONN_ERROR_PROTOBUF,         ctx->metrics.decode_fail_cnt           );
  FD_MCNT_SET( BUNDLE, CONN_ERROR_TRANSPORT,        ctx->metrics.transport_fail_cnt        );
  FD_MCNT_SET( BUNDLE, CONN_ERROR_NO_FEE_INFO,      ctx->metrics.missing_builder_info_fail_cnt );
  FD_MGAUGE_SET( BUNDLE, TXN_PENDING,          pending_txn_cnt( ctx->pending_txns )   );
  FD_MCNT_SET  ( BUNDLE, TXN_BUFFER_FULL,     ctx->metrics.backpressure_drop_cnt );

  FD_MGAUGE_SET( BUNDLE, RTT_SAMPLE_NANOS,   (ulong)ctx->rtt->latest_rtt   );
  FD_MGAUGE_SET( BUNDLE, RTT_SMOOTHED_NANOS, (ulong)ctx->rtt->smoothed_rtt );
  FD_MGAUGE_SET( BUNDLE, RTT_VARIANCE_NANOS, (ulong)ctx->rtt->var_rtt      );

  FD_MHIST_COPY( BUNDLE, MESSAGE_RX_DELAY_NANOS, ctx->metrics.msg_rx_delay );

  fd_wksp_t * wksp = fd_wksp_containing( ctx );
  fd_wksp_usage_t usage[1];
  ulong const free_tag = 0UL;
  if( FD_UNLIKELY( !fd_wksp_usage( wksp, &free_tag, 1UL, usage ) ) ) {
    FD_LOG_ERR(( "fd_wksp_usage failed" )); /* unreachable */
  }
  FD_MGAUGE_SET( BUNDLE, HEAP_SIZE_BYTES, usage->total_sz );
  FD_MGAUGE_SET( BUNDLE, HEAP_FREE_BYTES, usage->free_sz  );

  int status = fd_bundle_client_status( ctx );
  ulong state = (ulong)status;
  if( FD_UNLIKELY( ctx->sleep_mode ) ) state = FD_BUNDLE_STATE_SLEEPING;

  FD_MGAUGE_SET( BUNDLE, STATE, state );
  ctx->bundle_status_recent = (uchar)state;

  int tpu_status = fd_bundle_tpu_client_status( ctx );
  FD_MGAUGE_SET( BUNDLE, TPU_CONNECTED, tpu_status==FD_BUNDLE_STATE_CONNECTED );
  ctx->tpu_status_recent = (uchar)tpu_status;

  FD_MCNT_SET( BUNDLE, TPU_PACKET_RECEIVED,        ctx->metrics.tpu_packet_received_cnt    );
  FD_MCNT_SET( BUNDLE, TPU_TRANSACTION_RECEIVED,   ctx->metrics.tpu_txn_received_cnt       );
  FD_MCNT_SET( BUNDLE, LEADER_WINDOW_SUBMITTED,    ctx->metrics.leader_window_submitted_cnt );
  FD_MCNT_SET( BUNDLE, LEADER_WINDOW_FAILED,       ctx->metrics.leader_window_failed_cnt    );
  FD_MCNT_SET( BUNDLE, LEADER_WINDOW_EXPIRED,      ctx->metrics.leader_window_expired_cnt   );
  FD_MCNT_SET( BUNDLE, BLOCK_RECEIVED,               ctx->harmonic_block_received_cnt      );
  FD_MCNT_SET( BUNDLE, BLOCK_TRANSACTION_RECEIVED,   ctx->harmonic_block_txn_received_cnt  );
}

void
fd_bundle_tile_housekeeping( fd_bundle_tile_t * ctx ) {
  long log_interval_ns = (long)30e9;
  int  status          = fd_bundle_client_status( ctx );
  long log_next_ns     = ctx->last_bundle_status_log_nanos + log_interval_ns;
  long now_ns          = fd_log_wallclock();

  if( FD_UNLIKELY( !ctx->sleep_mode && status!=FD_BUNDLE_STATE_CONNECTED && now_ns>log_next_ns ) ) {
    FD_LOG_WARNING(( "No bundle server connection in the last %ld seconds", log_interval_ns/(long)1e9 ) );
    ctx->last_bundle_status_log_nanos = now_ns;
  }

  if( FD_UNLIKELY( ctx->tpu_conn_enabled ) ) {
    int tpu_status = fd_bundle_tpu_client_status( ctx );
    if( FD_UNLIKELY( tpu_status!=FD_BUNDLE_STATE_CONNECTED && now_ns>log_next_ns ) ) {
      FD_LOG_WARNING(( "No TPU endpoint connection (status=%d sock=%d sock_conn=%d auth=%d cfg_avail=%d cfg_wait=%d sub_live=%d sub_wait=%d defer_reset=%d)",
                       tpu_status,
                       ctx->tpu_tcp_sock,
                       ctx->tpu_tcp_sock_connected,
                       ctx->tpu_auther.state,
                       ctx->tpu_config_avail,
                       ctx->tpu_config_wait,
                       ctx->tpu_packet_subscription_live,
                       ctx->tpu_packet_subscription_wait,
                       ctx->tpu_defer_reset ));
      ctx->last_bundle_status_log_nanos = now_ns;
    }
  }

  if( FD_UNLIKELY( fd_keyswitch_state_query( ctx->keyswitch )==FD_KEYSWITCH_STATE_SWITCH_PENDING ) ) {
    if( ctx->tcp_sock>=0 ) fd_bundle_client_reset( ctx );
    ctx->halt_signing = 1;
    fd_memcpy( ctx->auther.pubkey, ctx->keyswitch->bytes, 32UL );

    /* Harmonic: also update TPU auther pubkey and reset TPU connection */
    if( ctx->tpu_conn_enabled ) {
      fd_memcpy( ctx->tpu_auther.pubkey, ctx->keyswitch->bytes, 32UL );
      ctx->tpu_defer_reset = 1;
    }
    fd_keyswitch_state( ctx->keyswitch, FD_KEYSWITCH_STATE_COMPLETED );
  }

  if( FD_UNLIKELY( fd_keyswitch_state_query( ctx->keyswitch )==FD_KEYSWITCH_STATE_UNHALT_PENDING ) ) {
    ctx->defer_reset        = 1;
    ctx->sleep_check_ns     = 0;
    ctx->halt_signing       = 0;
    ctx->next_step_deadline = 0L; /* the socket was closed outside a step: step now */
    fd_keyswitch_state( ctx->keyswitch, FD_KEYSWITCH_STATE_COMPLETED );
  }

  fd_bundle_tile_maybe_sleep( ctx, now_ns );
}

static void
fd_bundle_tile_publish_block_engine_update(
    fd_bundle_tile_t *  ctx,
    fd_stem_context_t * stem
) {
  fd_bundle_block_engine_update_t * update =
      fd_chunk_to_laddr( ctx->plugin_out.mem, ctx->plugin_out.chunk );
  memset( update, 0, sizeof(fd_bundle_block_engine_update_t) );

  strncpy( update->name, "jito", sizeof(update->name) );

  FD_TEST( fd_cstr_printf_check( update->url, sizeof(update->url), NULL,
                                "%s://%.*s:%u",
                                ctx->is_ssl ? "https" : "http",
                                (int)ctx->server_fqdn_len,
                                ctx->server_fqdn,
                                ctx->server_tcp_port ) );

  /* Format IPv4 string */
  snprintf( update->ip_cstr, sizeof(update->ip_cstr),
            FD_IP4_ADDR_FMT,
            FD_IP4_ADDR_FMT_ARGS( ctx->server_ip4_addr ) );

  ulong tspub = (ulong)fd_frag_meta_ts_comp( fd_bundle_now( ctx ) );
  fd_stem_publish(
      stem,
      ctx->plugin_out.idx,
      (ulong)ctx->bundle_status_recent,
      ctx->plugin_out.chunk,
      sizeof(fd_bundle_block_engine_update_t),
      0UL, /* ctl */
      0UL, /* seq */
      tspub
  );
  ctx->plugin_out.chunk = fd_dcache_compact_next( ctx->plugin_out.chunk, sizeof(fd_bundle_block_engine_update_t), ctx->plugin_out.chunk0, ctx->plugin_out.wmark );
}

static void
during_frag( fd_bundle_tile_t * ctx,
             ulong              in_idx,
             ulong              seq    FD_PARAM_UNUSED,
             ulong              sig,
             ulong              chunk,
             ulong              sz     FD_PARAM_UNUSED,
             ulong              ctl    FD_PARAM_UNUSED ) {

  if( FD_UNLIKELY( ctx->in_kind[ in_idx ]==IN_KIND_REPLAY_OUT ) ) {
    if( FD_UNLIKELY( sig==REPLAY_SIG_BECAME_LEADER ) ) {
      if( FD_UNLIKELY( chunk<ctx->replay_in.chunk0 || chunk>ctx->replay_in.wmark || sz!=sizeof(fd_became_leader_t) ) )
        FD_LOG_ERR(( "chunk %lu %lu corrupt, not in range [%lu,%lu]", chunk, sz, ctx->replay_in.chunk0, ctx->replay_in.wmark ));
      fd_memcpy( ctx->_became_leader, fd_chunk_to_laddr_const( ctx->replay_in.mem, chunk ), sizeof(fd_became_leader_t) );
      return;
    }
    if( FD_LIKELY( sig!=REPLAY_SIG_RESET ) ) return;
    if( FD_UNLIKELY( chunk<ctx->replay_in.chunk0 || chunk>ctx->replay_in.wmark || sz!=sizeof(fd_poh_reset_t) ) )
      FD_LOG_ERR(( "chunk %lu %lu corrupt, not in range [%lu,%lu]", chunk, sz, ctx->replay_in.chunk0, ctx->replay_in.wmark ));

    fd_poh_reset_t const * reset = fd_chunk_to_laddr_const( ctx->replay_in.mem, chunk );
    ctx->next_leader_slot_staged = reset->next_leader_slot;
    ctx->reset_slot_staged       = reset->completed_slot;
  }
}

static void
after_frag( fd_bundle_tile_t *  ctx,
            ulong               in_idx,
            ulong               seq    FD_PARAM_UNUSED,
            ulong               sig,
            ulong               sz     FD_PARAM_UNUSED,
            ulong               tsorig FD_PARAM_UNUSED,
            ulong               tspub  FD_PARAM_UNUSED,
            fd_stem_context_t * stem   FD_PARAM_UNUSED ) {

  if( FD_UNLIKELY( ctx->in_kind[ in_idx ]==IN_KIND_REPLAY_OUT ) ) {
    if( FD_UNLIKELY( sig==REPLAY_SIG_BECAME_LEADER ) ) {
      if( FD_UNLIKELY( !ctx->harmonic_block_mode ) ) return; /* Jito engines have no leader window RPC */
      /* Bundles for this slot are requested from here on, so nothing has
         been lost yet: numbering for the next block starts at 1. */
      ctx->harmonic_seq_tainted = 0;
      fd_bundle_client_queue_leader_window_info( ctx, ctx->_became_leader->slot, ctx->_became_leader->slot_end_ns );
      ctx->next_step_deadline = 0L; /* queued outside a step: step now to send it */
      return;
    }
    if( FD_LIKELY( sig!=REPLAY_SIG_RESET ) ) return;
    ctx->next_leader_slot = ctx->next_leader_slot_staged;
    ctx->reset_slot       = ctx->reset_slot_staged;
  }
}

static long
next_deadline( fd_bundle_tile_t * ctx ) {
  if( FD_UNLIKELY( ctx->halt_signing || ctx->sleep_mode || ctx->next_step_deadline==LONG_MAX ) ) return LONG_MAX;
  return fd_clock_tile_wallclock_to_tickcount( ctx->clock, ctx->next_step_deadline );
}

static void
before_credit( fd_bundle_tile_t *  ctx,
               fd_stem_context_t * stem,
               int *               charge_busy ) {
  if( FD_UNLIKELY( !ctx->stem ) ) {
    ctx->stem = stem;
  }

  if( FD_UNLIKELY( ctx->halt_signing || ctx->sleep_mode ) ) {
    if( ctx->sleep_mode && ctx->tcp_sock>=0 ) {
      fd_bundle_client_reset( ctx );
      /* Override backoff so we don't treat this as an error */
      ctx->backoff_until = 0;
      ctx->backoff_iter  = 0;
    }
    /* The socket is closed so any wake (even one that raced the close)
       is stale: drain it or the stem never parks, and rearm so the next
       connection can wake us. */
    if( FD_UNLIKELY( fd_fseq_query( ctx->waker_fseq )==1UL ) ) {
      fd_fseq_update( ctx->waker_fseq, 0UL );
      fd_waker_client_rearm( ctx->waker_client_idx );
    }
    return;
  }

  /* Defer gRPC while harmonic staging waits for after_credit so block
     stream order cannot run ahead of verify_out publishes. */
  if( FD_UNLIKELY( ctx->harmonic_pending_len ) ) return;

  if( pending_txn_empty( ctx->pending_txns ) ) {
    int  fired = fd_fseq_query( ctx->waker_fseq )==1UL;
    long now   = fd_bundle_now( ctx );
    if( FD_UNLIKELY( fired || now>=ctx->next_step_deadline ) ) {
      if( FD_LIKELY( fired ) ) fd_fseq_update( ctx->waker_fseq, 0UL );
      int busy = 0;
      fd_bundle_client_step( ctx, &busy );
      if( FD_LIKELY( fired ) ) fd_waker_client_rearm( ctx->waker_client_idx );
      *charge_busy = busy;
      /* A step that made progress may have more buffered work: re-step
         immediately. */
      ctx->next_step_deadline = busy ? 0L : fd_bundle_client_next_deadline( ctx, fd_bundle_now( ctx ) );
    }
  }
}

/* Publish TPU connection update to gossip link.
   For Frankendancer, the poh tile (Agave) receives this.
   For full Firedancer, the gossip tile receives this. */
static void
fd_bundle_tile_publish_tpu_update(
    fd_bundle_tile_t *  ctx,
    fd_stem_context_t * stem
) {
  fd_bundle_tpu_update_t * update =
      fd_chunk_to_laddr( ctx->gossip_out.mem, ctx->gossip_out.chunk );
  memset( update, 0, sizeof(fd_bundle_tpu_update_t) );

  int is_connected = ( ctx->tpu_status_recent == FD_BUNDLE_STATE_CONNECTED );
  update->status = is_connected ? FD_BUNDLE_TPU_UPDATE_CONNECTED : FD_BUNDLE_TPU_UPDATE_DISCONNECTED;

  /* When connected, populate TPU addresses from cached GetTpuConfigs response */
  if( is_connected && ctx->tpu_config_avail ) {
    update->tpu_ip4_addr     = ctx->tpu_config_tpu_ip4_addr;
    update->tpu_port         = ctx->tpu_config_tpu_port;
    update->tpu_fwd_ip4_addr = ctx->tpu_config_tpu_fwd_ip4_addr;
    update->tpu_fwd_port     = ctx->tpu_config_tpu_fwd_port;
  }

  if( is_connected ) {
    FD_LOG_NOTICE(( "Publishing TPU update: status=CONNECTED tpu=%u.%u.%u.%u:%u tpu_fwd=%u.%u.%u.%u:%u",
                    (update->tpu_ip4_addr    ) & 0xFFU, (update->tpu_ip4_addr>>8    ) & 0xFFU,
                    (update->tpu_ip4_addr>>16) & 0xFFU, (update->tpu_ip4_addr>>24   ) & 0xFFU,
                    update->tpu_port,
                    (update->tpu_fwd_ip4_addr    ) & 0xFFU, (update->tpu_fwd_ip4_addr>>8    ) & 0xFFU,
                    (update->tpu_fwd_ip4_addr>>16) & 0xFFU, (update->tpu_fwd_ip4_addr>>24   ) & 0xFFU,
                    update->tpu_fwd_port ));
  } else {
    FD_LOG_NOTICE(( "Publishing TPU update: status=DISCONNECTED" ));
  }

  ulong tspub = (ulong)fd_frag_meta_ts_comp( fd_bundle_now( ctx ) );
  fd_stem_publish(
      stem,
      ctx->gossip_out.idx,
      FD_BUNDLE_TPU_UPDATE,
      ctx->gossip_out.chunk,
      sizeof(fd_bundle_tpu_update_t),
      0UL, /* ctl */
      0UL, /* seq */
      tspub
  );
  ctx->gossip_out.chunk = fd_dcache_compact_next( ctx->gossip_out.chunk, sizeof(fd_bundle_tpu_update_t), ctx->gossip_out.chunk0, ctx->gossip_out.wmark );
}

static void
after_credit( fd_bundle_tile_t *  ctx,
              fd_stem_context_t * stem,
              int *               opt_poll_in,
              int *               charge_busy ) {
  if( FD_UNLIKELY( fd_clock_tile_recal_due( ctx->clock ) ) ) fd_clock_tile_recal( ctx->clock );

  /* Harmonic: block bundles are published whole or not at all, like
     regular bundles below: entries of one bundle are contiguous and
     share a bundle_id, and a bundle only goes out if all of it fits in
     this iteration's burst.  Downstream tiles treat them as ordinary
     bundles (sig==1), so pack never sees a prefix of a block bundle.
     The fallback deque is drained only in iterations where nothing was
     staged. */
  int published_block = 0;
  if( FD_UNLIKELY( ctx->harmonic_pending_len ) ) {
    ulong n = 0UL;
    while( n<ctx->harmonic_pending_len ) {
      ulong const id  = ctx->harmonic_staging[ n ].bundle_id;
      ulong       bsz = 0UL;
      while( n+bsz<ctx->harmonic_pending_len && ctx->harmonic_staging[ n+bsz ].bundle_id==id ) bsz++;
      if( FD_UNLIKELY( n+bsz>STEM_BURST ) ) break;

      for( ulong i=0UL; i<bsz; i++ ) {
        fd_bundle_harmonic_staged_txn_t const * s = &ctx->harmonic_staging[ n+i ];

        fd_txn_m_t * txnm = fd_chunk_to_laddr( ctx->verify_out.mem, ctx->verify_out.chunk );
        *txnm = (fd_txn_m_t) {
          .reference_block_height = 0UL,
          .payload_sz             = s->payload_sz,
          .txn_t_sz               = 0U,
          .source_ipv4            = s->source_ipv4,
          .source_tpu             = FD_TXN_M_TPU_SOURCE_HARMONIC,
          .first_seen_nanos       = s->first_seen_nanos,
          .block_engine   = {
            .bundle_id        = s->bundle_id,
            .bundle_txn_cnt   = s->bundle_txn_cnt,
            .commission       = s->commission,
            .revert_protected = s->revert_protected,
          },
        };
        fd_memcpy( txnm->block_engine.commission_pubkey, s->commission_pubkey, 32UL );
        fd_memcpy( fd_txn_m_payload( txnm ), s->payload, s->payload_sz );

        ulong sz    = fd_txn_m_realized_footprint( txnm, 0, 0 );
        ulong tspub = (ulong)fd_frag_meta_ts_comp( fd_bundle_now( ctx ) );
        fd_stem_publish( stem, ctx->verify_out.idx, 1UL, ctx->verify_out.chunk, sz, 0UL, 0UL, tspub );
        ctx->verify_out.chunk = fd_dcache_compact_next( ctx->verify_out.chunk, sz, ctx->verify_out.chunk0, ctx->verify_out.wmark );

        ctx->harmonic_block_txn_received_cnt++;
      }
      n += bsz;
    }
    ctx->harmonic_pending_len -= n;
    if( FD_UNLIKELY( ctx->harmonic_pending_len ) ) {
      memmove( ctx->harmonic_staging, ctx->harmonic_staging + n,
               ctx->harmonic_pending_len * sizeof(fd_bundle_harmonic_staged_txn_t) );
    }
    published_block = n>0UL;
    if( FD_LIKELY( published_block ) ) {
      *charge_busy = 1;
      *opt_poll_in = 0;
    }
  }

  if( !published_block && !pending_txn_empty( ctx->pending_txns ) ) {
    fd_bundle_pending_txn_t * head = pending_txn_peek_head( ctx->pending_txns );
    ulong drain_seq = head->bundle_seq;
    ulong drain_sig = head->sig;
    ulong drain_cnt = 0UL;

    do {
      fd_bundle_pending_txn_t const * txn = pending_txn_peek_head( ctx->pending_txns );

      fd_txn_m_t * txnm = fd_chunk_to_laddr( ctx->verify_out.mem, ctx->verify_out.chunk );
      *txnm = (fd_txn_m_t) {
        .reference_block_height = 0UL,
        .payload_sz             = txn->payload_sz,
        .txn_t_sz               = 0U,
        .source_ipv4            = txn->source_ipv4,
        .source_tpu             = txn->source_tpu,
        .first_seen_nanos       = txn->first_seen_nanos,
        .block_engine   = {
          .bundle_id      = txn->bundle_seq,
          .bundle_txn_cnt = txn->bundle_txn_cnt,
          .commission     = txn->commission,
        },
      };
      fd_memcpy( txnm->block_engine.commission_pubkey, txn->commission_pubkey, 32UL );
      fd_memcpy( fd_txn_m_payload( txnm ), txn->payload, txn->payload_sz );

      ulong sz    = fd_txn_m_realized_footprint( txnm, 0, 0 );
      ulong tspub = (ulong)fd_frag_meta_ts_comp( fd_bundle_now( ctx ) );
      fd_stem_publish( stem, ctx->verify_out.idx, txn->sig, ctx->verify_out.chunk, sz, 0UL, 0UL, tspub );
      ctx->verify_out.chunk = fd_dcache_compact_next( ctx->verify_out.chunk, sz, ctx->verify_out.chunk0, ctx->verify_out.wmark );

      pending_txn_remove_head( ctx->pending_txns );
      drain_cnt++;
    } while( fd_bundle_drain_continue( ctx->pending_txns, drain_sig, drain_seq, drain_cnt, STEM_BURST ) );

    *charge_busy = 1;
    *opt_poll_in = 0;
  }

  /* Drive the TPU endpoint if enabled */
  if( FD_UNLIKELY( ctx->tpu_conn_enabled && !ctx->halt_signing ) ) {
    fd_bundle_tpu_client_step( ctx, charge_busy );
  }

  if( ctx->plugin_out.mem ) {
    if( FD_UNLIKELY( ctx->bundle_status_recent != ctx->bundle_status_plugin ) ) {
      fd_bundle_tile_publish_block_engine_update( ctx, stem );
      ctx->bundle_status_plugin = (uchar)ctx->bundle_status_recent;
      *charge_busy = 1;
    }
  }

  /* Publish TPU status updates to gossip link */
  if( ctx->gossip_out.mem ) {
    int connected    = ctx->tpu_status_recent==FD_BUNDLE_STATE_CONNECTED;
    int addr_changed = connected &&
                       ( ctx->tpu_gossip_tpu_ip4_addr     != ctx->tpu_config_tpu_ip4_addr     ||
                         ctx->tpu_gossip_tpu_port         != ctx->tpu_config_tpu_port         ||
                         ctx->tpu_gossip_tpu_fwd_ip4_addr != ctx->tpu_config_tpu_fwd_ip4_addr ||
                         ctx->tpu_gossip_tpu_fwd_port     != ctx->tpu_config_tpu_fwd_port );
    if( FD_UNLIKELY( ctx->tpu_status_recent != ctx->tpu_status_gossip || addr_changed ) ) {
      fd_bundle_tile_publish_tpu_update( ctx, stem );
      ctx->tpu_status_gossip           = ctx->tpu_status_recent;
      ctx->tpu_gossip_tpu_ip4_addr     = ctx->tpu_config_tpu_ip4_addr;
      ctx->tpu_gossip_tpu_port         = ctx->tpu_config_tpu_port;
      ctx->tpu_gossip_tpu_fwd_ip4_addr = ctx->tpu_config_tpu_fwd_ip4_addr;
      ctx->tpu_gossip_tpu_fwd_port     = ctx->tpu_config_tpu_fwd_port;
      *charge_busy = 1;
    }
  }
}

static void
fd_bundle_tls_keylog_line( fd_bundle_tile_t * ctx,
                           char const *       label,
                           uchar const        client_random[ static 32 ],
                           uchar const *      secret,
                           ulong              secret_sz ) {
  char line[ 256 ];
  char * p = fd_cstr_init( line );
  p = fd_cstr_append_cstr( p, label );
  p = fd_hex_encode( p, client_random, 32UL );
  p = fd_cstr_append_char( p, ' ' );
  p = fd_hex_encode( p, secret, secret_sz );
  ulong line_sz = (ulong)(p-line);
  fd_cstr_fini( p );

  struct iovec iovs[2] = {
    { .iov_base=line,         .iov_len=line_sz },
    { .iov_base=(void *)"\n", .iov_len=1UL     },
  };
  if( FD_UNLIKELY( writev( ctx->keylog_fd, iovs, 2 )!=(long)(line_sz+1UL) ) ) {
    FD_LOG_WARNING(( "write(keylog) failed (%i-%s)", errno, fd_io_strerror( errno ) ));
  }
}

static void
fd_bundle_tls_keylog( void const * handshake,
                      void const * recv_secret,
                      void const * send_secret,
                      uint         encryption_level ) {
  fd_tlsrec_conn_t const * tls_conn = (fd_tlsrec_conn_t const *)(
      (ulong)handshake - offsetof(fd_tlsrec_conn_t, hs) );
  fd_bundle_tile_t * ctx = (fd_bundle_tile_t *)(
      (ulong)tls_conn - offsetof(fd_bundle_tile_t, tls_conn) );
  fd_tls_estate_t const * hs = handshake;

  char const * client_label;
  char const * server_label;
  switch( encryption_level ) {
  case FD_TLS_LEVEL_HANDSHAKE:
    client_label = "CLIENT_HANDSHAKE_TRAFFIC_SECRET ";
    server_label = "SERVER_HANDSHAKE_TRAFFIC_SECRET ";
    break;
  case FD_TLS_LEVEL_APPLICATION:
    client_label = "CLIENT_TRAFFIC_SECRET_0 ";
    server_label = "SERVER_TRAFFIC_SECRET_0 ";
    break;
  default:
    return;
  }

  uchar const * client_secret = hs->base.server ? recv_secret : send_secret;
  uchar const * server_secret = hs->base.server ? send_secret : recv_secret;
  fd_bundle_tls_keylog_line( ctx, client_label, hs->base.client_random,
                             client_secret, 32UL );
  fd_bundle_tls_keylog_line( ctx, server_label, hs->base.client_random,
                             server_secret, 32UL );
}

/* Harmonic: same as fd_bundle_tls_keylog for the TPU endpoint
   connection, which lives at tpu_tls_conn */
void
fd_bundle_tpu_tls_keylog( void const * handshake,
                          void const * recv_secret,
                          void const * send_secret,
                          uint         encryption_level ) {
  fd_tlsrec_conn_t const * tls_conn = (fd_tlsrec_conn_t const *)(
      (ulong)handshake - offsetof(fd_tlsrec_conn_t, hs) );
  fd_bundle_tile_t * ctx = (fd_bundle_tile_t *)(
      (ulong)tls_conn - offsetof(fd_bundle_tile_t, tpu_tls_conn) );
  fd_tls_estate_t const * hs = handshake;

  char const * client_label;
  char const * server_label;
  switch( encryption_level ) {
  case FD_TLS_LEVEL_HANDSHAKE:
    client_label = "CLIENT_HANDSHAKE_TRAFFIC_SECRET ";
    server_label = "SERVER_HANDSHAKE_TRAFFIC_SECRET ";
    break;
  case FD_TLS_LEVEL_APPLICATION:
    client_label = "CLIENT_TRAFFIC_SECRET_0 ";
    server_label = "SERVER_TRAFFIC_SECRET_0 ";
    break;
  default:
    return;
  }

  uchar const * client_secret = hs->base.server ? recv_secret : send_secret;
  uchar const * server_secret = hs->base.server ? send_secret : recv_secret;
  fd_bundle_tls_keylog_line( ctx, client_label, hs->base.client_random,
                             client_secret, 32UL );
  fd_bundle_tls_keylog_line( ctx, server_label, hs->base.client_random,
                             server_secret, 32UL );
}

#ifndef FD_TILE_TEST
static void
fd_bundle_tile_parse_endpoint( fd_bundle_tile_t *     ctx,
                               fd_topo_tile_t const * tile ) {
  fd_url_t url[1];
  _Bool is_ssl = 0;
  if( FD_UNLIKELY( fd_url_parse_endpoint( url,
                                          tile->bundle.url,
                                          tile->bundle.url_len,
                                          &ctx->server_tcp_port,
                                          &is_ssl,
                                          "[tiles.bundle.url]" ) ) ) {
    FD_LOG_ERR(( "Could not parse [tiles.bundle.url]" ));
  }
  if( FD_UNLIKELY( url->host_len>=FD_FQDN_BUF_MAX ) ) {
    FD_LOG_CRIT(( "Invalid url->host_len" )); /* unreachable */
  }
  fd_cstr_fini( fd_cstr_append_text( fd_cstr_init( ctx->server_fqdn ), url->host, url->host_len ) );
  ctx->server_fqdn_len = url->host_len;

  if( FD_UNLIKELY( tile->bundle.sni_len ) ) {
    fd_cstr_fini( fd_cstr_append_text( fd_cstr_init( ctx->server_sni ), tile->bundle.sni, tile->bundle.sni_len ) );
    ctx->server_sni_len = tile->bundle.sni_len;
  } else {
    fd_cstr_fini( fd_cstr_append_text( fd_cstr_init( ctx->server_sni ), url->host, url->host_len ) );
    ctx->server_sni_len = url->host_len;
  }

  if( FD_UNLIKELY( ctx->server_sni_len>=sizeof(ctx->tls->server_name) ) ) {
    FD_LOG_ERR(( "Server name is %lu bytes, longer than the %lu byte maximum: "
                 "check [tiles.bundle.url] and [tiles.bundle.tls_domain_name]",
                 ctx->server_sni_len, sizeof(ctx->tls->server_name)-1UL ));
  }

  ctx->is_ssl = !!is_ssl;
}

/* Parse the TPU endpoint URL if configured */
static void
fd_bundle_tile_parse_tpu_endpoint( fd_bundle_tile_t *     ctx,
                                   fd_topo_tile_t const * tile ) {
  /* Check if TPU endpoint is configured */
  if( FD_UNLIKELY( !tile->bundle.tpu_url_len ) ) {
    ctx->tpu_conn_enabled = 0;
    return;
  }

  fd_url_t url[1];
  _Bool is_ssl = 0;
  if( FD_UNLIKELY( fd_url_parse_endpoint( url,
                                            tile->bundle.tpu_url,
                                            tile->bundle.tpu_url_len,
                                            &ctx->tpu_server_tcp_port,
                                            &is_ssl,
                                            "[tiles.bundle.tpu_url]" ) ) ) {
    FD_LOG_ERR(( "Could not parse [tiles.bundle.tpu_url]" ));
  }
  if( FD_UNLIKELY( url->host_len > 255 ) ) {
    FD_LOG_CRIT(( "Invalid tpu_url->host_len" )); /* unreachable */
  }
  fd_cstr_fini( fd_cstr_append_text( fd_cstr_init( ctx->tpu_server_fqdn ), url->host, url->host_len ) );
  ctx->tpu_server_fqdn_len = url->host_len;

  if( FD_UNLIKELY( tile->bundle.tpu_sni_len ) ) {
    fd_cstr_fini( fd_cstr_append_text( fd_cstr_init( ctx->tpu_server_sni ), tile->bundle.tpu_sni, tile->bundle.tpu_sni_len ) );
    ctx->tpu_server_sni_len = tile->bundle.tpu_sni_len;
  } else {
    fd_cstr_fini( fd_cstr_append_text( fd_cstr_init( ctx->tpu_server_sni ), url->host, url->host_len ) );
    ctx->tpu_server_sni_len = url->host_len;
  }

  if( FD_UNLIKELY( ctx->tpu_server_sni_len>=sizeof(ctx->tls->server_name) ) ) {
    FD_LOG_ERR(( "Server name is %lu bytes, longer than the %lu byte maximum: "
                 "check [tiles.bundle.tpu_url] and [tiles.bundle.tpu_tls_domain_name]",
                 ctx->tpu_server_sni_len, sizeof(ctx->tls->server_name)-1UL ));
  }

  ctx->tpu_is_ssl = !!is_ssl;
  ctx->tpu_conn_enabled = 1;
}

static void
fd_bundle_tile_init_tls( fd_bundle_tile_t *     ctx,
                         fd_topo_tile_t const * tile ) {
  /* Initialize native TLS */
  fd_tls_t * tls = fd_tls_join( fd_tls_new( ctx->tls ) );

  uchar rng_key[ FD_CHACHA_KEY_SZ ];
  if( FD_UNLIKELY( !fd_rng_secure( rng_key, sizeof(rng_key) ) ) ) FD_LOG_CRIT(( "fd_rng_secure failed" ));
  fd_chacha_rng_init( ctx->tls_rng, rng_key, FD_CHACHA_RNG_ALGO_CHACHA8 );
  fd_memzero_explicit( rng_key, sizeof(rng_key) );
  tls->rng = ctx->tls_rng;
  if( FD_LIKELY( ctx->keylog_fd>=0 ) ) tls->secrets_fn = fd_bundle_tls_keylog;

  /* ALPN: h2 */
  static uchar const alpn[] = { 2, 'h', '2' };
  fd_memcpy( tls->alpn, alpn, sizeof(alpn) );
  tls->alpn_sz = sizeof(alpn);

  if( FD_UNLIKELY( !ctx->is_ssl && !ctx->tpu_is_ssl ) ) return; /* plaintext, nothing to verify */

  if( FD_UNLIKELY( !tile->bundle.tls_cert_verify ) ) {
    FD_LOG_WARNING(( "[tiles.bundle.tls_cert_verify] is disabled.  The block engine "
                     "certificate will not be verified.  This is insecure." ));
    return;
  }

  /* Load system CA certificates */
  if( FD_UNLIKELY( fd_x509_ca_store_load_system( ctx->ca_store )<0L ) ) {
    FD_LOG_ERR(( "No CA certificate bundle found, cannot verify the block engine "
                 "certificate.  Install the system CA certificates, or set "
                 "`[tiles.bundle] tls_cert_verify = false` to disable verification "
                 "(insecure)." ));
  }

  tls->ca_store = ctx->ca_store;
}

static void
privileged_init( fd_topo_t const *      topo,
                 fd_topo_tile_t const * tile ) {
  void * scratch = fd_topo_obj_laddr( topo, tile->tile_obj_id );

  ulong const pending_max = tile->bundle.out_depth;

  FD_SCRATCH_ALLOC_INIT( l, scratch );
  fd_bundle_tile_t * ctx         = FD_SCRATCH_ALLOC_APPEND( l, alignof(fd_bundle_tile_t), sizeof(fd_bundle_tile_t)                        );
  void *             grpc_mem    = FD_SCRATCH_ALLOC_APPEND( l, fd_grpc_client_align(),    fd_grpc_client_footprint( tile->bundle.buf_sz ) );
  void *             grpc_tpu_mem = FD_SCRATCH_ALLOC_APPEND( l, fd_grpc_client_align(),    fd_grpc_client_footprint( tile->bundle.buf_sz ) );
  void *             deque_mem   = FD_SCRATCH_ALLOC_APPEND( l, pending_txn_align(),        pending_txn_footprint( pending_max )            );
  void *             harmonic_staging_mem = FD_SCRATCH_ALLOC_APPEND( l,
      alignof(fd_bundle_harmonic_staged_txn_t),
      sizeof(fd_bundle_harmonic_staged_txn_t) * pending_max );

  ulong scratch_top = FD_SCRATCH_ALLOC_FINI( l, scratch_align() );
  if( FD_UNLIKELY( scratch_top > (ulong)scratch + scratch_footprint( tile ) ) )
    FD_LOG_ERR(( "scratch overflow %lu %lu %lu", scratch_top - (ulong)scratch - scratch_footprint( tile ), scratch_top, (ulong)scratch + scratch_footprint( tile ) ));

  memset( ctx, 0, sizeof(fd_bundle_tile_t) );
  ctx->grpc_client_mem  = grpc_mem;
  ctx->grpc_client_tpu_mem = grpc_tpu_mem;
  ctx->grpc_buf_max     = tile->bundle.buf_sz;
  ctx->tcp_sock         = -1;
  ctx->tpu_tcp_sock     = -1;
  ctx->waker_client_idx = tile->waker_client_idx;
  ctx->pending_txns     = pending_txn_join( pending_txn_new( deque_mem, pending_max ) );
  ctx->harmonic_staging     = harmonic_staging_mem;
  ctx->harmonic_staging_max = pending_max;

  fd_bundle_auther_init( &ctx->auther );
  uchar const * public_key = fd_keyload_load( tile->bundle.identity_key_path, 1 /* public key only */ );
  fd_memcpy( ctx->auther.pubkey, public_key, 32UL );

  ctx->keylog_fd = -1;

  /* Initialize harmonic block mode state */
  ctx->harmonic_block_mode = tile->bundle.harmonic_block_mode;

  if( FD_UNLIKELY( tile->bundle.key_log_path[0] ) ) {
    ctx->keylog_fd = open( tile->bundle.key_log_path, O_WRONLY|O_APPEND|O_CREAT, 0644 );
    if( FD_UNLIKELY( ctx->keylog_fd < 0 ) ) {
      FD_LOG_ERR(( "open(%s) failed (%i-%s)", tile->bundle.key_log_path, errno, fd_io_strerror( errno ) ));
    }
  }

  fd_bundle_tile_parse_endpoint( ctx, tile );
  fd_bundle_tile_parse_tpu_endpoint( ctx, tile );

  /* Initialize native TLS before seccomp (reads CA certs from disk) */
  fd_bundle_tile_init_tls( ctx, tile );

  /* Init resolver */
  if( FD_UNLIKELY( !fd_netdb_open_fds( ctx->netdb_fds ) ) ) {
    FD_LOG_ERR(( "fd_netdb_open_fds failed" ));
  }

  /* Random seed for header hashmap */
  if( FD_UNLIKELY( !fd_rng_secure( &ctx->map_seed, sizeof(ulong) ) ) ) {
    FD_LOG_CRIT(( "fd_rng_secure failed" ));
  }

  /* Random seed for timing RNG */
  uint rng_seed;
  if( FD_UNLIKELY( !fd_rng_secure( &rng_seed, sizeof(uint) ) ) ) {
    FD_LOG_CRIT(( "fd_rng_secure failed" ));
  }
  if( FD_UNLIKELY( !fd_rng_join( fd_rng_new( &ctx->rng, rng_seed, 0UL ) ) ) ) {
    FD_LOG_CRIT(( "fd_rng_join failed" )); /* unreachable */
  }
}

static fd_bundle_out_ctx_t
bundle_out_link( fd_topo_t const *      topo,
                 fd_topo_link_t const * link,
                 ulong                  out_link_idx ) {
  fd_bundle_out_ctx_t out = {0};
  out.idx    = out_link_idx;
  out.mem    = topo->workspaces[ topo->objs[ link->dcache_obj_id ].wksp_id ].wksp;
  out.chunk0 = fd_dcache_compact_chunk0( out.mem, link->dcache );
  out.wmark  = fd_dcache_compact_wmark ( out.mem, link->dcache, link->mtu );
  out.chunk  = out.chunk0;
  return out;
}

static void
unprivileged_init( fd_topo_t const *      topo,
                   fd_topo_tile_t const * tile ) {
  fd_bundle_tile_t * ctx = fd_topo_obj_laddr( topo, tile->tile_obj_id );
  if( FD_UNLIKELY( tile->kind_id!=0 ) ) {
    FD_LOG_ERR(( "There can only be one bundle tile" ));
  }

  fd_clock_tile_init( ctx->clock );

  ulong sign_in_idx = fd_topo_find_tile_in_link( topo, tile, "sign_bundle", tile->kind_id );
  if( FD_UNLIKELY( sign_in_idx==ULONG_MAX ) ) FD_LOG_ERR(( "Missing sign_bundle link" ));
  fd_topo_link_t const * sign_in  = &topo->links[ tile->in_link_id[ sign_in_idx ] ];

  ulong sign_out_idx = fd_topo_find_tile_out_link( topo, tile, "bundle_sign", tile->kind_id );
  if( FD_UNLIKELY( sign_out_idx==ULONG_MAX ) ) FD_LOG_ERR(( "Missing bundle_sign link" ));
  fd_topo_link_t const * sign_out = &topo->links[ tile->out_link_id[ sign_out_idx ] ];

  fd_sleep_t * sleep = NULL;
  if( FD_UNLIKELY( topo->sleep_obj_id!=ULONG_MAX ) ) {
    sleep = fd_sleep_join( fd_topo_obj_laddr( topo, topo->sleep_obj_id ) );
    FD_TEST( sleep );
  }

  if( FD_UNLIKELY( !fd_keyguard_client_join( fd_keyguard_client_new(
      ctx->keyguard_client,
      sign_out->mcache,
      sign_out->dcache,
      sign_in->mcache,
      sign_in->dcache,
      sign_out->mtu,
      sign_in->mtu,
      sleep,
      sign_out->id,
      fd_topo_find_link_consumer( topo, sign_out )
  ) ) ) ) {
    FD_LOG_ERR(( "fd_keyguard_client_join failed" )); /* unreachable */
  }

  ctx->keyswitch = fd_keyswitch_join( fd_topo_obj_laddr( topo, tile->id_keyswitch_obj_id ) );
  FD_TEST( ctx->keyswitch );

  FD_TEST( ctx->waker_client_idx!=ULONG_MAX );
  ctx->waker_fseq = fd_fseq_join( fd_topo_obj_laddr( topo, tile->waker_fseq_obj_id ) );
  FD_TEST( ctx->waker_fseq );

  ulong verify_out_idx = fd_topo_find_tile_out_link( topo, tile, "bundle_verif", tile->kind_id );
  if( FD_UNLIKELY( verify_out_idx==ULONG_MAX ) ) FD_LOG_ERR(( "Missing bundle_verif link" ));
  ctx->verify_out = bundle_out_link( topo, &topo->links[ tile->out_link_id[ verify_out_idx ] ], verify_out_idx );

  ulong plugin_out_idx = fd_topo_find_tile_out_link( topo, tile, "bundle_status", tile->kind_id );
  if( plugin_out_idx!=ULONG_MAX ) {
    ctx->plugin_out = bundle_out_link( topo, &topo->links[ tile->out_link_id[ plugin_out_idx ] ], plugin_out_idx );
  } else {
    ctx->plugin_out = (fd_bundle_out_ctx_t){ .idx=ULONG_MAX };
  }

  /* Initialize gossip output for TPU updates */
  ulong gossip_out_idx = fd_topo_find_tile_out_link( topo, tile, "bundle_gossi", tile->kind_id );
  if( gossip_out_idx!=ULONG_MAX ) {
    ctx->gossip_out = bundle_out_link( topo, &topo->links[ tile->out_link_id[ gossip_out_idx ] ], gossip_out_idx );
  } else {
    ctx->gossip_out = (fd_bundle_out_ctx_t){ .idx=ULONG_MAX };
  }

  /* Set socket receive buffer size */
  ulong so_rcvbuf = tile->bundle.buf_sz;
  if( FD_UNLIKELY( so_rcvbuf < 2048UL  ) ) FD_LOG_ERR(( "Invalid [development.bundle.buffer_size_kib]: too small" ));
  if( FD_UNLIKELY( so_rcvbuf > INT_MAX ) ) FD_LOG_ERR(( "Invalid [development.bundle.buffer_size_kib]: too large" ));
  ctx->so_rcvbuf = (int)so_rcvbuf;

  /* Set idle ping timer */
  ctx->keepalive_interval = (long)tile->bundle.keepalive_interval_nanos;

  ctx->bundle_status_plugin = 127;
  ctx->bundle_status_recent = (uchar)FD_BUNDLE_STATE_DISCONNECTED;
  ctx->last_bundle_status_log_nanos = fd_log_wallclock();

  FD_TEST( tile->in_cnt<=sizeof(ctx->in_kind)/sizeof(ctx->in_kind[0]) );
  int has_replay_in = 0;
  ulong polled_in_idx = 0UL;
  for( ulong i=0UL; i<tile->in_cnt; i++ ) {
    if( FD_UNLIKELY( !tile->in_link_poll[ i ] ) ) continue;

    fd_topo_link_t const * link = &topo->links[ tile->in_link_id[ i ] ];
    if( !strcmp( link->name, "replay_slot" ) ) {
      ctx->in_kind[ polled_in_idx ] = IN_KIND_REPLAY_OUT;
      fd_topo_wksp_t const * link_wksp = &topo->workspaces[ topo->objs[ link->dcache_obj_id ].wksp_id ];
      ctx->replay_in.mem    = link_wksp->wksp;
      ctx->replay_in.chunk0 = fd_dcache_compact_chunk0( ctx->replay_in.mem, link->dcache );
      ctx->replay_in.wmark  = fd_dcache_compact_wmark ( ctx->replay_in.mem, link->dcache, link->mtu );
      has_replay_in = 1;
    }
    polled_in_idx++;
  }

  ctx->next_leader_slot = ULONG_MAX;
  ctx->reset_slot       = ULONG_MAX;
  /* Harmonic must not start asleep: fd_bundle_tile_maybe_sleep returns
     early in harmonic mode and so can never clear this. */
  ctx->sleep_mode       = has_replay_in && !ctx->harmonic_block_mode; /* start asleep until we learn leader schedule */
  ctx->sleep_check_ns   = 0;
  ctx->halt_signing     = 0;
  if( !has_replay_in ) memset( &ctx->replay_in, 0, sizeof(ctx->replay_in) );

  ctx->tpu_status_gossip = 127;  /* Force initial update */
  ctx->tpu_gossip_tpu_ip4_addr     = 0U;
  ctx->tpu_gossip_tpu_port         = 0;
  ctx->tpu_gossip_tpu_fwd_ip4_addr = 0U;
  ctx->tpu_gossip_tpu_fwd_port     = 0;
  ctx->tpu_status_recent = FD_BUNDLE_STATE_DISCONNECTED;

  ctx->grpc_client = fd_grpc_client_new( ctx->grpc_client_mem, &fd_bundle_client_grpc_callbacks, ctx->grpc_metrics, ctx, ctx->grpc_buf_max, ctx->map_seed );
  if( FD_UNLIKELY( !ctx->grpc_client ) ) {
    FD_LOG_CRIT(( "fd_grpc_client_new failed" )); /* unreachable */
  }
  fd_grpc_client_set_version( ctx->grpc_client, fd_version_cstr, strlen( fd_version_cstr ) );
  fd_grpc_client_set_authority( ctx->grpc_client, ctx->server_sni, ctx->server_sni_len, ctx->server_tcp_port );

  /* Initialize TPU endpoint if configured */
  if( ctx->tpu_conn_enabled ) {
    fd_bundle_auther_init( &ctx->tpu_auther );
    fd_memcpy( ctx->tpu_auther.pubkey, ctx->auther.pubkey, 32UL );

    ctx->tpu_grpc_client = fd_grpc_client_new( ctx->grpc_client_tpu_mem, &fd_bundle_tpu_client_grpc_callbacks, ctx->tpu_grpc_metrics, ctx, ctx->grpc_buf_max, ctx->map_seed );
    if( FD_UNLIKELY( !ctx->tpu_grpc_client ) ) {
      FD_LOG_CRIT(( "fd_grpc_client_new for TPU endpoint failed" )); /* unreachable */
    }
    fd_grpc_client_set_version( ctx->tpu_grpc_client, fd_version_cstr, strlen( fd_version_cstr ) );
    fd_grpc_client_set_authority( ctx->tpu_grpc_client, ctx->tpu_server_sni, ctx->tpu_server_sni_len, ctx->tpu_server_tcp_port );

    FD_LOG_NOTICE(( "TPU bundle endpoint configured: %.*s", (int)ctx->tpu_server_fqdn_len, ctx->tpu_server_fqdn ));
  }

  if( ctx->harmonic_block_mode ) {
    FD_LOG_NOTICE(( "Harmonic block mode enabled" ));
  }

  fd_histf_new( ctx->metrics.msg_rx_delay,
      FD_MHIST_MIN( BUNDLE, MESSAGE_RX_DELAY_NANOS ),
      FD_MHIST_MAX( BUNDLE, MESSAGE_RX_DELAY_NANOS ) );
}

static ulong
populate_allowed_seccomp( fd_topo_t const *      topo,
                          fd_topo_tile_t const * tile,
                          ulong                  out_cnt,
                          struct sock_filter *   out ) {
  fd_bundle_tile_t * ctx = fd_topo_obj_laddr( topo, tile->tile_obj_id );

  uint epoll_inner_fd = (uint)FD_WAKER_INNER_FD( tile->waker_client_idx );
  uint epoll_outer_fd = (uint)FD_WAKER_OUTER_FD;

  populate_sock_filter_policy_fd_bundle_tile(
      out_cnt, out,
      (uint)fd_log_private_logfile_fd(),
      (uint)ctx->keylog_fd,
      (uint)ctx->netdb_fds->etc_hosts,
      (uint)ctx->netdb_fds->etc_resolv_conf,
      epoll_inner_fd,
      epoll_outer_fd
  );
  return sock_filter_policy_fd_bundle_tile_instr_cnt;
}

static ulong
populate_allowed_fds( fd_topo_t const *      topo,
                      fd_topo_tile_t const * tile,
                      ulong                  out_fds_cnt,
                      int *                  out_fds ) {
  fd_bundle_tile_t * ctx = fd_topo_obj_laddr( topo, tile->tile_obj_id );

  if( FD_UNLIKELY( out_fds_cnt<7UL ) ) FD_LOG_ERR(( "out_fds_cnt %lu", out_fds_cnt ));

  ulong out_cnt = 0UL;
  out_fds[ out_cnt++ ] = 2; /* stderr */
  if( FD_LIKELY( -1!=fd_log_private_logfile_fd() ) )
    out_fds[ out_cnt++ ] = fd_log_private_logfile_fd(); /* logfile */
  if( FD_LIKELY( ctx->netdb_fds->etc_hosts >= 0 ) )
    out_fds[ out_cnt++ ] = ctx->netdb_fds->etc_hosts;
  out_fds[ out_cnt++ ] = ctx->netdb_fds->etc_resolv_conf;
  if( FD_UNLIKELY( ctx->keylog_fd>=0 ) )
    out_fds[ out_cnt++ ] = ctx->keylog_fd;
  out_fds[ out_cnt++ ] = FD_WAKER_OUTER_FD;
  out_fds[ out_cnt++ ] = FD_WAKER_INNER_FD( tile->waker_client_idx );
  return out_cnt;
}

#define STEM_LAZY ((long)10e6)

#define STEM_CALLBACK_CONTEXT_TYPE  fd_bundle_tile_t
#define STEM_CALLBACK_CONTEXT_ALIGN alignof(fd_bundle_tile_t)

#define STEM_CALLBACK_DURING_HOUSEKEEPING fd_bundle_tile_housekeeping
#define STEM_CALLBACK_NEXT_DEADLINE       next_deadline
#define STEM_CALLBACK_METRICS_WRITE       metrics_write
#define STEM_CALLBACK_DURING_FRAG         during_frag
#define STEM_CALLBACK_AFTER_FRAG          after_frag
#define STEM_CALLBACK_BEFORE_CREDIT       before_credit
#define STEM_CALLBACK_AFTER_CREDIT        after_credit

#include "../stem/fd_stem.c"

fd_topo_run_tile_t fd_tile_bundle = {
  .name                     = "bundle",
  .populate_allowed_seccomp = populate_allowed_seccomp,
  .populate_allowed_fds     = populate_allowed_fds,
  .scratch_align            = scratch_align,
  .scratch_footprint        = scratch_footprint,
  .privileged_init          = privileged_init,
  .unprivileged_init        = unprivileged_init,
  .run                      = stem_run,
  .rlimit_file_cnt          = 64,
  .keep_host_networking     = 1,
  .allow_connect            = 1
};
#endif
