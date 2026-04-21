#include "test_bundle_common.c"
#include "proto/block_engine.pb.h"
#include "proto/packet.pb.h"
#include "../../ballet/base58/fd_base58.h"
#include "../../ballet/nanopb/pb_encode.h"

FD_IMPORT_BINARY( test_bundle_response, "src/disco/bundle/test_bundle_response.binpb" );

__attribute__((weak)) char const fdctl_version_string[] = "0.0.0";

#define TEST_STEM_BURST (5UL)

static long g_clock = 1L;

__attribute__((weak)) long
fd_bundle_now( void ) {
  return g_clock;
}

/* Test that packets and bundles get forwarded correctly to Firedancer
   components. */

static void
test_bundle_rx( fd_wksp_t * wksp ) {
  test_bundle_env_t env[1]; test_bundle_env_create( env, wksp );
  fd_bundle_tile_t * state = env->state;

  /* A SubscribePacketsResponse message with 2 packets included. The
     first packet is 1 byte { 0x48 }, the second packet is 2 bytes
     {0x48, 0x48}.

     message SubscribePacketsResponse {
      shared.Header header = 1;
      packet.PacketBatch batch = 2;
    }
  */
  static uchar subscribe_packets_msg[] = {
    0x12, 0x13, 0x0a, 0x07, 0x0a, 0x01, 0x48, 0x12,
    0x02, 0x08, 0x01, 0x0a, 0x08, 0x0a, 0x02, 0x48,
    0x48, 0x12, 0x02, 0x08, 0x02
  };
  fd_bundle_client_grpc_rx_msg(
      state,
      subscribe_packets_msg, sizeof(subscribe_packets_msg),
      FD_BUNDLE_CLIENT_REQ_Bundle_SubscribePackets
  );

  FD_TEST( pending_txn_cnt( state->pending_txns )==2UL );
  FD_TEST( pending_txn_peek_head( state->pending_txns )->sig==0UL );

  state->builder_info_avail = 1;

  fd_bundle_client_grpc_rx_msg(
      state,
      test_bundle_response, test_bundle_response_sz,
      FD_BUNDLE_CLIENT_REQ_Bundle_SubscribeBundles
  );

  FD_TEST( pending_txn_cnt( state->pending_txns )>2UL );

  test_bundle_env_destroy( env );
}

  /*
  Contains a single bundle with 5/6 transactions
  {
    "bundles": [
      {
        "bundle": {
          "header": null,
          "packets": [
            {
              "data": [
                72
              ],
              "meta": {
                "size": 1,
                "addr": "",
                "port": 0,
                "flags": null,
                "sender_stake": 0
              }
            },
            ...x5/6
          ]
        },
        "uuid": [0, 0, 0]
      }
    ]
  }
  */

/* Reusable test message with 5 transactions in a bundle */
static uchar subscribe_bundles_msg_x5[] = {
  0x0a, 0x52, 0x0a, 0x4b, 0x1a, 0x0d, 0x0a, 0x01, 0x48, 0x12, 0x08,
  0x08, 0x01, 0x12, 0x00, 0x18, 0x00, 0x28, 0x00, 0x1a, 0x0d, 0x0a,
  0x01, 0x48, 0x12, 0x08, 0x08, 0x01, 0x12, 0x00, 0x18, 0x00, 0x28,
  0x00, 0x1a, 0x0d, 0x0a, 0x01, 0x48, 0x12, 0x08, 0x08, 0x01, 0x12,
  0x00, 0x18, 0x00, 0x28, 0x00, 0x1a, 0x0d, 0x0a, 0x01, 0x48, 0x12,
  0x08, 0x08, 0x01, 0x12, 0x00, 0x18, 0x00, 0x28, 0x00, 0x1a, 0x0d,
  0x0a, 0x01, 0x48, 0x12, 0x08, 0x08, 0x01, 0x12, 0x00, 0x18, 0x00,
  0x28, 0x00, 0x12, 0x03, 0x00, 0x00, 0x00
};



static uchar subscribe_bundles_msg_x6[] = {
  0x0a, 0x61, 0x0a, 0x5a, 0x1a, 0x0d, 0x0a, 0x01, 0x48, 0x12, 0x08,
  0x08, 0x01, 0x12, 0x00, 0x18, 0x00, 0x28, 0x00, 0x1a, 0x0d, 0x0a,
  0x01, 0x48, 0x12, 0x08, 0x08, 0x01, 0x12, 0x00, 0x18, 0x00, 0x28,
  0x00, 0x1a, 0x0d, 0x0a, 0x01, 0x48, 0x12, 0x08, 0x08, 0x01, 0x12,
  0x00, 0x18, 0x00, 0x28, 0x00, 0x1a, 0x0d, 0x0a, 0x01, 0x48, 0x12,
  0x08, 0x08, 0x01, 0x12, 0x00, 0x18, 0x00, 0x28, 0x00, 0x1a, 0x0d,
  0x0a, 0x01, 0x48, 0x12, 0x08, 0x08, 0x01, 0x12, 0x00, 0x18, 0x00,
  0x28, 0x00, 0x1a, 0x0d, 0x0a, 0x01, 0x48, 0x12, 0x08, 0x08, 0x01,
  0x12, 0x00, 0x18, 0x00, 0x28, 0x00, 0x12, 0x03, 0x00, 0x00, 0x00
};

static void
test_bundle_rx_too_many_txns( fd_wksp_t * wksp ) {
  test_bundle_env_t env[1]; test_bundle_env_create( env, wksp );
  fd_bundle_tile_t * state = env->state;


  state->builder_info_avail = 1;
  fd_bundle_client_grpc_rx_msg(
      state,
      subscribe_bundles_msg_x5, sizeof(subscribe_bundles_msg_x5),
      FD_BUNDLE_CLIENT_REQ_Bundle_SubscribeBundles
  );

  FD_TEST( pending_txn_cnt( state->pending_txns )==5UL );
  test_bundle_env_destroy( env );

  test_bundle_env_create( env, wksp );
  state = env->state;

  /* Same as above, now with 6 transactions. Should be a NOP for bundles */

  state->builder_info_avail = 1;
  fd_bundle_client_grpc_rx_msg(
      state,
      subscribe_bundles_msg_x6, sizeof(subscribe_bundles_msg_x6),
      FD_BUNDLE_CLIENT_REQ_Bundle_SubscribeBundles
  );

  FD_TEST( state->bundle_txn_cnt==6 );
  FD_TEST( pending_txn_cnt( state->pending_txns )==0UL );
  test_bundle_env_destroy( env );
}

/* Ensure forwarding of bundles stops when builder fee info is missing. */

static void
test_bundle_no_builder_fee_info( fd_wksp_t * wksp ) {
  test_bundle_env_t env[1]; test_bundle_env_create( env, wksp );
  fd_bundle_tile_t * state = env->state;
  state->builder_info_avail = 0;

  /* Regular packets are always forwarded */
  static uchar subscribe_packets_msg[] = {
    0x12, 0x09, 0x0a, 0x07, 0x0a, 0x01, 0x48, 0x12,
    0x02, 0x08, 0x01
  };
  fd_bundle_client_grpc_rx_msg(
      state,
      subscribe_packets_msg, sizeof(subscribe_packets_msg),
      FD_BUNDLE_CLIENT_REQ_Bundle_SubscribePackets
  );
  FD_TEST( pending_txn_cnt( state->pending_txns )==1UL );
  FD_TEST( state->metrics.packet_received_cnt          ==1UL );
  FD_TEST( state->metrics.missing_builder_info_fail_cnt==0UL );

  /* Bundles are no longer forwarded */

  fd_bundle_client_grpc_rx_msg(
      state,
      test_bundle_response, test_bundle_response_sz,
      FD_BUNDLE_CLIENT_REQ_Bundle_SubscribeBundles
  );
  FD_TEST( pending_txn_cnt( state->pending_txns )==1UL );
  FD_TEST( state->metrics.bundle_received_cnt          ==0UL );
  FD_TEST( state->metrics.missing_builder_info_fail_cnt==1UL );

  test_bundle_env_destroy( env );
}

/* Ensure that the client reconnects (with a new TCP socket) if the
   server ends the stream */

static void
test_bundle_stream_ended( fd_wksp_t * wksp ) {
  test_bundle_env_t env[1];
  test_bundle_env_create( env, wksp );
  test_bundle_env_mock_conn( env );

  fd_bundle_tile_t * state = env->state;
  fd_h2_rbuf_t * rbuf_tx = fd_grpc_client_rbuf_tx( state->grpc_client );
  FD_TEST( rbuf_tx->hi_off==0UL );
  fd_grpc_resp_hdrs_t hdrs = {
    .h2_status   = 200,
    .grpc_status = FD_GRPC_STATUS_OK
  };
  FD_TEST( state->defer_reset==0 );
  fd_bundle_client_grpc_rx_end( state, FD_BUNDLE_CLIENT_REQ_Bundle_SubscribeBundles, &hdrs );
  FD_TEST( state->defer_reset==1 );

  test_bundle_env_destroy( env );
}

/* Same as above, but with hard stream resets */

static void
test_bundle_stream_reset( fd_wksp_t * wksp ) {
  test_bundle_env_t env[1];
  test_bundle_env_create( env, wksp );
  test_bundle_env_mock_conn( env );

  fd_bundle_tile_t * state   = env->state;
  fd_grpc_client_t * client  = state->grpc_client;
  fd_h2_conn_t *     h2_conn = fd_grpc_client_h2_conn( client );

  fd_grpc_h2_stream_t * stream = fd_grpc_client_stream_acquire( client, FD_BUNDLE_CLIENT_REQ_Bundle_SubscribeBundles );
  FD_TEST( stream );
  stream->hdrs.h2_status     = 200;
  stream->hdrs.is_grpc_proto = 1;

  FD_TEST( state->defer_reset==0 );
  fd_grpc_client_h2_callbacks.rst_stream( h2_conn, &stream->s, 0U, 1 );
  FD_TEST( state->defer_reset==1 );

  test_bundle_env_destroy( env );
}

/* Test response header timeout */

static void
test_bundle_header_timeout( fd_wksp_t * wksp ) {
  test_bundle_env_t env[1];
  test_bundle_env_create( env, wksp );
  test_bundle_env_mock_conn( env );

  fd_bundle_tile_t * state  = env->state;
  fd_grpc_client_t * client = state->grpc_client;

  fd_grpc_h2_stream_t * stream = fd_grpc_client_stream_acquire( client, FD_BUNDLE_CLIENT_REQ_Bundle_SubscribeBundles );
  FD_TEST( stream );
  stream->hdrs.h2_status        = 200;
  stream->hdrs.is_grpc_proto    = 1;
  stream->has_header_deadline   = 1;
  stream->header_deadline_nanos = 99L;

  /* FIXME ensure that receiving a header disarms the timeout */

  fd_grpc_client_service_streams( client, 100L );
  FD_TEST( state->defer_reset==1 );

  test_bundle_env_destroy( env );
}

/* Test response timeout */

static void
test_bundle_rx_end_timeout( fd_wksp_t * wksp ) {
  test_bundle_env_t env[1];
  test_bundle_env_create( env, wksp );
  test_bundle_env_mock_conn( env );

  fd_bundle_tile_t * state  = env->state;
  fd_grpc_client_t * client = state->grpc_client;

  fd_grpc_h2_stream_t * stream = fd_grpc_client_stream_acquire( client, FD_BUNDLE_CLIENT_REQ_Bundle_SubscribeBundles );
  FD_TEST( stream );
  stream->hdrs.h2_status        = 200;
  stream->hdrs.is_grpc_proto    = 1;
  stream->has_rx_end_deadline   = 1;
  stream->rx_end_deadline_nanos = 99L;

  fd_grpc_client_service_streams( client, 100L );
  FD_TEST( state->defer_reset==1 );

  test_bundle_env_destroy( env );
}

/* Test ping timeout */

static void
test_bundle_ping( fd_wksp_t * wksp ) {
  test_bundle_env_t env[1];
  test_bundle_env_create( env, wksp );
  test_bundle_env_mock_conn( env );

  fd_bundle_tile_t * state       = env->state;
  fd_grpc_client_t * grpc_client = state->grpc_client;
  long const ts_ping_tx = g_clock;
  fd_keepalive_tx( state->keepalive, state->rng, ts_ping_tx );
  state->grpc_client->conn->ping_tx = 1;
  FD_TEST( state->keepalive->inflight==1 );
  FD_TEST( !!state->keepalive->ts_deadline );
  FD_TEST( state->keepalive->ts_next_tx >= ts_ping_tx + (state->keepalive->interval>>1) );
  FD_TEST( state->keepalive->ts_last_tx==ts_ping_tx );
  FD_TEST( fd_keepalive_is_timeout( state->keepalive, ts_ping_tx )==0 );
  FD_TEST( fd_keepalive_should_tx ( state->keepalive, ts_ping_tx )==0 );

  /* PING ACK should update timer */
  g_clock += (long)10e6; /* 10ms passed */
  long const ts_ping_rx = g_clock;
  grpc_client->conn->ping_tx = 1;
  fd_h2_frame_hdr_t ping_ack_hdr = {
    .typlen = fd_h2_frame_typlen( FD_H2_FRAME_TYPE_PING, 8UL ),
    .flags  = FD_H2_FLAG_ACK
  };
  fd_h2_rbuf_push( grpc_client->frame_rx, &ping_ack_hdr, sizeof(fd_h2_frame_hdr_t) );
  ulong const ping_seq = 1UL;
  fd_h2_rbuf_push( grpc_client->frame_rx, &ping_seq, sizeof(ulong) );
  int charge_busy = 0;
  fd_bundle_client_step( state, &charge_busy );
  FD_TEST( fd_h2_rbuf_used_sz( grpc_client->frame_rx )==0UL );
  FD_TEST( grpc_client->conn->ping_tx == 0 );
  FD_TEST( state->defer_reset==0 );
  FD_TEST( state->keepalive->ts_last_tx==ts_ping_tx );
  FD_TEST( state->keepalive->ts_last_rx==ts_ping_rx );
  FD_TEST( !state->keepalive->inflight );
  FD_TEST( state->metrics.ping_ack_cnt==1UL );
  FD_TEST( state->rtt->latest_rtt==10e6f );
  FD_TEST( fd_keepalive_is_timeout( state->keepalive, ts_ping_rx )==0 );
  FD_TEST( fd_keepalive_should_tx ( state->keepalive, ts_ping_rx )==0 );

  /* Test PING TX */
  g_clock = state->keepalive->ts_next_tx;
  long const ts_ping_tx2 = g_clock;
  FD_TEST( fd_keepalive_is_timeout( state->keepalive, ts_ping_tx2 )==0 );
  FD_TEST( fd_keepalive_should_tx ( state->keepalive, ts_ping_tx2 )==1 );
  FD_TEST( state->keepalive->inflight==0 );
  charge_busy = 0;
  fd_bundle_client_step( state, &charge_busy );
  FD_TEST( state->defer_reset==0 );
  FD_TEST( fd_keepalive_is_timeout( state->keepalive, ts_ping_tx2 )==0 );
  FD_TEST( fd_keepalive_should_tx ( state->keepalive, ts_ping_tx2 )==0 );
  FD_TEST( state->keepalive->inflight==1 );
  FD_TEST( charge_busy==1 );
  FD_TEST( fd_h2_rbuf_used_sz( grpc_client->frame_tx )==sizeof(fd_h2_ping_t) );
  fd_h2_ping_t ping;
  fd_h2_rbuf_pop_copy( grpc_client->frame_tx, &ping, sizeof(fd_h2_ping_t) );
  FD_TEST( ping.hdr.typlen==fd_h2_frame_typlen( FD_H2_FRAME_TYPE_PING, 8UL ) );
  FD_TEST( ping.hdr.flags==0 );
  FD_TEST( state->keepalive->inflight==1 );
  charge_busy = 0;
  fd_bundle_client_step( state, &charge_busy );
  FD_TEST( charge_busy==0 );
  FD_TEST( fd_h2_rbuf_used_sz( grpc_client->frame_tx )==0 );

  /* Test timeout */
  g_clock = state->keepalive->ts_deadline + 1L;
  long const ts_ping_timeout = g_clock;
  FD_TEST( fd_keepalive_is_timeout( state->keepalive, ts_ping_timeout )==1 );

  /* Stepping should cause a reset due to timeout */
  fd_bundle_client_step( state, &charge_busy );
  FD_TEST( state->defer_reset==1 );

  test_bundle_env_destroy( env );
}

/* Check the client's behavior if an oversized message is received */

static void
test_bundle_msg_oversized( fd_wksp_t * wksp ) {
  test_bundle_env_t env[1];
  test_bundle_env_create( env, wksp );
  test_bundle_env_mock_conn( env );

  fd_bundle_tile_t * state  = env->state;
  fd_grpc_client_t * client = state->grpc_client;

  fd_grpc_h2_stream_t * stream = fd_grpc_client_stream_acquire( client, FD_BUNDLE_CLIENT_REQ_Bundle_SubscribeBundles );
  FD_TEST( stream );
  stream->hdrs.h2_status     = 200;
  stream->hdrs.is_grpc_proto = 1;

  fd_h2_conn_t * h2_conn = fd_grpc_client_h2_conn( state->grpc_client );
  fd_grpc_hdr_t hdr = {
    .compressed = 0,
    .msg_sz     = fd_uint_bswap( USHORT_MAX )
  };

  FD_TEST( state->bundle_subscription_live );
  fd_grpc_client_h2_callbacks.data( h2_conn, &stream->s, &hdr, sizeof(fd_grpc_hdr_t), 0UL );
  FD_TEST( !state->bundle_subscription_live );
  FD_TEST( state->defer_reset );

  test_bundle_env_destroy( env );
}

/* Ensure that the client resets after switching keys */

static void
test_bundle_keyswitch( fd_wksp_t * wksp ) {
  test_bundle_env_t env[1];
  test_bundle_env_create( env, wksp );
  test_bundle_env_mock_conn( env );
  fd_bundle_tile_t * state = env->state;

  void * keyswitch_mem = fd_wksp_alloc_laddr( wksp, fd_keyswitch_align(), fd_keyswitch_footprint(), 1UL );
  FD_TEST( keyswitch_mem );
  state->keyswitch = fd_keyswitch_join( fd_keyswitch_new( keyswitch_mem, FD_KEYSWITCH_STATE_UNLOCKED ) );
  memset( state->auther.pubkey, 0, 32 );

  fd_bundle_tile_housekeeping( state ); /* should not switch */
  FD_TEST( !state->defer_reset );

  fd_keyswitch_state( state->keyswitch, FD_KEYSWITCH_STATE_SWITCH_PENDING );
  state->keyswitch->bytes[0] = 0x01;
  fd_bundle_tile_housekeeping( state ); /* should switch */
  FD_TEST( state->defer_reset );
  FD_TEST( state->auther.pubkey[0] == 0x01 );

  test_bundle_env_destroy( env );
  fd_wksp_free_laddr( keyswitch_mem );
}

/* Verify that the bundle client status is reported correctly */

static void
test_bundle_client_status( fd_wksp_t * wksp ) {
  test_bundle_env_t env[1];
  test_bundle_env_create( env, wksp );
  test_bundle_env_mock_conn( env );
  fd_bundle_tile_t * state = env->state;
  fd_bundle_tile_t state_backup  = *state;
  fd_grpc_client_t client_backup = *state->grpc_client;

  FD_TEST( fd_bundle_client_status( state )==2 );
  state->tcp_sock_connected = 0;
  FD_TEST( fd_bundle_client_status( state )==0 );
  *state = state_backup;

  ushort const conn_dead_flags[] = {
    FD_H2_CONN_FLAGS_DEAD,
    FD_H2_CONN_FLAGS_SEND_GOAWAY
  };
  for( ulong i=0; i<sizeof(conn_dead_flags)/sizeof(ushort); i++ ) {
    FD_TEST( fd_bundle_client_status( state )==2 );
    state->grpc_client->conn->flags |= conn_dead_flags[ i ];
    FD_TEST( fd_bundle_client_status( state )==0 );
    *state->grpc_client = client_backup;
  }

  ushort const conn_prog_flags[] = {
    FD_H2_CONN_FLAGS_CLIENT_INITIAL,
    FD_H2_CONN_FLAGS_WAIT_SETTINGS_ACK_0,
    FD_H2_CONN_FLAGS_WAIT_SETTINGS_0,
    FD_H2_CONN_FLAGS_SERVER_INITIAL
  };
  for( ulong i=0; i<sizeof(conn_prog_flags)/sizeof(ushort); i++ ) {
    FD_TEST( fd_bundle_client_status( state )==2 );
    state->grpc_client->conn->flags |= conn_prog_flags[ i ];
    FD_TEST( fd_bundle_client_status( state )==1 );
    *state->grpc_client = client_backup;
  }

  for( int auth_state=0; auth_state<FD_BUNDLE_AUTH_STATE_DONE_WAIT; auth_state++ ) {
    FD_TEST( fd_bundle_client_status( state )==2 );
    state->auther.state = auth_state;
    FD_TEST( fd_bundle_client_status( state )==1 );
    state->auther.state = FD_BUNDLE_AUTH_STATE_DONE_WAIT;
  }

  FD_TEST( fd_bundle_client_status( state )==2 );
  state->builder_info_wait = 1;
  FD_TEST( fd_bundle_client_status( state )==2 ); /* rotate builder info without downtime */
  state->auther.state = FD_BUNDLE_AUTH_STATE_DONE_WAIT;

  FD_TEST( fd_bundle_client_status( state )==2 );
  state->builder_info_avail = 0;
  FD_TEST( fd_bundle_client_status( state )==1 );
  *state = state_backup;

  FD_TEST( fd_bundle_client_status( state )==2 );
  state->packet_subscription_live = 0;
  FD_TEST( fd_bundle_client_status( state )==1 );
  *state = state_backup;

  FD_TEST( fd_bundle_client_status( state )==2 );
  state->bundle_subscription_live = 0;
  FD_TEST( fd_bundle_client_status( state )==1 );
  *state = state_backup;

  FD_TEST( fd_bundle_client_status( state )==2 );
  state->keepalive->inflight     = 1;
  state->keepalive->ts_deadline -= g_clock-1L;
  FD_TEST( fd_bundle_client_status( state )==0 );
  *state = state_backup;

  FD_TEST( fd_bundle_client_status( state )==2 );
  state->grpc_client->h2_hs_done = 0;
  FD_TEST( fd_bundle_client_status( state )==1 );
  *state->grpc_client = client_backup;

  test_bundle_env_destroy( env );
}

/* Verify that reset clears everything */

static void
test_bundle_client_reset( fd_wksp_t * wksp ) {
  test_bundle_env_t env[1];
  test_bundle_env_create( env, wksp );
  test_bundle_env_mock_conn( env );
  fd_bundle_tile_t * state = env->state;

  FD_TEST( state->tcp_sock!=-1 );
  FD_TEST( state->tcp_sock_connected==1 );
  FD_TEST( state->defer_reset==0 );
  FD_TEST( state->builder_info_avail==1 );
  FD_TEST( state->builder_info_wait==0 );
  FD_TEST( state->packet_subscription_live==1 );
  FD_TEST( state->packet_subscription_wait==0 );
  FD_TEST( state->bundle_subscription_live==1 );
  FD_TEST( state->bundle_subscription_wait==0 );
  FD_TEST( state->rtt->is_rtt_valid==0 );
  FD_TEST( state->auther.state==FD_BUNDLE_AUTH_STATE_DONE_WAIT );
  FD_TEST( state->auther.needs_poll==0 );
  FD_TEST( state->grpc_client->ssl_hs_done==0 );
  FD_TEST( state->grpc_client->h2_hs_done==1 );
  FD_TEST( state->grpc_client->stream_cnt==2 );

  fd_bundle_client_reset( state );

  FD_TEST( state->tcp_sock==-1 );
  FD_TEST( state->tcp_sock_connected==0 );
  FD_TEST( state->defer_reset==0 );
  FD_TEST( state->builder_info_avail==0 );
  FD_TEST( state->builder_info_wait==0 );
  FD_TEST( state->packet_subscription_live==0 );
  FD_TEST( state->packet_subscription_wait==0 );
  FD_TEST( state->bundle_subscription_live==0 );
  FD_TEST( state->bundle_subscription_wait==0 );
  FD_TEST( state->rtt->is_rtt_valid==0 );
  FD_TEST( state->auther.state==FD_BUNDLE_AUTH_STATE_REQ_CHALLENGE );
  FD_TEST( state->auther.needs_poll==1 );
  FD_TEST( state->grpc_client->ssl_hs_done==0 );
  FD_TEST( state->grpc_client->h2_hs_done==0 );
  FD_TEST( state->grpc_client->stream_cnt==0 );

  test_bundle_env_destroy( env );
}

/* Utility to parse a request header */

static void
expect_h2_hdr( fd_h2_rbuf_t *       rbuf,
               ulong                stream_id,
               char const * const * pstr ) {
  fd_h2_frame_hdr_t frame_hdr;
  FD_TEST( fd_h2_rbuf_used_sz( rbuf )>=sizeof(fd_h2_frame_hdr_t) );
  fd_h2_rbuf_pop_copy( rbuf, &frame_hdr, sizeof(fd_h2_frame_hdr_t) );
  FD_TEST( fd_h2_frame_type( frame_hdr.typlen )==FD_H2_FRAME_TYPE_HEADERS );
  FD_TEST( fd_uint_bswap( frame_hdr.r_stream_id )==stream_id );
  FD_TEST( frame_hdr.flags==FD_H2_FLAG_END_HEADERS );

  uchar frame_body[ 512 ];
  ulong frame_sz = fd_h2_frame_length( frame_hdr.typlen );
  FD_TEST( fd_h2_rbuf_used_sz( rbuf )>=frame_sz );
  FD_TEST( fd_h2_rbuf_used_sz( rbuf )<=sizeof(frame_body) );
  fd_h2_rbuf_pop_copy( rbuf, frame_body, frame_sz );

  fd_hpack_rd_t hpack_rd[1];
  FD_TEST( fd_hpack_rd_init( hpack_rd, frame_body, frame_sz ) );

  while( *pstr ) {
    char const * exp_name = *(pstr++);
    char const * exp_val  = *(pstr++);
    fd_h2_hdr_t hdr;
    uchar scratch[ 128 ];
    uchar * pscratch = scratch;
    FD_TEST( !fd_hpack_rd_done( hpack_rd ) );
    FD_TEST( fd_hpack_rd_next( hpack_rd, &hdr, &pscratch, scratch+sizeof(scratch) )==FD_H2_SUCCESS );

    FD_LOG_DEBUG(( "Header: %.*s: %.*s",
                   (int)hdr.name_len,  hdr.name,
                   (int)hdr.value_len, hdr.value ));
    FD_TEST( hdr.name_len ==strlen( exp_name ) && 0==memcmp( hdr.name, exp_name, hdr.name_len  ) );
    FD_TEST( hdr.value_len==strlen( exp_val  ) && 0==memcmp( hdr.value, exp_val, hdr.value_len ) );
  }
  FD_TEST( fd_hpack_rd_done( hpack_rd ) );
}

/* Verify that the client requests builder fee info */

static void
test_bundle_client_request_builder_fee_info( fd_wksp_t * wksp ) {
  test_bundle_env_t env[1];
  test_bundle_env_create( env, wksp );
  test_bundle_env_mock_conn( env );
  fd_bundle_tile_t * const state       = env->state;
  fd_grpc_client_t * const grpc_client = state->grpc_client;

  /* Client should request new builder info */
  state->builder_info_avail = 0;
  FD_TEST( state->builder_info_wait==0 );

  /* But it's blocked on stream count ... */
  FD_TEST( fd_grpc_client_request_is_blocked( state->grpc_client )==0 );
  FD_TEST( state->grpc_client->stream_cnt==2 );
  state->grpc_client->conn->peer_settings.max_concurrent_streams = 2;
  FD_TEST( fd_grpc_client_request_is_blocked( state->grpc_client )==1 );
  int charge_busy = 0;
  fd_bundle_client_step( state, &charge_busy );
  FD_TEST( state->builder_info_wait==0 );

  /* Unblock it ... */
  state->grpc_client->conn->peer_settings.max_concurrent_streams = 3;
  FD_TEST( fd_grpc_client_request_is_blocked( state->grpc_client )==0 );
  fd_bundle_client_step( state, &charge_busy );
  FD_TEST( state->builder_info_wait==1 );

  /* Get newly created stream */
  FD_TEST( !grpc_client->request_stream ); /* request instantly flushed */
  ulong const stream_id = state->grpc_client->stream_ids[ 2 ];
  fd_grpc_h2_stream_t * stream = &state->grpc_client->stream_pool[ 2 ];
  FD_TEST( stream->s.stream_id==stream_id );
  FD_TEST( stream->request_ctx==FD_BUNDLE_CLIENT_REQ_Bundle_GetBlockBuilderFeeInfo );

  /* Request header */
  char const * const hdrs[] = {
    ":method",      "POST",
    ":scheme",      "https",
    ":path",        "/block_engine.BlockEngineValidator/GetBlockBuilderFeeInfo",
    "te",           "trailers",
    "content-type", "application/grpc+proto",
    "user-agent",   "grpc-firedancer/0.0.0",
    NULL
  };
  expect_h2_hdr( grpc_client->frame_tx, stream_id, hdrs );

  /* Request body */
  fd_h2_frame_hdr_t frame_hdr;
  FD_TEST( fd_h2_rbuf_used_sz( grpc_client->frame_tx )>=sizeof(fd_h2_frame_hdr_t) );
  fd_h2_rbuf_pop_copy( grpc_client->frame_tx, &frame_hdr, sizeof(fd_h2_frame_hdr_t) );
  FD_TEST( fd_h2_frame_type( frame_hdr.typlen )==FD_H2_FRAME_TYPE_DATA );
  FD_TEST( fd_h2_frame_length( frame_hdr.typlen )==5UL );
  FD_TEST( fd_uint_bswap( frame_hdr.r_stream_id )==stream_id );
  FD_TEST( frame_hdr.flags==FD_H2_FLAG_END_STREAM );
  fd_grpc_hdr_t grpc_hdr;
  FD_TEST( fd_h2_rbuf_used_sz( grpc_client->frame_tx )>=sizeof(fd_grpc_hdr_t) );
  fd_h2_rbuf_pop_copy( grpc_client->frame_tx, &grpc_hdr, sizeof(fd_grpc_hdr_t) );
  FD_TEST( grpc_hdr.compressed==0 );
  FD_TEST( grpc_hdr.msg_sz==0 );

  /* Inject a response */
  fd_bundle_client_grpc_rx_start( state, FD_BUNDLE_CLIENT_REQ_Bundle_GetBlockBuilderFeeInfo );

  uchar prev_builder_pubkey[ 32 ];
  for( ulong i=0UL; i<sizeof(prev_builder_pubkey); i++ ) prev_builder_pubkey[ i ] = (uchar)( i + 1U );
  fd_memcpy( state->builder_pubkey, prev_builder_pubkey, sizeof(prev_builder_pubkey) );
  uchar prev_builder_commission = 11U;
  state->builder_commission = prev_builder_commission;
  long prev_builder_valid_until = 123456789L;
  state->builder_info_valid_until = prev_builder_valid_until;

  /* Protobuf encoder util */
  uchar pb_buf[ 128 ];
  ulong pb_sz = 0UL;
  block_engine_BlockBuilderFeeInfoResponse resp = block_engine_BlockBuilderFeeInfoResponse_init_default;
#define ENCODE_MSG() do { \
    pb_ostream_t ostream = pb_ostream_from_buffer( pb_buf, sizeof(pb_buf) ); \
    FD_TEST( pb_encode( &ostream, &block_engine_BlockBuilderFeeInfoResponse_msg, &resp ) ); \
    pb_sz = ostream.bytes_written; \
  } while(0)

  /* Invalid Base58 */
  strcpy( resp.pubkey, "hello" );
  resp.commission = 2;
  ENCODE_MSG();
  fd_bundle_client_grpc_rx_msg( state, pb_buf, pb_sz, FD_BUNDLE_CLIENT_REQ_Bundle_GetBlockBuilderFeeInfo );
  FD_TEST( state->builder_info_avail==0 );
  FD_TEST( state->builder_info_wait==1 ); /* retry ... */
  FD_TEST( state->builder_commission==prev_builder_commission );
  FD_TEST( 0==memcmp( state->builder_pubkey, prev_builder_pubkey, sizeof(prev_builder_pubkey) ) );
  FD_TEST( state->builder_info_valid_until==prev_builder_valid_until );

  /* Invalid commission */
  uchar const pubkey[32] = { 1,2,3,4,5 };
  fd_base58_encode_32( pubkey, NULL, resp.pubkey );
  resp.commission = 101;
  ENCODE_MSG();
  fd_bundle_client_grpc_rx_msg( state, pb_buf, pb_sz, FD_BUNDLE_CLIENT_REQ_Bundle_GetBlockBuilderFeeInfo );
  FD_TEST( state->builder_info_avail==0 );
  FD_TEST( state->builder_info_wait==1 ); /* retry ... */
  FD_TEST( state->builder_commission==prev_builder_commission );
  FD_TEST( 0==memcmp( state->builder_pubkey, prev_builder_pubkey, sizeof(prev_builder_pubkey) ) );
  FD_TEST( state->builder_info_valid_until==prev_builder_valid_until );

  /* Valid response */
  resp.commission = 2;
  ENCODE_MSG();
  fd_bundle_client_grpc_rx_msg( state, pb_buf, pb_sz, FD_BUNDLE_CLIENT_REQ_Bundle_GetBlockBuilderFeeInfo );
  FD_TEST( state->builder_info_avail==1 );
  FD_TEST( state->builder_info_wait==1 );
  FD_TEST( state->builder_commission==2U );
  uchar decoded_builder_pubkey[ 32 ];
  FD_TEST( fd_base58_decode_32( resp.pubkey, decoded_builder_pubkey ) );
  FD_TEST( 0==memcmp( state->builder_pubkey, decoded_builder_pubkey, sizeof(decoded_builder_pubkey) ) );
  FD_TEST( state->builder_info_valid_until!=prev_builder_valid_until );

  /* End stream */
  fd_grpc_resp_hdrs_t grpc_resp_hdrs = {
    .h2_status   = 200,
    .grpc_status = FD_GRPC_STATUS_OK
  };
  fd_bundle_client_grpc_rx_end( state, FD_BUNDLE_CLIENT_REQ_Bundle_GetBlockBuilderFeeInfo, &grpc_resp_hdrs );
  FD_TEST( state->builder_info_wait==0 );

#undef ENCODE_MSG

  test_bundle_env_destroy( env );
}

/* Verify that the client subscribes to packets */

static void
test_bundle_client_subscribe_packets( fd_wksp_t * wksp ) {
  test_bundle_env_t env[1];
  test_bundle_env_create( env, wksp );
  test_bundle_env_mock_conn( env );
  fd_bundle_tile_t * const state       = env->state;
  fd_grpc_client_t * const grpc_client = state->grpc_client;

  state->packet_subscription_live = 0;
  FD_TEST( state->packet_subscription_wait==0 );

  /* But it's blocked on stream count ... */
  FD_TEST( fd_grpc_client_request_is_blocked( state->grpc_client )==0 );
  FD_TEST( state->grpc_client->stream_cnt==2 );
  state->grpc_client->conn->peer_settings.max_concurrent_streams = 2;
  FD_TEST( fd_grpc_client_request_is_blocked( state->grpc_client )==1 );
  int charge_busy = 0;
  fd_bundle_client_step( state, &charge_busy );
  FD_TEST( state->packet_subscription_wait==0 );

  /* Unblock it ... */
  state->grpc_client->conn->peer_settings.max_concurrent_streams = 3;
  FD_TEST( fd_grpc_client_request_is_blocked( state->grpc_client )==0 );
  fd_bundle_client_step( state, &charge_busy );
  FD_TEST( state->packet_subscription_wait==1 );

  /* Get newly created stream */
  FD_TEST( !grpc_client->request_stream ); /* request instantly flushed */
  ulong const stream_id = state->grpc_client->stream_ids[ 2 ];
  fd_grpc_h2_stream_t * stream = &state->grpc_client->stream_pool[ 2 ];
  FD_TEST( stream->s.stream_id==stream_id );
  FD_TEST( stream->request_ctx==FD_BUNDLE_CLIENT_REQ_Bundle_SubscribePackets );

  /* Request header */
  char const * const hdrs[] = {
    ":method",      "POST",
    ":scheme",      "https",
    ":path",        "/block_engine.BlockEngineValidator/SubscribePackets",
    "te",           "trailers",
    "content-type", "application/grpc+proto",
    "user-agent",   "grpc-firedancer/0.0.0",
    NULL
  };
  expect_h2_hdr( grpc_client->frame_tx, stream_id, hdrs );

  /* Request body */
  fd_h2_frame_hdr_t frame_hdr;
  FD_TEST( fd_h2_rbuf_used_sz( grpc_client->frame_tx )>=sizeof(fd_h2_frame_hdr_t) );
  fd_h2_rbuf_pop_copy( grpc_client->frame_tx, &frame_hdr, sizeof(fd_h2_frame_hdr_t) );
  FD_TEST( fd_h2_frame_type( frame_hdr.typlen )==FD_H2_FRAME_TYPE_DATA );
  FD_TEST( fd_h2_frame_length( frame_hdr.typlen )==5UL );
  FD_TEST( fd_uint_bswap( frame_hdr.r_stream_id )==stream_id );
  FD_TEST( frame_hdr.flags==FD_H2_FLAG_END_STREAM );
  fd_grpc_hdr_t grpc_hdr;
  FD_TEST( fd_h2_rbuf_used_sz( grpc_client->frame_tx )>=sizeof(fd_grpc_hdr_t) );
  fd_h2_rbuf_pop_copy( grpc_client->frame_tx, &grpc_hdr, sizeof(fd_grpc_hdr_t) );
  FD_TEST( grpc_hdr.compressed==0 );
  FD_TEST( grpc_hdr.msg_sz==0 );

  /* Inject a response */
  FD_TEST( state->packet_subscription_wait==1 );
  fd_bundle_client_grpc_rx_start( state, FD_BUNDLE_CLIENT_REQ_Bundle_SubscribePackets );
  FD_TEST( state->packet_subscription_wait==0 );

  test_bundle_env_destroy( env );
}

/* ========== Harmonic block tests ========== */

/* Helper to encode a block message with given slot and txn count.
   Returns the size of the encoded message. */
static ulong
encode_block_msg( uchar * buf, ulong buf_sz, ulong slot, ulong txn_cnt, ulong txn_sz ) {
  /* Slot as string */
  char slot_str[21];
  ulong slot_str_len = 0;
  ulong s = slot;
  if( s==0 ) {
    slot_str[slot_str_len++] = '0';
  } else {
    char tmp[21];
    ulong tmp_len = 0;
    while( s ) {
      tmp[tmp_len++] = (char)('0' + (s % 10));
      s /= 10;
    }
    for( ulong i=0; i<tmp_len; i++ ) {
      slot_str[slot_str_len++] = tmp[tmp_len-1-i];
    }
  }
  slot_str[slot_str_len] = '\0';

  /* Create a single packet proto */
  uchar single_pkt[2100];
  pb_ostream_t pkt_stream = pb_ostream_from_buffer( single_pkt, sizeof(single_pkt) );
  packet_Packet pkt = packet_Packet_init_default;
  pkt.data.size = (pb_size_t)txn_sz;
  memset( pkt.data.bytes, 0x42, txn_sz );
  pkt.has_meta = 0;
  if( !pb_encode( &pkt_stream, &packet_Packet_msg, &pkt ) ) {
    FD_LOG_WARNING(( "pb_encode failed: %s", PB_GET_ERROR(&pkt_stream) ));
    return 0;
  }
  ulong single_pkt_sz = pkt_stream.bytes_written;

  /* Calculate Bundle size */
  ulong bundle_content_sz = 0;
  for( ulong i=0; i<txn_cnt; i++ ) {
    bundle_content_sz += 1;  /* field tag 0x1a */
    ulong sz = single_pkt_sz;
    do { bundle_content_sz++; sz >>= 7; } while( sz );
    bundle_content_sz += single_pkt_sz;
  }

  /* Calculate BundleUuid size */
  ulong bundle_uuid_content_sz = 0;
  bundle_uuid_content_sz += 1;  /* field tag 0x0a */
  ulong sz = bundle_content_sz;
  do { bundle_uuid_content_sz++; sz >>= 7; } while( sz );
  bundle_uuid_content_sz += bundle_content_sz;
  bundle_uuid_content_sz += 1;  /* field tag 0x12 */
  sz = slot_str_len;
  do { bundle_uuid_content_sz++; sz >>= 7; } while( sz );
  bundle_uuid_content_sz += slot_str_len;

  /* Helper macro to encode a varint */
  #define ENCODE_VARINT(val) do { \
    ulong _v = (val); \
    while( _v >= 0x80 ) { \
      *ptr++ = (uchar)((_v & 0x7F) | 0x80); \
      _v >>= 7; \
    } \
    *ptr++ = (uchar)_v; \
  } while(0)

  /* Encode */
  uchar * ptr = buf;
  uchar * const end = buf + buf_sz;

  /* Field 1: bundles */
  *ptr++ = 0x0a;
  ENCODE_VARINT( bundle_uuid_content_sz );

  /* BundleUuid.bundle (field 1) */
  *ptr++ = 0x0a;
  ENCODE_VARINT( bundle_content_sz );

  /* Bundle.packets */
  for( ulong i=0; i<txn_cnt; i++ ) {
    if( ptr + single_pkt_sz + 10 >= end ) {
      FD_LOG_WARNING(( "Buffer overflow at packet %lu", i ));
      return 0;
    }
    *ptr++ = 0x1a;
    ENCODE_VARINT( single_pkt_sz );
    memcpy( ptr, single_pkt, single_pkt_sz );
    ptr += single_pkt_sz;
  }

  /* BundleUuid.uuid (field 2) */
  *ptr++ = 0x12;
  ENCODE_VARINT( slot_str_len );

  #undef ENCODE_VARINT
  memcpy( ptr, slot_str, slot_str_len );
  ptr += slot_str_len;

  return (ulong)(ptr - buf);
}

/* Static buffer for block message with slot 12345678 */
static uchar subscribe_blocks_msg_slot_12345678[256];
static ulong subscribe_blocks_msg_slot_12345678_sz = 0;

/* Static buffer for block message with slot 99999999 */
static uchar subscribe_blocks_msg_slot_99999999[512];
static ulong subscribe_blocks_msg_slot_99999999_sz = 0;

/* Static buffer for block message with 8 txns, slot 55555555 */
static uchar subscribe_blocks_msg_8txns[512];
static ulong subscribe_blocks_msg_8txns_sz = 0;

/* Initialize the block test messages (call once at start of tests) */
static void
init_block_test_messages( void ) {
  /* 2 txns, 1 byte each, slot=12345678 */
  subscribe_blocks_msg_slot_12345678_sz = encode_block_msg(
      subscribe_blocks_msg_slot_12345678, sizeof(subscribe_blocks_msg_slot_12345678),
      12345678UL, 2UL, 1UL );
  FD_TEST( subscribe_blocks_msg_slot_12345678_sz > 0 );

  /* 6 txns, 1 byte each, slot=99999999 */
  subscribe_blocks_msg_slot_99999999_sz = encode_block_msg(
      subscribe_blocks_msg_slot_99999999, sizeof(subscribe_blocks_msg_slot_99999999),
      99999999UL, 6UL, 1UL );
  FD_TEST( subscribe_blocks_msg_slot_99999999_sz > 0 );

  /* 8 txns, 1 byte each, slot=55555555 */
  subscribe_blocks_msg_8txns_sz = encode_block_msg(
      subscribe_blocks_msg_8txns, sizeof(subscribe_blocks_msg_8txns),
      55555555UL, 8UL, 1UL );
  FD_TEST( subscribe_blocks_msg_8txns_sz > 0 );
}

static ulong published_txn_cnt( test_bundle_env_t const * env );
static fd_txn_m_t const * published_txn( test_bundle_env_t const * env, ulong seq, fd_frag_meta_t const ** opt_meta );
static void expect_published_harmonic_txn( test_bundle_env_t const * env, ulong seq, uchar const * payload, ulong payload_sz, ulong block_slot, ushort block_txn_cnt, uchar commission, uchar const * commission_pubkey );
static ulong flush_harmonic_staging( fd_bundle_tile_t * state );
static ulong flush_harmonic_staging_budgeted( fd_bundle_tile_t * state, ulong budget );
static ulong publish_after_credit( fd_bundle_tile_t * state );

/* Test that harmonic blocks correctly parse slot from uuid and tag txn_m */
static void
test_harmonic_block_slot_parsing( fd_wksp_t * wksp ) {
  FD_LOG_NOTICE(( "Testing harmonic block slot parsing from uuid" ));
  test_bundle_env_t env[1]; test_bundle_env_create( env, wksp );
  test_bundle_env_enable_harmonic_block_mode( env, wksp );
  test_bundle_env_mock_harmonic_block_conn( env );
  fd_bundle_tile_t * state = env->state;

  state->builder_info_avail  = 1;
  state->builder_commission  = 10U;
  uchar builder_pubkey[ 32 ];
  for( ulong i=0UL; i<32UL; i++ ) builder_pubkey[ i ] = (uchar)( i + 0xA0U );
  fd_memcpy( state->builder_pubkey, builder_pubkey, 32UL );

  /* Receive block with slot=12345678 (2 txns <= 32 → staging path) */
  fd_bundle_client_grpc_callbacks.rx_msg(
      state,
      subscribe_blocks_msg_slot_12345678, subscribe_blocks_msg_slot_12345678_sz,
      FD_BUNDLE_CLIENT_REQ_SubscribeBlocks
  );

  FD_TEST( state->harmonic_block_slot==12345678UL );
  FD_TEST( state->harmonic_block_received_cnt==1UL );
  FD_TEST( state->harmonic_pending_len==2UL );
  FD_TEST( state->harmonic_block_txn_received_cnt==0UL );
  FD_TEST( published_txn_cnt( env )==0UL );

  /* Flush staging (simulates after_credit) */
  FD_TEST( flush_harmonic_staging( state )==2UL );
  FD_TEST( state->harmonic_block_txn_received_cnt==2UL );
  FD_TEST( published_txn_cnt( env )==2UL );

  uchar const expected_payload[] = { 0x42 };
  expect_published_harmonic_txn( env, 0UL, expected_payload, 1UL, 12345678UL, 2, 10U, builder_pubkey );
  expect_published_harmonic_txn( env, 1UL, expected_payload, 1UL, 12345678UL, 2, 10U, builder_pubkey );

  FD_LOG_NOTICE(( "Harmonic block slot parsing test passed (slot=12345678)" ));
  test_bundle_env_destroy( env );
}

/* Test that harmonic blocks can have > 5 transactions (unlike regular bundles) */
static void
test_harmonic_block_rx_many_txns( fd_wksp_t * wksp ) {
  FD_LOG_NOTICE(( "Testing harmonic block rx with many txns (no len=5 limit)" ));
  test_bundle_env_t env[1]; test_bundle_env_create( env, wksp );
  test_bundle_env_enable_harmonic_block_mode( env, wksp );
  test_bundle_env_mock_harmonic_block_conn( env );
  fd_bundle_tile_t * state = env->state;

  state->builder_info_avail = 1;

  /* Test 6 transactions (6 <= 32 → staging path) */
  fd_bundle_client_grpc_callbacks.rx_msg(
      state,
      subscribe_blocks_msg_slot_99999999, subscribe_blocks_msg_slot_99999999_sz,
      FD_BUNDLE_CLIENT_REQ_SubscribeBlocks
  );

  FD_TEST( state->harmonic_block_slot==99999999UL );
  FD_TEST( state->harmonic_block_received_cnt==1UL );
  FD_TEST( state->harmonic_pending_len==6UL );
  FD_TEST( state->harmonic_block_txn_received_cnt==0UL );

  FD_TEST( flush_harmonic_staging( state )==6UL );
  FD_TEST( state->harmonic_block_txn_received_cnt==6UL );
  FD_TEST( published_txn_cnt( env )==6UL );

  fd_frag_meta_t const * meta = NULL;
  fd_txn_m_t const * txnm0 = published_txn( env, 0UL, &meta );
  FD_TEST( meta->sig==2UL );
  FD_TEST( txnm0->source_tpu==FD_TXN_M_TPU_SOURCE_HARMONIC );
  FD_TEST( txnm0->block_engine.block_slot==99999999UL );

  FD_LOG_NOTICE(( "Harmonic block rx many txns test passed (6 txns ok, slot=99999999)" ));
  test_bundle_env_destroy( env );
}

/* Test ingestion from all 3 sources: packets, bundles, and harmonic blocks */
static void
test_all_three_sources( fd_wksp_t * wksp ) {
  FD_LOG_NOTICE(( "Testing all 3 sources: packets, bundles, and harmonic blocks" ));
  test_bundle_env_t env[1]; test_bundle_env_create( env, wksp );
  test_bundle_env_enable_harmonic_block_mode( env, wksp );
  test_bundle_env_mock_harmonic_block_conn( env );
  fd_bundle_tile_t * state = env->state;

  state->builder_info_avail = 1;

  /* 1. Receive packets (2 packets → pending deque) */
  static uchar subscribe_packets_msg[] = {
    0x12, 0x13, 0x0a, 0x07, 0x0a, 0x01, 0x48, 0x12,
    0x02, 0x08, 0x01, 0x0a, 0x08, 0x0a, 0x02, 0x48,
    0x48, 0x12, 0x02, 0x08, 0x02
  };
  fd_bundle_client_grpc_rx_msg(
      state,
      subscribe_packets_msg, sizeof(subscribe_packets_msg),
      FD_BUNDLE_CLIENT_REQ_Bundle_SubscribePackets
  );
  FD_TEST( state->metrics.packet_received_cnt==2UL );
  FD_TEST( pending_txn_cnt( state->pending_txns )==2UL );

  /* 2. Receive bundles (5 txns → pending deque) */
  fd_bundle_client_grpc_rx_msg(
      state,
      subscribe_bundles_msg_x5, sizeof(subscribe_bundles_msg_x5),
      FD_BUNDLE_CLIENT_REQ_Bundle_SubscribeBundles
  );
  FD_TEST( state->metrics.bundle_received_cnt==1UL );
  FD_TEST( pending_txn_cnt( state->pending_txns )==7UL );

  /* 3. Receive harmonic blocks (6 txns → staging) */
  fd_bundle_client_grpc_callbacks.rx_msg(
      state,
      subscribe_blocks_msg_slot_99999999, subscribe_blocks_msg_slot_99999999_sz,
      FD_BUNDLE_CLIENT_REQ_SubscribeBlocks
  );
  FD_TEST( state->harmonic_block_received_cnt==1UL );
  FD_TEST( state->harmonic_pending_len==6UL );
  FD_TEST( state->harmonic_block_slot==99999999UL );

  /* Nothing published yet */
  FD_TEST( published_txn_cnt( env )==0UL );

  /* Flush harmonic staging (6 published) */
  FD_TEST( flush_harmonic_staging( state )==6UL );
  FD_TEST( state->harmonic_block_txn_received_cnt==6UL );

  /* Drain pending deque: 2 packets then 5 bundle txns */
  FD_TEST( publish_after_credit( state )==2UL );
  FD_TEST( publish_after_credit( state )==5UL );
  FD_TEST( pending_txn_empty( state->pending_txns ) );

  FD_TEST( published_txn_cnt( env )==13UL );

  FD_LOG_NOTICE(( "All 3 sources test passed (6 harmonic + 2 packets + 5 bundle txns = 13)" ));
  test_bundle_env_destroy( env );
}

/* Test that harmonic blocks and bundles can be received interleaved.
   Must flush harmonic staging between harmonic rx_msg calls since
   staging can only hold one block at a time. */
static void
test_interleaved_sources( fd_wksp_t * wksp ) {
  FD_LOG_NOTICE(( "Testing interleaved bundles and harmonic blocks" ));
  test_bundle_env_t env[1]; test_bundle_env_create( env, wksp );
  test_bundle_env_enable_harmonic_block_mode( env, wksp );
  test_bundle_env_mock_harmonic_block_conn( env );
  fd_bundle_tile_t * state = env->state;

  state->builder_info_avail = 1;

  /* Bundle 1 (5 txns → pending deque) */
  fd_bundle_client_grpc_rx_msg(
      state,
      subscribe_bundles_msg_x5, sizeof(subscribe_bundles_msg_x5),
      FD_BUNDLE_CLIENT_REQ_Bundle_SubscribeBundles
  );
  FD_TEST( state->metrics.bundle_received_cnt==1UL );
  FD_TEST( pending_txn_cnt( state->pending_txns )==5UL );

  /* Harmonic block 1 (6 txns, slot=99999999 → staging) */
  fd_bundle_client_grpc_callbacks.rx_msg(
      state,
      subscribe_blocks_msg_slot_99999999, subscribe_blocks_msg_slot_99999999_sz,
      FD_BUNDLE_CLIENT_REQ_SubscribeBlocks
  );
  FD_TEST( state->harmonic_block_received_cnt==1UL );
  FD_TEST( state->harmonic_block_slot==99999999UL );
  FD_TEST( state->harmonic_pending_len==6UL );

  /* Flush staging before next harmonic block */
  FD_TEST( flush_harmonic_staging( state )==6UL );

  /* Bundle 2 (5 txns → pending deque) */
  fd_bundle_client_grpc_rx_msg(
      state,
      subscribe_bundles_msg_x5, sizeof(subscribe_bundles_msg_x5),
      FD_BUNDLE_CLIENT_REQ_Bundle_SubscribeBundles
  );
  FD_TEST( state->metrics.bundle_received_cnt==2UL );
  FD_TEST( pending_txn_cnt( state->pending_txns )==10UL );

  /* Harmonic block 2 (2 txns, slot=12345678 → staging) */
  fd_bundle_client_grpc_callbacks.rx_msg(
      state,
      subscribe_blocks_msg_slot_12345678, subscribe_blocks_msg_slot_12345678_sz,
      FD_BUNDLE_CLIENT_REQ_SubscribeBlocks
  );
  FD_TEST( state->harmonic_block_received_cnt==2UL );
  FD_TEST( state->harmonic_block_slot==12345678UL );
  FD_TEST( state->harmonic_pending_len==2UL );

  FD_TEST( flush_harmonic_staging( state )==2UL );

  /* Drain pending deque: bundle 1 (5) + bundle 2 (5) */
  FD_TEST( publish_after_credit( state )==5UL );
  FD_TEST( publish_after_credit( state )==5UL );
  FD_TEST( pending_txn_empty( state->pending_txns ) );

  FD_TEST( published_txn_cnt( env )==18UL );

  FD_LOG_NOTICE(( "Interleaved sources test passed (6 + 5 + 2 + 5 = 18 txns)" ));
  test_bundle_env_destroy( env );
}

/* Test budget-limited harmonic staging drain with memmove.
   Stages 8 txns, drains TEST_STEM_BURST (5), verifies memmove shifts
   remaining 3 correctly, then drains those. */
static void
test_harmonic_staging_partial_drain( fd_wksp_t * wksp ) {
  FD_LOG_NOTICE(( "Testing harmonic staging partial drain with memmove" ));
  test_bundle_env_t env[1]; test_bundle_env_create( env, wksp );
  test_bundle_env_enable_harmonic_block_mode( env, wksp );
  test_bundle_env_mock_harmonic_block_conn( env );
  fd_bundle_tile_t * state = env->state;

  state->builder_info_avail = 1;

  fd_bundle_client_grpc_callbacks.rx_msg(
      state,
      subscribe_blocks_msg_8txns, subscribe_blocks_msg_8txns_sz,
      FD_BUNDLE_CLIENT_REQ_SubscribeBlocks
  );
  FD_TEST( state->harmonic_pending_len==8UL );
  FD_TEST( state->harmonic_block_slot==55555555UL );
  FD_TEST( published_txn_cnt( env )==0UL );

  /* Tag each staged txn with a distinct marker so we can verify
     memmove correctness after partial drain. */
  for( ulong i=0UL; i<8UL; i++ ) {
    state->harmonic_staging[i].payload[0] = (uchar)i;
  }

  /* Drain with budget=TEST_STEM_BURST (5).  Should publish 5,
     memmove the remaining 3. */
  FD_TEST( flush_harmonic_staging_budgeted( state, TEST_STEM_BURST )==5UL );
  FD_TEST( state->harmonic_pending_len==3UL );
  FD_TEST( published_txn_cnt( env )==5UL );

  /* Verify memmove shifted txns [5,6,7] to positions [0,1,2]. */
  FD_TEST( state->harmonic_staging[0].payload[0]==5 );
  FD_TEST( state->harmonic_staging[1].payload[0]==6 );
  FD_TEST( state->harmonic_staging[2].payload[0]==7 );

  /* Drain remaining 3. */
  FD_TEST( flush_harmonic_staging_budgeted( state, TEST_STEM_BURST )==3UL );
  FD_TEST( state->harmonic_pending_len==0UL );
  FD_TEST( published_txn_cnt( env )==8UL );

  FD_LOG_NOTICE(( "Harmonic staging partial drain test passed" ));
  test_bundle_env_destroy( env );
}

/* Verify that the before_credit gate defers gRPC processing while
   harmonic staging is occupied.  In production, before_credit returns
   early when harmonic_pending_len>0, which prevents new block data
   from being read — no data is dropped, just delayed until staging
   drains.  This test verifies the invariant those gates rely on. */
static void
test_harmonic_before_credit_gate( fd_wksp_t * wksp ) {
  FD_LOG_NOTICE(( "Testing before_credit gate defers gRPC during staging" ));
  test_bundle_env_t env[1]; test_bundle_env_create( env, wksp );
  test_bundle_env_enable_harmonic_block_mode( env, wksp );
  test_bundle_env_mock_harmonic_block_conn( env );
  fd_bundle_tile_t * state = env->state;

  state->builder_info_avail = 1;

  /* Receive a harmonic block (2 txns) → staging is occupied */
  fd_bundle_client_grpc_callbacks.rx_msg(
      state,
      subscribe_blocks_msg_slot_12345678, subscribe_blocks_msg_slot_12345678_sz,
      FD_BUNDLE_CLIENT_REQ_SubscribeBlocks
  );
  FD_TEST( state->harmonic_pending_len==2UL );

  /* Gate is active: harmonic_pending_len > 0.  In production,
     before_credit would return here without calling
     fd_bundle_client_step, so no new gRPC data is consumed. */

  /* Also push some packets into the pending deque to show that both
     the harmonic gate AND the pending_txn_empty gate in before_credit
     would prevent fd_bundle_client_step from being called. */
  static uchar subscribe_packets_msg[] = {
    0x12, 0x09, 0x0a, 0x07, 0x0a, 0x01, 0x48, 0x12,
    0x02, 0x08, 0x01
  };
  fd_bundle_client_grpc_rx_msg(
      state,
      subscribe_packets_msg, sizeof(subscribe_packets_msg),
      FD_BUNDLE_CLIENT_REQ_Bundle_SubscribePackets
  );
  FD_TEST( pending_txn_cnt( state->pending_txns )==1UL );

  /* Both gates are active.  Harmonic gate takes priority. */
  FD_TEST( state->harmonic_pending_len > 0UL );

  /* Flush harmonic staging → harmonic gate opens */
  FD_TEST( flush_harmonic_staging( state )==2UL );
  FD_TEST( state->harmonic_pending_len==0UL );

  /* Now before_credit would check pending_txns (non-empty → still no
     gRPC step).  Drain the pending deque too. */
  FD_TEST( publish_after_credit( state )==1UL );
  FD_TEST( pending_txn_empty( state->pending_txns ) );

  /* Both gates are now open.  A new harmonic block can be received. */
  fd_bundle_client_grpc_callbacks.rx_msg(
      state,
      subscribe_blocks_msg_slot_99999999, subscribe_blocks_msg_slot_99999999_sz,
      FD_BUNDLE_CLIENT_REQ_SubscribeBlocks
  );
  FD_TEST( state->harmonic_pending_len==6UL );
  FD_TEST( state->harmonic_block_slot==99999999UL );
  FD_TEST( state->harmonic_block_received_cnt==2UL );

  FD_TEST( flush_harmonic_staging( state )==6UL );
  FD_TEST( published_txn_cnt( env )==9UL );

  FD_LOG_NOTICE(( "before_credit gate test passed" ));
  test_bundle_env_destroy( env );
}

/* Verify that fd_bundle_client_reset clears harmonic staging state
   so a reconnection starts clean. */
static void
test_reset_clears_harmonic_state( fd_wksp_t * wksp ) {
  FD_LOG_NOTICE(( "Testing reset clears harmonic state" ));
  test_bundle_env_t env[1]; test_bundle_env_create( env, wksp );
  test_bundle_env_enable_harmonic_block_mode( env, wksp );
  test_bundle_env_mock_harmonic_block_conn( env );
  fd_bundle_tile_t * state = env->state;

  state->builder_info_avail = 1;

  /* Stage some harmonic txns */
  fd_bundle_client_grpc_callbacks.rx_msg(
      state,
      subscribe_blocks_msg_slot_12345678, subscribe_blocks_msg_slot_12345678_sz,
      FD_BUNDLE_CLIENT_REQ_SubscribeBlocks
  );
  FD_TEST( state->harmonic_pending_len==2UL );
  FD_TEST( state->harmonic_staged_block_slot==12345678UL );
  FD_TEST( state->harmonic_staged_block_txn_cnt==2 );
  FD_TEST( state->harmonic_block_received_cnt==1UL );

  /* Reset must clear harmonic staging so stale data does not leak
     across reconnections. */
  fd_bundle_client_reset( state );

  FD_TEST( state->harmonic_pending_len==0UL );
  FD_TEST( state->harmonic_staged_block_slot==0UL );
  FD_TEST( state->harmonic_staged_block_txn_cnt==0 );
  FD_TEST( state->harmonic_block_subscription_live==0 );
  FD_TEST( state->harmonic_block_subscription_wait==0 );

  FD_LOG_NOTICE(( "Reset clears harmonic state test passed" ));
  test_bundle_env_destroy( env );
}

/* Test oversized block: 50,000 transactions, 600 bytes each.

   NOTE: This test predates the staging-based flow and assumed direct
   in-callback publishes.  Block stages are now buffered in
   harmonic_staging (sized to bundle.out_depth, default 16384) and
   drained from after_credit, so a single rx_msg cannot publish more
   than the staging capacity.  In production the server caps a single
   stage at 32 txns; nothing larger than that should ever be received.
   Keeping the test code in case it is useful for future regression
   work, but it is intentionally not invoked from main(). */
FD_FN_UNUSED static void
test_harmonic_block_oversized( fd_wksp_t * wksp ) {
  (void)wksp;  /* Use a separate larger workspace for this test */
  FD_LOG_NOTICE(( "Testing oversized harmonic block (50000 txns x 600 bytes)" ));

  ulong const txn_cnt  = 50000UL;
  ulong const txn_sz   = 600UL;
  ulong const slot     = 123456789UL;

  /* Slot as string: "123456789" (9 chars) */
  char const slot_str[] = "123456789";
  ulong const slot_str_len = sizeof(slot_str) - 1UL;

  /* Calculate buffer size needed for the protobuf message.
     Each packet in protobuf is roughly:
     - 2 bytes: field tag + length prefix for data
     - txn_sz bytes: data
     - ~12 bytes: meta fields
     Plus overhead for BundleUuid, Bundle wrapper, etc. */
  ulong const pkt_overhead = 20UL;
  ulong const msg_sz = txn_cnt * (txn_sz + pkt_overhead) + 256UL;

  /* Create a larger workspace for this test (~40MB message + overhead) */
  ulong cpu_idx = fd_tile_cpu_id( fd_tile_idx() );
  if( cpu_idx>fd_shmem_cpu_cnt() ) cpu_idx = 0UL;
  // fd_wksp_t * big_wksp = fd_wksp_new_anonymous(
  //     fd_cstr_to_shmem_page_sz( "gigantic" ),
  //     2UL,  /* 2 gigantic pages = 2GB, but will only use what we need */
  //     fd_shmem_cpu_idx( fd_shmem_numa_idx( cpu_idx ) ),
  //     "big_wksp", 16UL );
  // if( !big_wksp ) {
    /* Fall back to huge pages if gigantic not available */
  fd_wksp_t * big_wksp = fd_wksp_new_anonymous(
        fd_cstr_to_shmem_page_sz( "huge" ),
        32UL,  /* 32 huge pages = 64MB */
        fd_shmem_cpu_idx( fd_shmem_numa_idx( cpu_idx ) ),
        "big_wksp", 16UL );
  // }
  FD_TEST( big_wksp );

  uchar * msg_buf = fd_wksp_alloc_laddr( big_wksp, 1UL, msg_sz, 1UL );
  FD_TEST( msg_buf );

  /* Build the protobuf message manually for efficiency.
     Structure: SubscribeBundlesResponse { bundles: [ BundleUuid { bundle: Bundle { packets: [...] }, uuid: "123456789" } ] } */

  uchar * ptr = msg_buf;
  uchar * const msg_end = msg_buf + msg_sz;

  /* Build packets data first to know sizes */
  uchar txn_data[600];
  memset( txn_data, 0x42, txn_sz );  /* Fill with 0x42 */

  /* Create a single packet proto (will be repeated) */
  uchar single_pkt[700];
  pb_ostream_t pkt_stream = pb_ostream_from_buffer( single_pkt, sizeof(single_pkt) );
  packet_Packet pkt = packet_Packet_init_default;
  pkt.data.size = (pb_size_t)txn_sz;
  memcpy( pkt.data.bytes, txn_data, txn_sz );
  pkt.has_meta = 0;
  FD_TEST( pb_encode( &pkt_stream, &packet_Packet_msg, &pkt ) );
  ulong single_pkt_sz = pkt_stream.bytes_written;

  /* Calculate Bundle size (packets field is repeated, field tag=3) */
  ulong pkt_varint_len = 0; { ulong v = single_pkt_sz; do { pkt_varint_len++; v >>= 7; } while( v ); }
  ulong bundle_content_sz = txn_cnt * (1UL + pkt_varint_len + single_pkt_sz);

  /* Calculate varint length for bundle_content_sz */
  ulong bundle_varint_len = 0; { ulong v = bundle_content_sz; do { bundle_varint_len++; v >>= 7; } while( v ); }

  /* Calculate varint length for slot_str_len */
  ulong slot_varint_len = 0; { ulong v = slot_str_len; do { slot_varint_len++; v >>= 7; } while( v ); }

  /* Calculate BundleUuid size */
  ulong bundle_uuid_content_sz = 0UL;
  bundle_uuid_content_sz += 1UL;  /* field 1 tag */
  bundle_uuid_content_sz += bundle_varint_len;
  bundle_uuid_content_sz += bundle_content_sz;
  bundle_uuid_content_sz += 1UL;  /* field 2 tag */
  bundle_uuid_content_sz += slot_varint_len;
  bundle_uuid_content_sz += slot_str_len;

  /* Encode SubscribeBundlesResponse { bundles: [ ... ] } */
  /* Field 1: bundles (repeated BundleUuid) */
  *ptr++ = 0x0a;  /* field 1, wire type 2 (length-delimited) */

  /* Encode varint for bundle_uuid size */
  ulong remaining = bundle_uuid_content_sz;
  while( remaining >= 0x80 ) {
    *ptr++ = (uchar)((remaining & 0x7F) | 0x80);
    remaining >>= 7;
  }
  *ptr++ = (uchar)remaining;

  /* BundleUuid { bundle: Bundle, uuid: string } */
  /* Field 1: bundle */
  *ptr++ = 0x0a;  /* field 1, wire type 2 */

  /* Encode varint for bundle size */
  remaining = bundle_content_sz;
  while( remaining >= 0x80 ) {
    *ptr++ = (uchar)((remaining & 0x7F) | 0x80);
    remaining >>= 7;
  }
  *ptr++ = (uchar)remaining;

  /* Bundle { packets: [...] } - encode 50000 packets */
  for( ulong i=0; i<txn_cnt; i++ ) {
    FD_TEST( ptr + single_pkt_sz + 3 < msg_end );
    *ptr++ = 0x1a;  /* field 3 (packets), wire type 2 */
    /* Encode varint for packet size */
    remaining = single_pkt_sz;
    while( remaining >= 0x80 ) {
      *ptr++ = (uchar)((remaining & 0x7F) | 0x80);
      remaining >>= 7;
    }
    *ptr++ = (uchar)remaining;
    memcpy( ptr, single_pkt, single_pkt_sz );
    ptr += single_pkt_sz;
  }

  /* Field 2: uuid */
  *ptr++ = 0x12;  /* field 2, wire type 2 */
  *ptr++ = (uchar)slot_str_len;
  memcpy( ptr, slot_str, slot_str_len );
  ptr += slot_str_len;

  ulong actual_msg_sz = (ulong)(ptr - msg_buf);
  FD_LOG_NOTICE(( "Encoded oversized block message: %lu bytes", actual_msg_sz ));

  /* Now test receiving this block - use the big workspace */
  test_bundle_env_t env[1]; test_bundle_env_create( env, big_wksp );
  test_bundle_env_enable_harmonic_block_mode( env, big_wksp );
  test_bundle_env_mock_harmonic_block_conn( env );
  fd_bundle_tile_t * state = env->state;

  state->builder_info_avail = 1;

  fd_bundle_client_grpc_callbacks.rx_msg(
      state,
      msg_buf, actual_msg_sz,
      FD_BUNDLE_CLIENT_REQ_SubscribeBlocks
  );

  /* Verify slot was parsed correctly */
  FD_TEST( state->harmonic_block_slot==slot );
  FD_TEST( state->harmonic_block_received_cnt==1UL );
  FD_TEST( state->harmonic_block_txn_received_cnt==txn_cnt );

  /* Verify first txn_m is tagged correctly */
  fd_txn_m_t * txnm0 = fd_chunk_to_laddr( env->out_dcache, 0UL );
  FD_TEST( txnm0->reference_slot==0UL );  /* resolv will populate from blockhash */
  FD_TEST( txnm0->block_engine.block_slot==slot );
  FD_TEST( txnm0->source_tpu==FD_TXN_M_TPU_SOURCE_HARMONIC );
  FD_TEST( txnm0->payload_sz==txn_sz );

  FD_LOG_NOTICE(( "Oversized harmonic block test passed (%lu txns x %lu bytes, slot=%lu)", txn_cnt, txn_sz, slot ));

  test_bundle_env_destroy( env );
  fd_wksp_free_laddr( msg_buf );
  fd_wksp_delete_anonymous( big_wksp );
}

/* Verify that the client subscribes to bundles */

static void
test_bundle_client_subscribe_bundles( fd_wksp_t * wksp ) {
  test_bundle_env_t env[1];
  test_bundle_env_create( env, wksp );
  test_bundle_env_mock_conn( env );
  fd_bundle_tile_t * const state       = env->state;
  fd_grpc_client_t * const grpc_client = state->grpc_client;

  state->bundle_subscription_live = 0;
  FD_TEST( state->bundle_subscription_wait==0 );

  /* But it's blocked on stream count ... */
  FD_TEST( fd_grpc_client_request_is_blocked( state->grpc_client )==0 );
  FD_TEST( state->grpc_client->stream_cnt==2 );
  state->grpc_client->conn->peer_settings.max_concurrent_streams = 2;
  FD_TEST( fd_grpc_client_request_is_blocked( state->grpc_client )==1 );
  int charge_busy = 0;
  fd_bundle_client_step( state, &charge_busy );
  FD_TEST( state->bundle_subscription_wait==0 );

  /* Unblock it ... */
  state->grpc_client->conn->peer_settings.max_concurrent_streams = 3;
  FD_TEST( fd_grpc_client_request_is_blocked( state->grpc_client )==0 );
  fd_bundle_client_step( state, &charge_busy );
  FD_TEST( state->bundle_subscription_wait==1 );

  /* Get newly created stream */
  FD_TEST( !grpc_client->request_stream ); /* request instantly flushed */
  ulong const stream_id = state->grpc_client->stream_ids[ 2 ];
  fd_grpc_h2_stream_t * stream = &state->grpc_client->stream_pool[ 2 ];
  FD_TEST( stream->s.stream_id==stream_id );
  FD_TEST( stream->request_ctx==FD_BUNDLE_CLIENT_REQ_Bundle_SubscribeBundles );

  /* Request header */
  char const * const hdrs[] = {
    ":method",      "POST",
    ":scheme",      "https",
    ":path",        "/block_engine.BlockEngineValidator/SubscribeBundles",
    "te",           "trailers",
    "content-type", "application/grpc+proto",
    "user-agent",   "grpc-firedancer/0.0.0",
    NULL
  };
  expect_h2_hdr( grpc_client->frame_tx, stream_id, hdrs );

  /* Request body */
  fd_h2_frame_hdr_t frame_hdr;
  FD_TEST( fd_h2_rbuf_used_sz( grpc_client->frame_tx )>=sizeof(fd_h2_frame_hdr_t) );
  fd_h2_rbuf_pop_copy( grpc_client->frame_tx, &frame_hdr, sizeof(fd_h2_frame_hdr_t) );
  FD_TEST( fd_h2_frame_type( frame_hdr.typlen )==FD_H2_FRAME_TYPE_DATA );
  FD_TEST( fd_h2_frame_length( frame_hdr.typlen )==5UL );
  FD_TEST( fd_uint_bswap( frame_hdr.r_stream_id )==stream_id );
  FD_TEST( frame_hdr.flags==FD_H2_FLAG_END_STREAM );
  fd_grpc_hdr_t grpc_hdr;
  FD_TEST( fd_h2_rbuf_used_sz( grpc_client->frame_tx )>=sizeof(fd_grpc_hdr_t) );
  fd_h2_rbuf_pop_copy( grpc_client->frame_tx, &grpc_hdr, sizeof(fd_grpc_hdr_t) );
  FD_TEST( grpc_hdr.compressed==0 );
  FD_TEST( grpc_hdr.msg_sz==0 );

  /* Inject a response */
  FD_TEST( state->bundle_subscription_wait==1 );
  fd_bundle_client_grpc_rx_start( state, FD_BUNDLE_CLIENT_REQ_Bundle_SubscribeBundles );
  FD_TEST( state->bundle_subscription_wait==0 );

  test_bundle_env_destroy( env );
}

typedef struct {
  uchar const * payload;
  ulong         payload_sz;
} test_packet_desc_t;

typedef struct {
  test_packet_desc_t const * packets;
  ulong                      packet_cnt;
} test_packet_list_t;

typedef struct {
  test_packet_desc_t const * packets;
  ulong                      packet_cnt;
  uchar                      uuid[ 16 ];
  ulong                      uuid_sz;
} test_bundle_desc_t;

typedef struct {
  test_bundle_desc_t const * bundles;
  ulong                      bundle_cnt;
} test_bundle_list_t;

static bool
encode_test_packet_list( pb_ostream_t *     stream,
                         pb_field_t const * field,
                         void * const *     arg ) {
  test_packet_list_t const * packet_list = *arg;

  for( ulong i=0UL; i<packet_list->packet_cnt; i++ ) {
    test_packet_desc_t const * desc = &packet_list->packets[ i ];
    packet_Packet packet = packet_Packet_init_default;
    FD_TEST( desc->payload_sz<=sizeof(packet.data.bytes) );
    packet.data.size = (pb_size_t)desc->payload_sz;
    fd_memcpy( packet.data.bytes, desc->payload, desc->payload_sz );

    if( FD_UNLIKELY( !pb_encode_tag_for_field( stream, field ) ) ) return false;
    if( FD_UNLIKELY( !pb_encode_submessage( stream, &packet_Packet_msg, &packet ) ) ) return false;
  }

  return true;
}

static bool
encode_test_bundle_list( pb_ostream_t *     stream,
                         pb_field_t const * field,
                         void * const *     arg ) {
  test_bundle_list_t const * bundle_list = *arg;

  for( ulong i=0UL; i<bundle_list->bundle_cnt; i++ ) {
    test_bundle_desc_t const * desc = &bundle_list->bundles[ i ];
    test_packet_list_t packet_list = {
      .packets    = desc->packets,
      .packet_cnt = desc->packet_cnt,
    };
    bundle_BundleUuid bundle_uuid = bundle_BundleUuid_init_default;
    bundle_uuid.has_bundle = true;
    bundle_uuid.bundle.packets = (pb_callback_t) {
      .funcs.encode = encode_test_packet_list,
      .arg          = &packet_list,
    };
    FD_TEST( desc->uuid_sz<=sizeof(bundle_uuid.uuid.bytes) );
    bundle_uuid.uuid.size = (pb_size_t)desc->uuid_sz;
    fd_memcpy( bundle_uuid.uuid.bytes, desc->uuid, desc->uuid_sz );

    if( FD_UNLIKELY( !pb_encode_tag_for_field( stream, field ) ) ) return false;
    if( FD_UNLIKELY( !pb_encode_submessage( stream, &bundle_BundleUuid_msg, &bundle_uuid ) ) ) return false;
  }

  return true;
}

static ulong
encode_subscribe_packets_response( uchar const *          payload_buf,
                                   ulong                  payload_buf_sz,
                                   test_packet_desc_t *   packets,
                                   ulong                  packet_cnt ) {
  block_engine_SubscribePacketsResponse resp = block_engine_SubscribePacketsResponse_init_default;
  test_packet_list_t packet_list = {
    .packets    = packets,
    .packet_cnt = packet_cnt,
  };
  resp.has_batch = true;
  resp.batch.packets = (pb_callback_t) {
    .funcs.encode = encode_test_packet_list,
    .arg          = &packet_list,
  };

  pb_ostream_t ostream = pb_ostream_from_buffer( (pb_byte_t *)payload_buf, payload_buf_sz );
  FD_TEST( pb_encode( &ostream, &block_engine_SubscribePacketsResponse_msg, &resp ) );
  return ostream.bytes_written;
}

static ulong
encode_subscribe_bundles_response( uchar const *          payload_buf,
                                   ulong                  payload_buf_sz,
                                   test_bundle_desc_t *   bundles,
                                   ulong                  bundle_cnt ) {
  block_engine_SubscribeBundlesResponse resp = block_engine_SubscribeBundlesResponse_init_default;
  test_bundle_list_t bundle_list = {
    .bundles    = bundles,
    .bundle_cnt = bundle_cnt,
  };
  resp.bundles = (pb_callback_t) {
    .funcs.encode = encode_test_bundle_list,
    .arg          = &bundle_list,
  };

  pb_ostream_t ostream = pb_ostream_from_buffer( (pb_byte_t *)payload_buf, payload_buf_sz );
  FD_TEST( pb_encode( &ostream, &block_engine_SubscribeBundlesResponse_msg, &resp ) );
  return ostream.bytes_written;
}

static ulong
published_txn_cnt( test_bundle_env_t const * env ) {
  return env->stem_seqs[ 0 ];
}

static fd_txn_m_t const *
published_txn( test_bundle_env_t const *  env,
               ulong                      seq,
               fd_frag_meta_t const **    opt_meta ) {
  FD_TEST( seq<published_txn_cnt( env ) );
  fd_frag_meta_t const * meta = env->out_mcache + fd_mcache_line_idx( seq, env->stem_depths[ 0 ] );
  FD_TEST( meta->seq==seq );
  if( opt_meta ) *opt_meta = meta;
  return (fd_txn_m_t const *)fd_chunk_to_laddr( env->out_dcache, meta->chunk );
}

static void
expect_published_txn( test_bundle_env_t const * env,
                      ulong                     seq,
                      ulong                     sig,
                      uchar const *             payload,
                      ulong                     payload_sz,
                      ulong                     bundle_id,
                      ulong                     bundle_txn_cnt,
                      uchar                     commission,
                      uchar const *             commission_pubkey ) {
  fd_frag_meta_t const * meta = NULL;
  fd_txn_m_t const * txnm = published_txn( env, seq, &meta );

  FD_TEST( meta->sig==sig );
  FD_TEST( txnm->payload_sz==payload_sz );
  FD_TEST( txnm->txn_t_sz==0U );
  FD_TEST( txnm->source_tpu==FD_TXN_M_TPU_SOURCE_BUNDLE );
  FD_TEST( txnm->block_engine.bundle_id==bundle_id );
  FD_TEST( txnm->block_engine.bundle_txn_cnt==bundle_txn_cnt );
  FD_TEST( txnm->block_engine.commission==commission );
  FD_TEST( 0==memcmp( txnm->block_engine.commission_pubkey, commission_pubkey, 32UL ) );
  FD_TEST( 0==memcmp( fd_txn_m_payload_const( txnm ), payload, payload_sz ) );
}

static void
expect_published_harmonic_txn( test_bundle_env_t const * env,
                               ulong                     seq,
                               uchar const *             payload,
                               ulong                     payload_sz,
                               ulong                     block_slot,
                               ushort                    block_txn_cnt,
                               uchar                     commission,
                               uchar const *             commission_pubkey ) {
  fd_frag_meta_t const * meta = NULL;
  fd_txn_m_t const * txnm = published_txn( env, seq, &meta );

  FD_TEST( meta->sig==2UL );
  FD_TEST( txnm->payload_sz==payload_sz );
  FD_TEST( txnm->txn_t_sz==0U );
  FD_TEST( txnm->source_tpu==FD_TXN_M_TPU_SOURCE_HARMONIC );
  FD_TEST( txnm->block_engine.block_slot==block_slot );
  FD_TEST( txnm->block_engine.bundle_txn_cnt==block_txn_cnt );
  FD_TEST( txnm->block_engine.commission==commission );
  FD_TEST( 0==memcmp( txnm->block_engine.commission_pubkey, commission_pubkey, 32UL ) );
  FD_TEST( 0==memcmp( fd_txn_m_payload_const( txnm ), payload, payload_sz ) );
}

/* Mirror the production after_credit publish loop so tests can verify
   actual published output without exposing the static callback. */

static ulong
publish_after_credit( fd_bundle_tile_t * state ) {
  if( pending_txn_empty( state->pending_txns ) ) return 0UL;

  fd_stem_context_t * stem = state->stem;
  fd_bundle_pending_txn_t * head = pending_txn_peek_head( state->pending_txns );
  ulong drain_seq = head->bundle_seq;
  ulong drain_sig = head->sig;
  ulong drain_cnt = 0UL;

  do {
    fd_bundle_pending_txn_t const * txn = pending_txn_peek_head( state->pending_txns );

    fd_txn_m_t * txnm = fd_chunk_to_laddr( state->verify_out.mem, state->verify_out.chunk );
    *txnm = (fd_txn_m_t) {
      .reference_slot = 0UL,
      .payload_sz     = txn->payload_sz,
      .txn_t_sz       = 0U,
      .source_ipv4    = txn->source_ipv4,
      .source_tpu     = FD_TXN_M_TPU_SOURCE_BUNDLE,
      .block_engine   = {
        .bundle_id      = txn->bundle_seq,
        .bundle_txn_cnt = (ushort)txn->bundle_txn_cnt,
        .commission     = txn->commission,
      },
    };
    fd_memcpy( txnm->block_engine.commission_pubkey, txn->commission_pubkey, 32UL );
    fd_memcpy( fd_txn_m_payload( txnm ), txn->payload, txn->payload_sz );

    ulong sz    = fd_txn_m_realized_footprint( txnm, 0, 0 );
    ulong tspub = (ulong)fd_frag_meta_ts_comp( fd_bundle_now() );
    fd_stem_publish( stem, state->verify_out.idx, txn->sig, state->verify_out.chunk, sz, 0UL, 0UL, tspub );
    state->verify_out.chunk = fd_dcache_compact_next( state->verify_out.chunk, sz, state->verify_out.chunk0, state->verify_out.wmark );

    pending_txn_remove_head( state->pending_txns );
    drain_cnt++;
  } while( fd_bundle_drain_continue( state->pending_txns, drain_sig, drain_seq, drain_cnt, TEST_STEM_BURST ) );

  return drain_cnt;
}

/* Flush harmonic staging buffer by publishing to verify_out, mirroring
   the harmonic drain loop in after_credit. */

static ulong
flush_harmonic_staging( fd_bundle_tile_t * state ) {
  ulong n = state->harmonic_pending_len;
  if( !n ) return 0UL;

  fd_stem_context_t * stem = state->stem;
  for( ulong i=0UL; i<n; i++ ) {
    fd_bundle_harmonic_staged_txn_t const * s = &state->harmonic_staging[i];

    fd_txn_m_t * txnm = fd_chunk_to_laddr( state->verify_out.mem, state->verify_out.chunk );
    *txnm = (fd_txn_m_t) {
      .reference_slot = 0UL,
      .payload_sz     = s->payload_sz,
      .txn_t_sz       = 0U,
      .source_ipv4    = s->source_ipv4,
      .source_tpu     = FD_TXN_M_TPU_SOURCE_HARMONIC,
      .block_engine   = {
        .block_slot     = s->block_slot,
        .bundle_txn_cnt = s->block_txn_cnt,
        .commission     = s->commission,
      },
    };
    fd_memcpy( txnm->block_engine.commission_pubkey, s->commission_pubkey, 32UL );
    fd_memcpy( fd_txn_m_payload( txnm ), s->payload, s->payload_sz );

    ulong sz    = fd_txn_m_realized_footprint( txnm, 0, 0 );
    ulong tspub = (ulong)fd_frag_meta_ts_comp( fd_bundle_now() );
    fd_stem_publish( stem, state->verify_out.idx, 2UL, state->verify_out.chunk, sz, 0UL, 0UL, tspub );
    state->verify_out.chunk = fd_dcache_compact_next( state->verify_out.chunk, sz, state->verify_out.chunk0, state->verify_out.wmark );

    state->harmonic_block_txn_received_cnt++;
  }
  state->harmonic_pending_len = 0UL;
  return n;
}

/* Budget-limited version of flush_harmonic_staging.  Mirrors the
   production after_credit loop: drain at most `budget` txns, then
   memmove the remainder to the front of the staging array. */

static ulong
flush_harmonic_staging_budgeted( fd_bundle_tile_t * state, ulong budget ) {
  ulong n = fd_ulong_min( state->harmonic_pending_len, budget );
  if( !n ) return 0UL;

  fd_stem_context_t * stem = state->stem;
  for( ulong i=0UL; i<n; i++ ) {
    fd_bundle_harmonic_staged_txn_t const * s = &state->harmonic_staging[i];

    fd_txn_m_t * txnm = fd_chunk_to_laddr( state->verify_out.mem, state->verify_out.chunk );
    *txnm = (fd_txn_m_t) {
      .reference_slot = 0UL,
      .payload_sz     = s->payload_sz,
      .txn_t_sz       = 0U,
      .source_ipv4    = s->source_ipv4,
      .source_tpu     = FD_TXN_M_TPU_SOURCE_HARMONIC,
      .block_engine   = {
        .block_slot     = s->block_slot,
        .bundle_txn_cnt = s->block_txn_cnt,
        .commission     = s->commission,
      },
    };
    fd_memcpy( txnm->block_engine.commission_pubkey, s->commission_pubkey, 32UL );
    fd_memcpy( fd_txn_m_payload( txnm ), s->payload, s->payload_sz );

    ulong sz    = fd_txn_m_realized_footprint( txnm, 0, 0 );
    ulong tspub = (ulong)fd_frag_meta_ts_comp( fd_bundle_now() );
    fd_stem_publish( stem, state->verify_out.idx, 2UL, state->verify_out.chunk, sz, 0UL, 0UL, tspub );
    state->verify_out.chunk = fd_dcache_compact_next( state->verify_out.chunk, sz, state->verify_out.chunk0, state->verify_out.wmark );

    state->harmonic_block_txn_received_cnt++;
  }
  state->harmonic_pending_len -= n;
  if( state->harmonic_pending_len ) {
    memmove( state->harmonic_staging, state->harmonic_staging + n,
             state->harmonic_pending_len * sizeof(fd_bundle_harmonic_staged_txn_t) );
  }
  return n;
}

/* Simulate after_credit drain using the same continuation logic as
   production (fd_bundle_drain_continue). */

static ulong
drain_one_bundle( fd_bundle_pending_txn_t * deque ) {
  if( pending_txn_empty( deque ) ) return 0UL;

  fd_bundle_pending_txn_t * head = pending_txn_peek_head( deque );
  ulong drain_seq = head->bundle_seq;
  ulong drain_sig = head->sig;
  ulong cnt = 0UL;

  do {
    pending_txn_pop_head( deque );
    cnt++;
  } while( fd_bundle_drain_continue( deque, drain_sig, drain_seq, cnt, TEST_STEM_BURST ) );

  return cnt;
}

/* Verify that the drain logic publishes one complete bundle
   atomically, stopping at bundle boundaries. */

static void
test_packet_publish_after_credit( fd_wksp_t * wksp ) {
  test_bundle_env_t env[1];
  test_bundle_env_create( env, wksp );
  fd_bundle_tile_t * state = env->state;

  uchar const payload0[] = { 0x48 };
  uchar const payload1[] = { 0xAA, 0xBB };
  test_packet_desc_t packets[] = {
    { .payload=payload0, .payload_sz=sizeof(payload0) },
    { .payload=payload1, .payload_sz=sizeof(payload1) },
  };
  uchar pb_buf[ 256 ];
  ulong pb_sz = encode_subscribe_packets_response( pb_buf, sizeof(pb_buf), packets, 2UL );

  fd_bundle_client_grpc_rx_msg(
      state,
      pb_buf, pb_sz,
      FD_BUNDLE_CLIENT_REQ_Bundle_SubscribePackets
  );

  FD_TEST( pending_txn_cnt( state->pending_txns )==2UL );
  FD_TEST( published_txn_cnt( env )==0UL );

  uchar const zero_pubkey[ 32 ] = {0};
  FD_TEST( publish_after_credit( state )==2UL );
  FD_TEST( pending_txn_empty( state->pending_txns ) );
  FD_TEST( published_txn_cnt( env )==2UL );
  expect_published_txn( env, 0UL, 0UL, payload0, sizeof(payload0), 0UL, 1UL, 0U, zero_pubkey );
  expect_published_txn( env, 1UL, 0UL, payload1, sizeof(payload1), 0UL, 1UL, 0U, zero_pubkey );

  test_bundle_env_destroy( env );
}

/* Verify that bundles publish atomically: one full bundle per
   after_credit-style drain, never crossing into the next bundle. */

static void
test_packet_publish_after_credit_atomic( fd_wksp_t * wksp ) {
  test_bundle_env_t env[1];
  test_bundle_env_create( env, wksp );
  fd_bundle_tile_t * state = env->state;
  state->builder_info_avail  = 1;
  state->builder_commission  = 7U;
  uchar builder_pubkey[ 32 ];
  for( ulong i=0UL; i<32UL; i++ ) builder_pubkey[ i ] = (uchar)( i + 1U );
  fd_memcpy( state->builder_pubkey, builder_pubkey, sizeof(builder_pubkey) );

  uchar const bundle_a0[] = { 0xA0 };
  uchar const bundle_a1[] = { 0xA1 };
  uchar const bundle_a2[] = { 0xA2 };
  uchar const bundle_b0[] = { 0xB0 };
  uchar const bundle_b1[] = { 0xB1 };
  test_packet_desc_t bundle_a[] = {
    { .payload=bundle_a0, .payload_sz=sizeof(bundle_a0) },
    { .payload=bundle_a1, .payload_sz=sizeof(bundle_a1) },
    { .payload=bundle_a2, .payload_sz=sizeof(bundle_a2) },
  };
  test_packet_desc_t bundle_b[] = {
    { .payload=bundle_b0, .payload_sz=sizeof(bundle_b0) },
    { .payload=bundle_b1, .payload_sz=sizeof(bundle_b1) },
  };
  test_bundle_desc_t bundles[] = {
    { .packets=bundle_a, .packet_cnt=3UL, .uuid={1,2,3}, .uuid_sz=3UL },
    { .packets=bundle_b, .packet_cnt=2UL, .uuid={4,5,6}, .uuid_sz=3UL },
  };
  uchar pb_buf[ 512 ];
  ulong pb_sz = encode_subscribe_bundles_response( pb_buf, sizeof(pb_buf), bundles, 2UL );

  fd_bundle_client_grpc_rx_msg(
      state,
      pb_buf, pb_sz,
      FD_BUNDLE_CLIENT_REQ_Bundle_SubscribeBundles
  );

  FD_TEST( pending_txn_cnt( state->pending_txns )==5UL );
  FD_TEST( published_txn_cnt( env )==0UL );

  FD_TEST( publish_after_credit( state )==3UL );
  FD_TEST( pending_txn_cnt( state->pending_txns )==2UL );
  FD_TEST( pending_txn_peek_head( state->pending_txns )->bundle_seq==2UL );
  expect_published_txn( env, 0UL, 1UL, bundle_a0, sizeof(bundle_a0), 1UL, 3UL, 7U, builder_pubkey );
  expect_published_txn( env, 1UL, 1UL, bundle_a1, sizeof(bundle_a1), 1UL, 3UL, 7U, builder_pubkey );
  expect_published_txn( env, 2UL, 1UL, bundle_a2, sizeof(bundle_a2), 1UL, 3UL, 7U, builder_pubkey );

  FD_TEST( publish_after_credit( state )==2UL );
  FD_TEST( pending_txn_empty( state->pending_txns ) );
  FD_TEST( published_txn_cnt( env )==5UL );
  expect_published_txn( env, 3UL, 1UL, bundle_b0, sizeof(bundle_b0), 2UL, 2UL, 7U, builder_pubkey );
  expect_published_txn( env, 4UL, 1UL, bundle_b1, sizeof(bundle_b1), 2UL, 2UL, 7U, builder_pubkey );

  test_bundle_env_destroy( env );
}

/* Bundles are all-or-nothing at the head of the queue: after_credit
   must publish the entire bundle before touching following packets. */

static void
test_bundle_publish_all_or_nothing( fd_wksp_t * wksp ) {
  test_bundle_env_t env[1];
  test_bundle_env_create( env, wksp );
  fd_bundle_tile_t * state = env->state;
  state->builder_info_avail = 1;

  uchar const b0[] = { 0x10 };
  uchar const b1[] = { 0x11 };
  uchar const b2[] = { 0x12 };
  uchar const b3[] = { 0x13 };
  uchar const b4[] = { 0x14 };
  uchar const pkt[] = { 0x99 };
  test_packet_desc_t bundle_packets[] = {
    { .payload=b0, .payload_sz=sizeof(b0) },
    { .payload=b1, .payload_sz=sizeof(b1) },
    { .payload=b2, .payload_sz=sizeof(b2) },
    { .payload=b3, .payload_sz=sizeof(b3) },
    { .payload=b4, .payload_sz=sizeof(b4) },
  };
  test_bundle_desc_t bundles[] = {
    { .packets=bundle_packets, .packet_cnt=5UL, .uuid={7,7,7}, .uuid_sz=3UL },
  };
  test_packet_desc_t trailing_packet[] = {
    { .payload=pkt, .payload_sz=sizeof(pkt) },
  };
  uchar bundle_buf[ 512 ];
  uchar packet_buf[ 128 ];
  ulong bundle_sz = encode_subscribe_bundles_response( bundle_buf, sizeof(bundle_buf), bundles, 1UL );
  ulong packet_sz = encode_subscribe_packets_response( packet_buf, sizeof(packet_buf), trailing_packet, 1UL );

  fd_bundle_client_grpc_rx_msg(
      state,
      bundle_buf, bundle_sz,
      FD_BUNDLE_CLIENT_REQ_Bundle_SubscribeBundles
  );
  fd_bundle_client_grpc_rx_msg(
      state,
      packet_buf, packet_sz,
      FD_BUNDLE_CLIENT_REQ_Bundle_SubscribePackets
  );

  FD_TEST( pending_txn_cnt( state->pending_txns )==6UL );
  FD_TEST( publish_after_credit( state )==5UL );
  FD_TEST( published_txn_cnt( env )==5UL );
  FD_TEST( pending_txn_cnt( state->pending_txns )==1UL );
  FD_TEST( pending_txn_peek_head( state->pending_txns )->sig==0UL );

  uchar const zero_pubkey[ 32 ] = {0};
  expect_published_txn( env, 0UL, 1UL, b0, sizeof(b0), 1UL, 5UL, 0U, zero_pubkey );
  expect_published_txn( env, 1UL, 1UL, b1, sizeof(b1), 1UL, 5UL, 0U, zero_pubkey );
  expect_published_txn( env, 2UL, 1UL, b2, sizeof(b2), 1UL, 5UL, 0U, zero_pubkey );
  expect_published_txn( env, 3UL, 1UL, b3, sizeof(b3), 1UL, 5UL, 0U, zero_pubkey );
  expect_published_txn( env, 4UL, 1UL, b4, sizeof(b4), 1UL, 5UL, 0U, zero_pubkey );

  FD_TEST( publish_after_credit( state )==1UL );
  FD_TEST( pending_txn_empty( state->pending_txns ) );
  expect_published_txn( env, 5UL, 0UL, pkt, sizeof(pkt), 0UL, 1UL, 0U, zero_pubkey );

  test_bundle_env_destroy( env );
}

/* Verify queue boundary behavior with mixed packets and bundles. */

static void
test_mixed_queue_boundary_behavior( fd_wksp_t * wksp ) {
  test_bundle_env_t env[1];
  test_bundle_env_create( env, wksp );
  fd_bundle_tile_t * state = env->state;
  state->builder_info_avail = 1;

  uchar const p0[] = { 0x20 };
  uchar const p1[] = { 0x21 };
  uchar const p2[] = { 0x22 };
  uchar const p3[] = { 0x23 };
  uchar const b0[] = { 0x30 };
  uchar const b1[] = { 0x31 };
  uchar const p4[] = { 0x24 };
  uchar const p5[] = { 0x25 };
  test_packet_desc_t leading_packets[] = {
    { .payload=p0, .payload_sz=sizeof(p0) },
    { .payload=p1, .payload_sz=sizeof(p1) },
    { .payload=p2, .payload_sz=sizeof(p2) },
    { .payload=p3, .payload_sz=sizeof(p3) },
  };
  test_packet_desc_t bundle_packets[] = {
    { .payload=b0, .payload_sz=sizeof(b0) },
    { .payload=b1, .payload_sz=sizeof(b1) },
  };
  test_bundle_desc_t bundles[] = {
    { .packets=bundle_packets, .packet_cnt=2UL, .uuid={9,9,9}, .uuid_sz=3UL },
  };
  test_packet_desc_t trailing_packets[] = {
    { .payload=p4, .payload_sz=sizeof(p4) },
    { .payload=p5, .payload_sz=sizeof(p5) },
  };
  uchar packet_buf0[ 256 ];
  uchar bundle_buf [ 256 ];
  uchar packet_buf1[ 256 ];
  ulong packet_sz0 = encode_subscribe_packets_response( packet_buf0, sizeof(packet_buf0), leading_packets, 4UL );
  ulong bundle_sz  = encode_subscribe_bundles_response( bundle_buf, sizeof(bundle_buf), bundles, 1UL );
  ulong packet_sz1 = encode_subscribe_packets_response( packet_buf1, sizeof(packet_buf1), trailing_packets, 2UL );

  fd_bundle_client_grpc_rx_msg( state, packet_buf0, packet_sz0, FD_BUNDLE_CLIENT_REQ_Bundle_SubscribePackets );
  fd_bundle_client_grpc_rx_msg( state, bundle_buf,  bundle_sz,  FD_BUNDLE_CLIENT_REQ_Bundle_SubscribeBundles );
  fd_bundle_client_grpc_rx_msg( state, packet_buf1, packet_sz1, FD_BUNDLE_CLIENT_REQ_Bundle_SubscribePackets );

  uchar const zero_pubkey[ 32 ] = {0};
  FD_TEST( pending_txn_cnt( state->pending_txns )==8UL );

  FD_TEST( publish_after_credit( state )==4UL );
  FD_TEST( pending_txn_cnt( state->pending_txns )==4UL );
  expect_published_txn( env, 0UL, 0UL, p0, sizeof(p0), 0UL, 1UL, 0U, zero_pubkey );
  expect_published_txn( env, 1UL, 0UL, p1, sizeof(p1), 0UL, 1UL, 0U, zero_pubkey );
  expect_published_txn( env, 2UL, 0UL, p2, sizeof(p2), 0UL, 1UL, 0U, zero_pubkey );
  expect_published_txn( env, 3UL, 0UL, p3, sizeof(p3), 0UL, 1UL, 0U, zero_pubkey );

  FD_TEST( publish_after_credit( state )==2UL );
  FD_TEST( pending_txn_cnt( state->pending_txns )==2UL );
  expect_published_txn( env, 4UL, 1UL, b0, sizeof(b0), 1UL, 2UL, 0U, zero_pubkey );
  expect_published_txn( env, 5UL, 1UL, b1, sizeof(b1), 1UL, 2UL, 0U, zero_pubkey );

  FD_TEST( publish_after_credit( state )==2UL );
  FD_TEST( pending_txn_empty( state->pending_txns ) );
  expect_published_txn( env, 6UL, 0UL, p4, sizeof(p4), 0UL, 1UL, 0U, zero_pubkey );
  expect_published_txn( env, 7UL, 0UL, p5, sizeof(p5), 0UL, 1UL, 0U, zero_pubkey );

  test_bundle_env_destroy( env );
}

/* Messages are only buffered by grpc_rx_msg. Nothing should hit the
   output link until after_credit runs. */

static void
test_no_publish_before_after_credit( fd_wksp_t * wksp ) {
  test_bundle_env_t env[1];
  test_bundle_env_create( env, wksp );
  fd_bundle_tile_t * state = env->state;
  state->builder_info_avail = 1;

  uchar const packet_payload[] = { 0x55 };
  uchar const bundle_payload0[] = { 0x66 };
  uchar const bundle_payload1[] = { 0x67 };
  test_packet_desc_t packets[] = {
    { .payload=packet_payload, .payload_sz=sizeof(packet_payload) },
  };
  test_packet_desc_t bundle_packets[] = {
    { .payload=bundle_payload0, .payload_sz=sizeof(bundle_payload0) },
    { .payload=bundle_payload1, .payload_sz=sizeof(bundle_payload1) },
  };
  test_bundle_desc_t bundles[] = {
    { .packets=bundle_packets, .packet_cnt=2UL, .uuid={5,4,3}, .uuid_sz=3UL },
  };
  uchar packet_buf[ 128 ];
  uchar bundle_buf[ 256 ];
  ulong packet_sz = encode_subscribe_packets_response( packet_buf, sizeof(packet_buf), packets, 1UL );
  ulong bundle_sz = encode_subscribe_bundles_response( bundle_buf, sizeof(bundle_buf), bundles, 1UL );

  fd_bundle_client_grpc_rx_msg(
      state,
      packet_buf, packet_sz,
      FD_BUNDLE_CLIENT_REQ_Bundle_SubscribePackets
  );
  FD_TEST( pending_txn_cnt( state->pending_txns )==1UL );
  FD_TEST( published_txn_cnt( env )==0UL );

  fd_bundle_client_grpc_rx_msg(
      state,
      bundle_buf, bundle_sz,
      FD_BUNDLE_CLIENT_REQ_Bundle_SubscribeBundles
  );
  FD_TEST( pending_txn_cnt( state->pending_txns )==3UL );
  FD_TEST( published_txn_cnt( env )==0UL );

  test_bundle_env_destroy( env );
}

static void
test_bundle_drain_atomicity( fd_wksp_t * wksp ) {
  test_bundle_env_t env[1];
  test_bundle_env_create( env, wksp );
  fd_bundle_tile_t * state = env->state;
  state->builder_info_avail = 1;

  /* Push bundle A (3 txns, bundle_seq=1) */
  for( ulong i=0; i<3; i++ ) {
    fd_bundle_pending_txn_t entry = { .sig=1UL, .bundle_seq=1UL };
    pending_txn_push_tail( state->pending_txns, entry );
  }

  /* Push bundle B (2 txns, bundle_seq=2) */
  for( ulong i=0; i<2; i++ ) {
    fd_bundle_pending_txn_t entry = { .sig=1UL, .bundle_seq=2UL };
    pending_txn_push_tail( state->pending_txns, entry );
  }

  FD_TEST( pending_txn_cnt( state->pending_txns )==5UL );

  /* First drain: should pop exactly bundle A (3 txns) */
  ulong drained = drain_one_bundle( state->pending_txns );
  FD_TEST( drained==3UL );
  FD_TEST( pending_txn_cnt( state->pending_txns )==2UL );
  FD_TEST( pending_txn_peek_head( state->pending_txns )->bundle_seq==2UL );

  /* Second drain: should pop exactly bundle B (2 txns) */
  drained = drain_one_bundle( state->pending_txns );
  FD_TEST( drained==2UL );
  FD_TEST( pending_txn_empty( state->pending_txns ) );

  test_bundle_env_destroy( env );
}

/* Verify that individual packets drain up to STEM_BURST per call. */

static void
test_packet_drain_batch( fd_wksp_t * wksp ) {
  test_bundle_env_t env[1];
  test_bundle_env_create( env, wksp );
  fd_bundle_tile_t * state = env->state;

  /* Push 4 packets (< STEM_BURST) -- should drain in one call */
  for( ulong i=0; i<4; i++ ) {
    fd_bundle_pending_txn_t entry = { .sig=0UL, .bundle_seq=0UL };
    pending_txn_push_tail( state->pending_txns, entry );
  }
  FD_TEST( drain_one_bundle( state->pending_txns )==4UL );
  FD_TEST( pending_txn_empty( state->pending_txns ) );

  /* Push 8 packets (> STEM_BURST) -- should drain 5 then 3 */
  for( ulong i=0; i<8; i++ ) {
    fd_bundle_pending_txn_t entry = { .sig=0UL, .bundle_seq=0UL };
    pending_txn_push_tail( state->pending_txns, entry );
  }
  FD_TEST( drain_one_bundle( state->pending_txns )==TEST_STEM_BURST );
  FD_TEST( pending_txn_cnt( state->pending_txns )==3UL );
  FD_TEST( drain_one_bundle( state->pending_txns )==3UL );
  FD_TEST( pending_txn_empty( state->pending_txns ) );

  test_bundle_env_destroy( env );
}

/* Verify correct drain ordering when bundles and packets are
   interleaved in the deque. */

static void
test_interleaved_drain( fd_wksp_t * wksp ) {
  test_bundle_env_t env[1];
  test_bundle_env_create( env, wksp );
  fd_bundle_tile_t * state = env->state;
  state->builder_info_avail = 1;

  /* packet, bundle(3 txns), packet, packet */
  fd_bundle_pending_txn_t pkt = { .sig=0UL, .bundle_seq=0UL };
  pending_txn_push_tail( state->pending_txns, pkt );

  for( ulong i=0; i<3; i++ ) {
    fd_bundle_pending_txn_t b = { .sig=1UL, .bundle_seq=5UL };
    pending_txn_push_tail( state->pending_txns, b );
  }

  pending_txn_push_tail( state->pending_txns, pkt );
  pending_txn_push_tail( state->pending_txns, pkt );

  FD_TEST( pending_txn_cnt( state->pending_txns )==6UL );

  /* Drain 1: leading packet (1, stops because next entry is a bundle) */
  FD_TEST( drain_one_bundle( state->pending_txns )==1UL );
  FD_TEST( pending_txn_cnt( state->pending_txns )==5UL );

  /* Drain 2: bundle (3 txns, same bundle_seq) */
  FD_TEST( drain_one_bundle( state->pending_txns )==3UL );
  FD_TEST( pending_txn_cnt( state->pending_txns )==2UL );

  /* Drain 3: trailing 2 packets batched (2 < STEM_BURST) */
  FD_TEST( drain_one_bundle( state->pending_txns )==2UL );
  FD_TEST( pending_txn_empty( state->pending_txns ) );

  test_bundle_env_destroy( env );
}

/* Verify that the pending_txn_full guard fires and counts drops. */

static void
test_deque_overflow_guard( fd_wksp_t * wksp ) {
  test_bundle_env_t env[1];
  test_bundle_env_create( env, wksp );
  fd_bundle_tile_t * state = env->state;

  ulong cap = pending_txn_max( state->pending_txns );
  for( ulong i=0; i<cap; i++ ) {
    fd_bundle_pending_txn_t entry = {0};
    entry.sig = 0UL;
    pending_txn_push_tail( state->pending_txns, entry );
  }
  FD_TEST( pending_txn_full( state->pending_txns ) );
  FD_TEST( state->metrics.backpressure_drop_cnt==0UL );

  static uchar single_packet_msg[] = {
    0x12, 0x09, 0x0a, 0x07, 0x0a, 0x01, 0x48, 0x12,
    0x02, 0x08, 0x01
  };
  fd_bundle_client_grpc_rx_msg(
      state,
      single_packet_msg, sizeof(single_packet_msg),
      FD_BUNDLE_CLIENT_REQ_Bundle_SubscribePackets
  );

  FD_TEST( pending_txn_cnt( state->pending_txns )==cap );
  FD_TEST( state->metrics.backpressure_drop_cnt==1UL );

  test_bundle_env_destroy( env );
}

/* Verify that the client submits leader window info */

static void
test_bundle_client_submit_leader_window_info( fd_wksp_t * wksp ) {
  test_bundle_env_t env[1];
  test_bundle_env_create( env, wksp );
  test_bundle_env_mock_conn( env );
  fd_bundle_tile_t * const state       = env->state;
  fd_grpc_client_t * const grpc_client = state->grpc_client;

  FD_TEST( state->submit_leader_window_info_wait==0 );

  /* But it's blocked on stream count ... */
  FD_TEST( fd_grpc_client_request_is_blocked( state->grpc_client )==0 );
  FD_TEST( state->grpc_client->stream_cnt==2 );
  state->grpc_client->conn->peer_settings.max_concurrent_streams = 2;
  FD_TEST( fd_grpc_client_request_is_blocked( state->grpc_client )==1 );
  long const test_timestamp_ns = 1234567890123456789L;
  ulong const test_slot = 999999UL;
  fd_bundle_client_submit_leader_window_info( state, test_slot, test_timestamp_ns );
  FD_TEST( state->submit_leader_window_info_wait==0 );

  /* Unblock it ... */
  state->grpc_client->conn->peer_settings.max_concurrent_streams = 3;
  FD_TEST( fd_grpc_client_request_is_blocked( state->grpc_client )==0 );
  fd_bundle_client_submit_leader_window_info( state, test_slot, test_timestamp_ns );
  FD_TEST( state->submit_leader_window_info_wait==1 );

  /* Get newly created stream */
  FD_TEST( !grpc_client->request_stream ); /* request instantly flushed */
  ulong const stream_id = state->grpc_client->stream_ids[ 2 ];
  fd_grpc_h2_stream_t * stream = &state->grpc_client->stream_pool[ 2 ];
  FD_TEST( stream->s.stream_id==stream_id );
  FD_TEST( stream->request_ctx==FD_BUNDLE_CLIENT_REQ_SubmitLeaderWindowInfo );

  /* Request header */
  char const * const hdrs[] = {
    ":method",      "POST",
    ":scheme",      "https",
    ":path",        "/block_engine.BlockEngineValidator/SubmitLeaderWindowInfo",
    "te",           "trailers",
    "content-type", "application/grpc+proto",
    "user-agent",   "grpc-firedancer/0.0.0",
    NULL
  };
  expect_h2_hdr( grpc_client->frame_tx, stream_id, hdrs );

  /* Request body */
  fd_h2_frame_hdr_t frame_hdr;
  FD_TEST( fd_h2_rbuf_used_sz( grpc_client->frame_tx )>=sizeof(fd_h2_frame_hdr_t) );
  fd_h2_rbuf_pop_copy( grpc_client->frame_tx, &frame_hdr, sizeof(fd_h2_frame_hdr_t) );
  FD_TEST( fd_h2_frame_type( frame_hdr.typlen )==FD_H2_FRAME_TYPE_DATA );
  FD_TEST( fd_uint_bswap( frame_hdr.r_stream_id )==stream_id );
  FD_TEST( frame_hdr.flags==FD_H2_FLAG_END_STREAM );
  fd_grpc_hdr_t grpc_hdr;
  FD_TEST( fd_h2_rbuf_used_sz( grpc_client->frame_tx )>=sizeof(fd_grpc_hdr_t) );
  fd_h2_rbuf_pop_copy( grpc_client->frame_tx, &grpc_hdr, sizeof(fd_grpc_hdr_t) );
  FD_TEST( grpc_hdr.compressed==0 );
  FD_TEST( grpc_hdr.msg_sz>0 );

  /* Skip the protobuf message data */
  ulong frame_len = fd_h2_frame_length( frame_hdr.typlen );
  ulong remaining = frame_len - sizeof(fd_grpc_hdr_t);
  if( remaining > 0UL ) {
    FD_TEST( fd_h2_rbuf_used_sz( grpc_client->frame_tx ) >= remaining );
    fd_h2_rbuf_skip( grpc_client->frame_tx, remaining );
  }

  /* Inject a response */
  fd_bundle_client_grpc_rx_start( state, FD_BUNDLE_CLIENT_REQ_SubmitLeaderWindowInfo );

  /* Protobuf encoder util */
  uchar pb_buf[ 128 ];
  ulong pb_sz = 0UL;
  block_engine_SubmitLeaderWindowInfoResponse resp = block_engine_SubmitLeaderWindowInfoResponse_init_default;
#define ENCODE_MSG() do { \
    pb_ostream_t ostream = pb_ostream_from_buffer( pb_buf, sizeof(pb_buf) ); \
    FD_TEST( pb_encode( &ostream, &block_engine_SubmitLeaderWindowInfoResponse_msg, &resp ) ); \
    pb_sz = ostream.bytes_written; \
  } while(0)

  /* Valid response */
  ENCODE_MSG();
  fd_bundle_client_grpc_rx_msg( state, pb_buf, pb_sz, FD_BUNDLE_CLIENT_REQ_SubmitLeaderWindowInfo );
  FD_TEST( state->submit_leader_window_info_wait==1 );

  /* End stream */
  fd_grpc_resp_hdrs_t grpc_resp_hdrs = {
    .h2_status   = 200,
    .grpc_status = FD_GRPC_STATUS_OK
  };
  fd_bundle_client_grpc_rx_end( state, FD_BUNDLE_CLIENT_REQ_SubmitLeaderWindowInfo, &grpc_resp_hdrs );
  FD_TEST( state->submit_leader_window_info_wait==0 );

#undef ENCODE_MSG

  test_bundle_env_destroy( env );
}

int
main( int     argc,
      char ** argv ) {
  fd_boot( &argc, &argv );

  ulong cpu_idx = fd_tile_cpu_id( fd_tile_idx() );
  if( cpu_idx>fd_shmem_cpu_cnt() ) cpu_idx = 0UL;

  char const * _page_sz = fd_env_strip_cmdline_cstr ( &argc, &argv, "--page-sz",     NULL, "normal"                     );
  ulong        page_cnt = fd_env_strip_cmdline_ulong( &argc, &argv, "--page-cnt",    NULL, 256UL                        );
  ulong        numa_idx = fd_env_strip_cmdline_ulong( &argc, &argv, "--numa-idx",    NULL, fd_shmem_numa_idx( cpu_idx ) );

  fd_wksp_t * wksp = fd_wksp_new_anonymous( fd_cstr_to_shmem_page_sz( _page_sz ), page_cnt, fd_shmem_cpu_idx( numa_idx ), "wksp", 16UL );
  FD_TEST( wksp );

  /* Initialize block test messages */
  init_block_test_messages();

  test_bundle_rx( wksp );
  test_bundle_rx_too_many_txns( wksp );
  test_bundle_stream_ended( wksp );
  test_bundle_stream_reset( wksp );
  test_bundle_header_timeout( wksp );
  test_bundle_rx_end_timeout( wksp );
  test_bundle_ping( wksp );
  test_bundle_msg_oversized( wksp );
  test_bundle_keyswitch( wksp );
  test_bundle_client_status( wksp );
  test_bundle_client_reset( wksp );
  test_bundle_no_builder_fee_info( wksp );
  test_bundle_client_request_builder_fee_info( wksp );
  test_bundle_client_submit_leader_window_info( wksp );
  test_bundle_client_subscribe_packets( wksp );
  test_bundle_client_subscribe_bundles( wksp );

  /* Harmonic block mode tests */
  test_harmonic_block_slot_parsing( wksp );
  test_harmonic_block_rx_many_txns( wksp );
  /* test_harmonic_block_oversized( wksp ); -- staging-based flow caps a
     single rx_msg at harmonic_staging_max txns; production server caps
     a single stage at 32 anyway, so this test is no longer relevant. */
  test_all_three_sources( wksp );
  test_interleaved_sources( wksp );

  test_packet_publish_after_credit( wksp );
  test_packet_publish_after_credit_atomic( wksp );
  test_bundle_publish_all_or_nothing( wksp );
  test_mixed_queue_boundary_behavior( wksp );
  test_no_publish_before_after_credit( wksp );
  test_bundle_drain_atomicity( wksp );
  test_packet_drain_batch( wksp );
  test_interleaved_drain( wksp );
  test_deque_overflow_guard( wksp );
  test_harmonic_staging_partial_drain( wksp );
  test_harmonic_before_credit_gate( wksp );
  test_reset_clears_harmonic_state( wksp );

  /* Check for memory leaks */
  fd_wksp_usage_t wksp_usage;
  FD_TEST( fd_wksp_usage( wksp, NULL, 0UL, &wksp_usage ) );
  FD_TEST( wksp_usage.free_cnt==wksp_usage.total_cnt );

  fd_wksp_delete_anonymous( wksp );

  FD_LOG_NOTICE(( "pass" ));
  fd_halt();
  return 0;
}
