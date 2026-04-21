/* fd_bundle_client.c steps gRPC related tasks. */

#define _GNU_SOURCE /* SOL_TCP */
#include "fd_bundle_auth.h"
#include "fd_bundle_tile_private.h"
#include "fd_bundle_tile.h"
#include "proto/block_engine.pb.h"
#include "proto/bundle.pb.h"
#include "proto/packet.pb.h"
#include "proto/tpu.pb.h"
#include "../fd_txn_m.h"
#include "../../waltz/h2/fd_h2_conn.h"
#include "../../waltz/http/fd_url.h" /* fd_url_unescape */
#include "../../waltz/openssl/fd_openssl.h"
#include "../../ballet/base58/fd_base58.h"
#include "../../ballet/nanopb/pb_decode.h"
#include "../../util/net/fd_ip4.h"

#include <fcntl.h>
#include <errno.h>
#include <unistd.h> /* close */
#include <poll.h> /* poll */
#include <sys/socket.h> /* socket */
#include <netinet/in.h>
#include <netinet/ip.h>
#include <netinet/tcp.h>

#define FD_BUNDLE_CLIENT_REQUEST_TIMEOUT ((long)8e9) /* 8 seconds */


/* Forward declarations for harmonic block handlers */
static void fd_bundle_client_handle_block_batch( fd_bundle_tile_t * ctx, pb_istream_t * istream );

__attribute__((weak)) long
fd_bundle_now( void ) {
  return fd_log_wallclock();
}

void
fd_bundle_client_reset( fd_bundle_tile_t * ctx ) {
  if( FD_UNLIKELY( ctx->tcp_sock >= 0 ) ) {
    if( FD_UNLIKELY( 0!=close( ctx->tcp_sock ) ) ) {
      FD_LOG_ERR(( "close(tcp_sock=%i) failed (%i-%s)", ctx->tcp_sock, errno, fd_io_strerror( errno ) ));
    }
    ctx->tcp_sock = -1;
    ctx->tcp_sock_connected = 0;
  }
  ctx->defer_reset = 0;

  ctx->builder_info_avail       = 0;
  ctx->builder_info_wait        = 0;
  ctx->submit_leader_window_info_wait = 0;
  ctx->packet_subscription_live = 0;
  ctx->packet_subscription_wait = 0;
  ctx->bundle_subscription_live = 0;
  ctx->bundle_subscription_wait = 0;
  ctx->harmonic_block_subscription_live = 0;
  ctx->harmonic_block_subscription_wait = 0;

  fd_memset( ctx->rtt, 0, sizeof(fd_rtt_estimate_t) );

# if FD_HAS_OPENSSL
  if( FD_UNLIKELY( ctx->ssl ) ) {
    SSL_free( ctx->ssl );
    ctx->ssl = NULL;
  }
# endif

  fd_bundle_tile_backoff( ctx, fd_bundle_now() );

  fd_bundle_auther_reset( &ctx->auther );
  fd_grpc_client_reset( ctx->grpc_client );
}

static int
fd_bundle_client_do_connect( fd_bundle_tile_t const * ctx,
                             uint                     ip4_addr ) {
  struct sockaddr_in addr = {
    .sin_family      = AF_INET,
    .sin_addr.s_addr = ip4_addr,
    .sin_port        = fd_ushort_bswap( ctx->server_tcp_port )
  };
  int err = connect( ctx->tcp_sock, fd_type_pun_const( &addr ), sizeof(struct sockaddr_in) );
  /* FD_LIKELY is used here as EINPROGRESS is expected even to local tcp ports */
  if( FD_LIKELY( err==-1 ) ) {
    return errno;
  }
  return 0;
}

static int
fd_bundle_client_get_connect_result( fd_bundle_tile_t const * ctx ) {
  int so_err = 0;
  socklen_t so_err_sz = sizeof(so_err);
  if( FD_UNLIKELY( getsockopt( ctx->tcp_sock, SOL_SOCKET, SO_ERROR, &so_err, &so_err_sz )==-1 ) ) {
    return errno;
  }
  return so_err;
}

static void
fd_bundle_client_create_conn( fd_bundle_tile_t * ctx ) {
  fd_bundle_client_reset( ctx );

  /* FIXME IPv6 support */
  fd_addrinfo_t hints = {0};
  hints.ai_family = AF_INET;
  fd_addrinfo_t * res = NULL;
  uchar scratch[ 4096 ];
  void * pscratch = scratch;
  int err = fd_getaddrinfo( ctx->server_fqdn, &hints, &res, &pscratch, sizeof(scratch) );
  if( FD_UNLIKELY( err ) ) {
    FD_LOG_WARNING(( "fd_getaddrinfo `%s` failed (%d-%s)", ctx->server_fqdn, err, fd_gai_strerror( err ) ));
    fd_bundle_client_reset( ctx );
    ctx->metrics.transport_fail_cnt++;
    return;
  }
  uint const ip4_addr = ((struct sockaddr_in *)res->ai_addr)->sin_addr.s_addr;
  ctx->server_ip4_addr = ip4_addr;

  int tcp_sock = socket( AF_INET, SOCK_STREAM|SOCK_CLOEXEC, 0 );
  if( FD_UNLIKELY( tcp_sock<0 ) ) {
    FD_LOG_ERR(( "socket(AF_INET,SOCK_STREAM|SOCK_CLOEXEC,0) failed (%i-%s)", errno, fd_io_strerror( errno ) ));
  }
  ctx->tcp_sock = tcp_sock;

  if( FD_UNLIKELY( 0!=setsockopt( tcp_sock, SOL_SOCKET, SO_RCVBUF, &ctx->so_rcvbuf, sizeof(int) ) ) ) {
    FD_LOG_ERR(( "setsockopt(SOL_SOCKET,SO_RCVBUF,%i) failed (%i-%s)", ctx->so_rcvbuf, errno, fd_io_strerror( errno ) ));
  }

  int tcp_nodelay = 1;
  if( FD_UNLIKELY( 0!=setsockopt( tcp_sock, SOL_TCP, TCP_NODELAY, &tcp_nodelay, sizeof(int) ) ) ) {
    FD_LOG_ERR(( "setsockopt failed (%d-%s)", errno, fd_io_strerror( errno ) ));
  }

  if( FD_UNLIKELY( fcntl( tcp_sock, F_SETFL, O_NONBLOCK )==-1 ) ) {
    FD_LOG_ERR(( "fcntl(tcp_sock,F_SETFL,O_NONBLOCK) failed (%i-%s)", errno, fd_io_strerror( errno ) ));
  }

  char const * scheme = "http";
# if FD_HAS_OPENSSL
  if( ctx->is_ssl ) scheme = "https";
# endif

  FD_LOG_INFO(( "Connecting to %s://" FD_IP4_ADDR_FMT ":%hu (%.*s)",
                scheme,
                FD_IP4_ADDR_FMT_ARGS( ip4_addr ), ctx->server_tcp_port,
                (int)ctx->server_sni_len, ctx->server_sni ));

  int connect_err = fd_bundle_client_do_connect( ctx, ip4_addr );
  /* FD_LIKELY as EINPROGRESS is expected */
  if( FD_LIKELY( connect_err ) ) {
    if( FD_UNLIKELY( connect_err!=EINPROGRESS ) ) {
      FD_LOG_WARNING(( "connect(tcp_sock," FD_IP4_ADDR_FMT ":%u) failed (%i-%s)",
                      FD_IP4_ADDR_FMT_ARGS( ip4_addr ), ctx->server_tcp_port,
                      connect_err, fd_io_strerror( connect_err ) ));
      fd_bundle_client_reset( ctx );
      ctx->metrics.transport_fail_cnt++;
      return;
    }
  }

# if FD_HAS_OPENSSL
  if( ctx->is_ssl ) {
    BIO * bio = fd_openssl_bio_new_socket( ctx->tcp_sock, BIO_NOCLOSE );
    if( FD_UNLIKELY( !bio ) ) {
      FD_LOG_ERR(( "fd_openssl_bio_new_socket failed" ));
    }

    SSL * ssl = SSL_new( ctx->ssl_ctx );
    if( FD_UNLIKELY( !ssl ) ) {
      FD_LOG_ERR(( "SSL_new failed" ));
    }

    SSL_set_bio( ssl, bio, bio ); /* moves ownership of bio */
    SSL_set_connect_state( ssl );

    /* Indicate to endpoint which server name we want */
    if( FD_UNLIKELY( !SSL_set_tlsext_host_name( ssl, ctx->server_sni ) ) ) {
      FD_LOG_ERR(( "SSL_set_tlsext_host_name failed" ));
    }

    /* Enable hostname verification */
    if( FD_UNLIKELY( !SSL_set1_host( ssl, ctx->server_sni ) ) ) {
      FD_LOG_ERR(( "SSL_set1_host failed" ));
    }

    ctx->ssl = ssl;
  }
# endif /* FD_HAS_OPENSSL */

  fd_grpc_client_reset( ctx->grpc_client );
  fd_keepalive_init( ctx->keepalive, ctx->rng, ctx->keepalive_interval, ctx->keepalive_interval, fd_bundle_now() );
}

static int
fd_bundle_client_drive_io( fd_bundle_tile_t * ctx,
                           int *              charge_busy ) {
# if FD_HAS_OPENSSL
  if( ctx->is_ssl ) {
    return fd_grpc_client_rxtx_ossl( ctx->grpc_client, ctx->ssl, charge_busy );
  }
# endif /* FD_HAS_OPENSSL */

  return fd_grpc_client_rxtx_socket( ctx->grpc_client, ctx->tcp_sock, charge_busy );
}

static void
fd_bundle_client_request_builder_info( fd_bundle_tile_t * ctx ) {
  if( FD_UNLIKELY( fd_grpc_client_request_is_blocked( ctx->grpc_client ) ) ) return;

  block_engine_BlockBuilderFeeInfoRequest req = block_engine_BlockBuilderFeeInfoRequest_init_default;
  static char const path[] = "/block_engine.BlockEngineValidator/GetBlockBuilderFeeInfo";
  fd_grpc_h2_stream_t * request = fd_grpc_client_request_start(
      ctx->grpc_client,
      path, sizeof(path)-1,
      FD_BUNDLE_CLIENT_REQ_Bundle_GetBlockBuilderFeeInfo,
      &block_engine_BlockBuilderFeeInfoRequest_msg, &req,
      ctx->auther.access_token, ctx->auther.access_token_sz,
      0 /* is_streaming */
  );
  if( FD_UNLIKELY( !request ) ) return;
  fd_grpc_client_deadline_set(
      request,
      FD_GRPC_DEADLINE_RX_END,
      fd_log_wallclock() + FD_BUNDLE_CLIENT_REQUEST_TIMEOUT );

  ctx->builder_info_wait = 1;
}

static void
fd_bundle_client_subscribe_packets( fd_bundle_tile_t * ctx ) {
  if( FD_UNLIKELY( fd_grpc_client_request_is_blocked( ctx->grpc_client ) ) ) return;

  block_engine_SubscribePacketsRequest req = block_engine_SubscribePacketsRequest_init_default;
  static char const path[] = "/block_engine.BlockEngineValidator/SubscribePackets";
  fd_grpc_h2_stream_t * request = fd_grpc_client_request_start(
      ctx->grpc_client,
      path, sizeof(path)-1,
      FD_BUNDLE_CLIENT_REQ_Bundle_SubscribePackets,
      &block_engine_SubscribePacketsRequest_msg, &req,
      ctx->auther.access_token, ctx->auther.access_token_sz,
      0 /* is_streaming */
  );
  if( FD_UNLIKELY( !request ) ) return;
  fd_grpc_client_deadline_set(
      request,
      FD_GRPC_DEADLINE_HEADER,
      fd_log_wallclock() + FD_BUNDLE_CLIENT_REQUEST_TIMEOUT );

  ctx->packet_subscription_wait = 1;
}

static void
fd_bundle_client_subscribe_bundles( fd_bundle_tile_t * ctx ) {
  if( FD_UNLIKELY( fd_grpc_client_request_is_blocked( ctx->grpc_client ) ) ) return;

  block_engine_SubscribeBundlesRequest req = block_engine_SubscribeBundlesRequest_init_default;

  /* When harmonic_block_mode is enabled, use SubscribeBundles2.
     TODO: revert once all clients migrate */
  static char const path[]  = "/block_engine.BlockEngineValidator/SubscribeBundles";
  static char const path2[] = "/block_engine.BlockEngineValidator/SubscribeBundles2";
  char const * use_path = ctx->harmonic_block_mode ? path2 : path;
  ulong        use_len  = ctx->harmonic_block_mode ? sizeof(path2)-1 : sizeof(path)-1;

  fd_grpc_h2_stream_t * request = fd_grpc_client_request_start(
      ctx->grpc_client,
      use_path, use_len,
      FD_BUNDLE_CLIENT_REQ_Bundle_SubscribeBundles,
      &block_engine_SubscribeBundlesRequest_msg, &req,
      ctx->auther.access_token, ctx->auther.access_token_sz,
      0 /* is_streaming */
  );
  if( FD_UNLIKELY( !request ) ) return;
  fd_grpc_client_deadline_set(
      request,
      FD_GRPC_DEADLINE_HEADER,
      fd_log_wallclock() + FD_BUNDLE_CLIENT_REQUEST_TIMEOUT );

  ctx->bundle_subscription_wait = 1;
}

/* Subscribe to harmonic blocks (third stream on same connection).
   Uses the same proto as SubscribeBundles but with different semantics:
   - slot:ulong instead of bundle_id:string (sent as decimal string in uuid field)
   - no len=5 limit on transactions */
static void
fd_bundle_client_subscribe_blocks( fd_bundle_tile_t * ctx ) {
  if( FD_UNLIKELY( fd_grpc_client_request_is_blocked( ctx->grpc_client ) ) ) return;

  block_engine_SubscribeBundlesRequest req = block_engine_SubscribeBundlesRequest_init_default;
  static char const path[] = "/block_engine.BlockEngineValidator/SubscribeBlocks";
  fd_grpc_h2_stream_t * request = fd_grpc_client_request_start(
      ctx->grpc_client,
      path, sizeof(path)-1,
      FD_BUNDLE_CLIENT_REQ_SubscribeBlocks,
      &block_engine_SubscribeBundlesRequest_msg, &req,
      ctx->auther.access_token, ctx->auther.access_token_sz,
      0 /* is_streaming */
  );
  if( FD_UNLIKELY( !request ) ) return;
  fd_grpc_client_deadline_set(
      request,
      FD_GRPC_DEADLINE_HEADER,
      fd_log_wallclock() + FD_BUNDLE_CLIENT_REQUEST_TIMEOUT );

  ctx->harmonic_block_subscription_wait = 1;
}

void
fd_bundle_client_send_ping( fd_bundle_tile_t * ctx ) {
  if( FD_UNLIKELY( !ctx->grpc_client ) ) return; /* no client */
  fd_h2_conn_t * conn = fd_grpc_client_h2_conn( ctx->grpc_client );
  if( FD_UNLIKELY( !conn ) ) return; /* no conn */
  if( FD_UNLIKELY( conn->flags ) ) return; /* conn busy */
  fd_h2_rbuf_t * rbuf_tx = fd_grpc_client_rbuf_tx( ctx->grpc_client );

  if( FD_LIKELY( fd_h2_tx_ping( conn, rbuf_tx ) ) ) {
    long now = fd_bundle_now();
    fd_keepalive_tx( ctx->keepalive, ctx->rng, now );
    FD_LOG_DEBUG(( "Keepalive TX (deadline=+%gs)", (double)( ctx->keepalive->ts_deadline-now )/1e9 ));
  }
}

void
fd_bundle_client_submit_leader_window_info( fd_bundle_tile_t * ctx,
                                            ulong              slot,
                                            long               start_timestamp_ns ) {
  if( FD_UNLIKELY( !ctx->grpc_client ) ) return; /* no client */
  if( FD_UNLIKELY( fd_grpc_client_request_is_blocked( ctx->grpc_client ) ) ) return;

  block_engine_SubmitLeaderWindowInfoRequest req = block_engine_SubmitLeaderWindowInfoRequest_init_default;
  req.slot = slot;
  req.has_start_timestamp = 1;
  req.start_timestamp.seconds = start_timestamp_ns / (long)1e9;
  req.start_timestamp.nanos   = (int32_t)( start_timestamp_ns % (long)1e9 );

  static char const path[] = "/block_engine.BlockEngineValidator/SubmitLeaderWindowInfo";
  fd_grpc_h2_stream_t * request = fd_grpc_client_request_start(
      ctx->grpc_client,
      path, sizeof(path)-1,
      FD_BUNDLE_CLIENT_REQ_SubmitLeaderWindowInfo,
      &block_engine_SubmitLeaderWindowInfoRequest_msg, &req,
      ctx->auther.access_token, ctx->auther.access_token_sz,
      0 /* is_streaming */
  );
  if( FD_UNLIKELY( !request ) ) return;
  fd_grpc_client_deadline_set(
      request,
      FD_GRPC_DEADLINE_RX_END,
      fd_log_wallclock() + FD_BUNDLE_CLIENT_REQUEST_TIMEOUT );

  ctx->submit_leader_window_info_wait = 1;
}

int
fd_bundle_client_step_reconnect( fd_bundle_tile_t * ctx,
                                 long               now ) {
  /* Drive auth */
  if( FD_UNLIKELY( ctx->auther.needs_poll ) ) {
    fd_bundle_auther_poll( &ctx->auther, ctx->grpc_client, ctx->keyguard_client );
    return 1;
  }
  if( FD_UNLIKELY( ctx->auther.state!=FD_BUNDLE_AUTH_STATE_DONE_WAIT ) ) return 0;

  /* Request block builder info */
  int const builder_info_expired = ( ctx->builder_info_valid_until - now )<0;
  if( FD_UNLIKELY( ( ( !ctx->builder_info_avail ) |
                     ( builder_info_expired     ) ) &
                   ( !ctx->builder_info_wait      ) ) ) {
    fd_bundle_client_request_builder_info( ctx );
    return 1;
  }

  /* Subscribe to packets */
  if( FD_UNLIKELY( !ctx->packet_subscription_live && !ctx->packet_subscription_wait ) ) {
    fd_bundle_client_subscribe_packets( ctx );
    return 1;
  }

  /* Subscribe to bundles */
  if( FD_UNLIKELY( !ctx->bundle_subscription_live && !ctx->bundle_subscription_wait ) ) {
    fd_bundle_client_subscribe_bundles( ctx );
    return 1;
  }

  /* Subscribe to blocks (harmonic block mode) */
  if( FD_UNLIKELY( ctx->harmonic_block_mode &&
                   !ctx->harmonic_block_subscription_live &&
                   !ctx->harmonic_block_subscription_wait ) ) {
    fd_bundle_client_subscribe_blocks( ctx );
    return 1;
  }

  /* Send a PING */
  if( FD_UNLIKELY( fd_keepalive_should_tx( ctx->keepalive, now ) ) ) {
    fd_bundle_client_send_ping( ctx );
    return 1;
  }

  return 0;
}

static void
fd_bundle_client_step1( fd_bundle_tile_t * ctx,
                        int *              charge_busy ) {

  /* Wait for TCP socket to connect */
  if( FD_UNLIKELY( !ctx->tcp_sock_connected ) ) {
    if( FD_UNLIKELY( ctx->tcp_sock < 0 ) ) goto reconnect;

    struct pollfd pfds[1] = {
      { .fd = ctx->tcp_sock, .events = POLLOUT }
    };
    int poll_res = fd_syscall_poll( pfds, 1, 0 );
    if( FD_UNLIKELY( poll_res<0 ) ) {
      FD_LOG_ERR(( "fd_syscall_poll(tcp_sock) failed (%i-%s)", errno, fd_io_strerror( errno ) ));
    }
    if( poll_res==0 ) return;

    int connect_result = 0;
    if( pfds[0].revents & (POLLERR|POLLHUP) ) {
      connect_result = fd_bundle_client_get_connect_result( ctx );
    connect_failed:
      FD_LOG_INFO(( "Bundle gRPC connect attempt failed (%i-%s)", connect_result, fd_io_strerror( connect_result ) ));
      fd_bundle_client_reset( ctx );
      ctx->metrics.transport_fail_cnt++;
      *charge_busy = 1;
      return;
    }
    if( pfds[0].revents & POLLOUT ) {
      connect_result = fd_bundle_client_get_connect_result( ctx );
      if( FD_UNLIKELY( connect_result!=0 ) ) {
        goto connect_failed;
      }
      FD_LOG_DEBUG(( "Bundle TCP socket connected" ));
      ctx->tcp_sock_connected = 1;
      *charge_busy = 1;
      return;
    }
    return;
  }

  /* gRPC conn died? */
  if( FD_UNLIKELY( !ctx->grpc_client ) ) {
    long sleep_start;
  reconnect:
    sleep_start = fd_bundle_now();
    if( FD_UNLIKELY( fd_bundle_tile_should_stall( ctx, sleep_start ) ) ) {
      long wait_dur = ctx->backoff_until - sleep_start;
      fd_log_sleep( fd_long_min( wait_dur, 1e6 ) );
      return;
    }
    fd_bundle_client_create_conn( ctx );
    *charge_busy = 1;
    return;
  }

  /* Did a HTTP/2 PING time out */
  long check_ts = ctx->cached_ts = fd_bundle_now();
  if( FD_UNLIKELY( fd_keepalive_is_timeout( ctx->keepalive, check_ts ) ) ) {
    FD_LOG_WARNING(( "Bundle gRPC timed out (HTTP/2 PING went unanswered for %.2f seconds)",
                     (double)( check_ts - ctx->keepalive->ts_last_tx )/1e9 ));
    ctx->keepalive->inflight = 0;
    ctx->defer_reset = 1;
    *charge_busy = 1;
    return;
  }

  /* Drive I/O, SSL handshake, and any inflight requests */
  if( FD_UNLIKELY( -1==fd_bundle_client_drive_io( ctx, charge_busy ) || ctx->defer_reset /* new error? */ ) ) {
    fd_bundle_client_reset( ctx );
    ctx->metrics.transport_fail_cnt++;
    *charge_busy = 1;
    return;
  }

  /* Are we ready to issue a new request? */
  if( FD_UNLIKELY( fd_grpc_client_request_is_blocked( ctx->grpc_client ) ) ) return;
  long io_ts = fd_bundle_now();
  if( FD_UNLIKELY( fd_bundle_tile_should_stall( ctx, io_ts ) ) ) return;

  *charge_busy |= fd_bundle_client_step_reconnect( ctx, io_ts );
}

static void
fd_bundle_client_log_status( fd_bundle_tile_t * ctx ) {
  int status = fd_bundle_client_status( ctx );

  int const connected_now    = ( status==FD_BUNDLE_BLOCK_ENGINE_STATUS_CONNECTED );
  int const connected_before = ( ctx->bundle_status_logged==FD_BUNDLE_BLOCK_ENGINE_STATUS_CONNECTED );

  if( FD_UNLIKELY( connected_now!=connected_before ) ) {
    long ts = fd_log_wallclock();
    if( FD_LIKELY( ts-(ctx->last_bundle_status_log_nanos) >= (long)1e6 ) ) {
      if( connected_now ) {
        FD_LOG_NOTICE(( "Connected to bundle server" ));
      } else {
        FD_LOG_WARNING(( "Disconnected from bundle server" ));
      }
      ctx->last_bundle_status_log_nanos = ts;
      ctx->bundle_status_logged = (uchar)status;
    }
  }
}

void
fd_bundle_client_step( fd_bundle_tile_t * ctx,
                       int *              charge_busy ) {
  /* Edge-trigger logging with rate limiting */
  fd_bundle_client_step1( ctx, charge_busy );
  fd_bundle_client_log_status( ctx );
}

void
fd_bundle_tile_backoff( fd_bundle_tile_t * ctx,
                        long               now ) {
  uint iter = ctx->backoff_iter;
  if( now < ctx->backoff_reset ) iter = 0U;
  iter++;

  /* FIXME proper backoff */
  long wait_ns = (long)2e9;
  wait_ns = (long)( fd_rng_ulong( ctx->rng ) & ( (1UL<<fd_ulong_find_msb_w_default( (ulong)wait_ns, 0 ))-1UL ) );

  ctx->backoff_until = now +   wait_ns;
  ctx->backoff_reset = now + 2*wait_ns;

  ctx->backoff_iter = iter;
}

static void
fd_bundle_client_grpc_conn_established( void * app_ctx ) {
  (void)app_ctx;
  FD_LOG_INFO(( "Bundle gRPC connection established" ));
}

static void
fd_bundle_client_grpc_conn_dead( void * app_ctx,
                                 uint   h2_err,
                                 int    closed_by ) {
  fd_bundle_tile_t * ctx = app_ctx;
  FD_LOG_INFO(( "Bundle gRPC connection closed %s (%u-%s)",
                closed_by ? "by peer" : "due to error",
                h2_err, fd_h2_strerror( h2_err ) ));
  ctx->defer_reset = 1;
}

/* Buffers a bundle transaction for deferred publishing by after_credit. */

static void
fd_bundle_tile_publish_bundle_txn(
    fd_bundle_tile_t * ctx,
    void const *       txn,
    ulong              txn_sz,  /* <=FD_TXN_MTU */
    ulong              bundle_txn_cnt,
    uint               source_ipv4
) {
  if( FD_UNLIKELY( !ctx->builder_info_avail ) ) {
    ctx->metrics.missing_builder_info_fail_cnt++; /* unreachable */
    return;
  }

  if( FD_UNLIKELY( pending_txn_full( ctx->pending_txns ) ) ) {
    ctx->metrics.backpressure_drop_cnt++;
    return;
  }

  fd_bundle_pending_txn_t * entry = pending_txn_push_tail_nocopy( ctx->pending_txns );
  fd_memcpy( entry->payload, txn, txn_sz );
  entry->payload_sz     = (ushort)txn_sz;
  entry->source_ipv4    = source_ipv4;
  entry->source_tpu     = FD_TXN_M_TPU_SOURCE_BUNDLE;
  entry->sig            = 1UL;
  entry->bundle_seq     = ctx->bundle_seq;
  entry->bundle_txn_cnt = bundle_txn_cnt;
  entry->commission     = (uchar)ctx->builder_commission;
  fd_memcpy( entry->commission_pubkey, ctx->builder_pubkey, 32UL );
  ctx->metrics.txn_received_cnt++;
}

/* Buffers a regular transaction for deferred publishing by after_credit. */

static void
fd_bundle_tile_publish_txn(
    fd_bundle_tile_t * ctx,
    void const *       txn,
    ulong              txn_sz,  /* <=FD_TXN_MTU */
    uint               source_ipv4,
    uchar              source_tpu
) {
  if( FD_UNLIKELY( pending_txn_full( ctx->pending_txns ) ) ) {
    ctx->metrics.backpressure_drop_cnt++;
    return;
  }

  fd_bundle_pending_txn_t * entry = pending_txn_push_tail_nocopy( ctx->pending_txns );
  fd_memcpy( entry->payload, txn, txn_sz );
  entry->payload_sz     = (ushort)txn_sz;
  entry->source_ipv4    = source_ipv4;
  entry->source_tpu     = source_tpu;
  entry->sig            = 0UL;
  entry->bundle_seq     = 0UL;
  entry->bundle_txn_cnt = 1UL;
  entry->commission     = 0U;
  fd_memset( entry->commission_pubkey, 0, 32UL );
  ctx->metrics.txn_received_cnt++;
}

/* Called for each transaction in a bundle.  Simply counts up
   bundle_txn_cnt, but does not publish anything. */

static bool
fd_bundle_client_visit_pb_bundle_txn_preflight(
    pb_istream_t *     istream,
    pb_field_t const * field,
    void **            arg
) {
  (void)istream; (void)field;
  fd_bundle_tile_t * ctx = *arg;
  ctx->bundle_txn_cnt++;
  return true;
}

/* Called for each transaction in a bundle.  Publishes each transaction
   to the tango message bus. */

static bool
fd_bundle_client_visit_pb_bundle_txn(
    pb_istream_t *     istream,
    pb_field_t const * field,
    void **            arg
) {
  (void)field;
  fd_bundle_tile_t * ctx = *arg;

  packet_Packet packet = packet_Packet_init_default;
  if( FD_UNLIKELY( !pb_decode( istream, &packet_Packet_msg, &packet ) ) ) {
    ctx->metrics.decode_fail_cnt++;
    FD_LOG_WARNING(( "Protobuf decode of (packet.Packet) failed" ));
    return false;
  }

  if( FD_UNLIKELY( packet.data.size == 0 ) ) {
    FD_LOG_WARNING(( "Bundle server delivered an empty packet, ignoring" ));
    return true;
  }

  if( FD_UNLIKELY( packet.data.size > FD_TXN_MTU ) ) {
    FD_LOG_WARNING(( "Bundle server delivered an oversize transaction, ignoring" ));
    return true;
  }

  uint _ip4; uint ip4 = fd_uint_if( packet.has_meta, fd_cstr_to_ip4_addr( packet.meta.addr, &_ip4 ) ? _ip4 : ctx->server_ip4_addr, ctx->server_ip4_addr );
  fd_bundle_tile_publish_bundle_txn(
      ctx,
      packet.data.bytes, packet.data.size,
      ctx->bundle_txn_cnt,
      ip4
  );

  return true;
}

static void
fd_bundle_client_sample_rx_delay(
    fd_bundle_tile_t *                ctx,
    google_protobuf_Timestamp const * ts
) {
  ulong tsorig = (ulong)ts->seconds*(ulong)1e9 + (ulong)ts->nanos;
  fd_histf_sample( ctx->metrics.msg_rx_delay, fd_ulong_sat_sub( (ulong)ctx->cached_ts, tsorig ) );
}

/* Called for each BundleUuid in a SubscribeBundlesResponse. */

static bool
fd_bundle_client_visit_pb_bundle_uuid(
    pb_istream_t *     istream,
    pb_field_t const * field,
    void **            arg
) {
  (void)field;
  fd_bundle_tile_t * ctx = *arg;

  /* Reset bundle state */

  ctx->bundle_txn_cnt = 0UL;

  /* Do two decode passes.  This is required because we need to know the
     number of transactions in a bundle ahead of time.  However, due to
     the Protobuf wire encoding, we don't know the number of txns that
     will come until we've parsed everything.

     First pass: Count number of bundles. */

  pb_istream_t peek = *istream;
  bundle_BundleUuid bundle = bundle_BundleUuid_init_default;
  bundle.bundle.packets = (pb_callback_t) {
    .funcs.decode = fd_bundle_client_visit_pb_bundle_txn_preflight,
    .arg          = ctx
  };
  if( FD_UNLIKELY( !pb_decode( &peek, &bundle_BundleUuid_msg, &bundle ) ) ) {
    ctx->metrics.decode_fail_cnt++;
    FD_LOG_WARNING(( "Protobuf decode of (bundle.BundleUuid) failed: %s", peek.errmsg ));
    return false;
  }

  /* At this opint, ctx->bundle_txn_cnt is correctly set.  Too many txns
     is treated as a NOP.

     Second pass: Actually publish bundle packets */

  if( FD_UNLIKELY( ctx->bundle_txn_cnt>FD_BUNDLE_CLIENT_MAX_TXN_PER_BUNDLE ) ) return true;

  if( FD_UNLIKELY( pending_txn_avail( ctx->pending_txns )<ctx->bundle_txn_cnt ) ) {
    ctx->metrics.backpressure_drop_cnt += ctx->bundle_txn_cnt;
    return true;
  }

  ctx->bundle_seq++;
  bundle = (bundle_BundleUuid)bundle_BundleUuid_init_default;
  bundle.bundle.packets = (pb_callback_t) {
    .funcs.decode = fd_bundle_client_visit_pb_bundle_txn,
    .arg          = ctx
  };

  ctx->metrics.bundle_received_cnt++;

  if( FD_UNLIKELY( !pb_decode( istream, &bundle_BundleUuid_msg, &bundle ) ) ) {
    ctx->metrics.decode_fail_cnt++;
    FD_LOG_WARNING(( "Protobuf decode of (bundle.BundleUuid) failed (internal error): %s", istream->errmsg ));
    return false;
  }

  fd_bundle_client_sample_rx_delay( ctx, &bundle.bundle.header.ts );

  return true;
}

/* Handle a SubscribeBundlesResponse from a SubscribeBundles gRPC call. */

static void
fd_bundle_client_handle_bundle_batch(
    fd_bundle_tile_t * ctx,
    pb_istream_t *     istream
) {
  if( FD_UNLIKELY( !ctx->builder_info_avail ) ) {
    ctx->metrics.missing_builder_info_fail_cnt++; /* unreachable */
    return;
  }

  block_engine_SubscribeBundlesResponse res = block_engine_SubscribeBundlesResponse_init_default;
  res.bundles = (pb_callback_t) {
    .funcs.decode = fd_bundle_client_visit_pb_bundle_uuid,
    .arg          = ctx
  };
  if( FD_UNLIKELY( !pb_decode( istream, &block_engine_SubscribeBundlesResponse_msg, &res ) ) ) {
    ctx->metrics.decode_fail_cnt++;
    FD_LOG_WARNING(( "Protobuf decode of (block_engine.SubscribeBundlesResponse) failed: %s", istream->errmsg ));
    return;
  }
}

/* Called for each 'Packet' (a regular transaction) of a
   SubscribePacketsResponse. */

static bool
fd_bundle_client_visit_pb_packet(
    pb_istream_t *     istream,
    pb_field_t const * field,
    void **            arg
) {
  (void)field;
  fd_bundle_tile_t * ctx = *arg;

  packet_Packet packet = packet_Packet_init_default;
  if( FD_UNLIKELY( !pb_decode( istream, &packet_Packet_msg, &packet ) ) ) {
    ctx->metrics.decode_fail_cnt++;
    FD_LOG_WARNING(( "Protobuf decode of (packet.Packet) failed" ));
    return false;
  }

  if( FD_UNLIKELY( packet.data.size == 0 ) ) {
    FD_LOG_WARNING(( "Bundle server delivered an empty packet, ignoring" ));
    return true;
  }

  if( FD_UNLIKELY( packet.data.size > FD_TXN_MTU ) ) {
    FD_LOG_WARNING(( "Bundle server delivered an oversize transaction, ignoring" ));
    return true;
  }


  uint _ip4; uint ip4 = fd_uint_if( packet.has_meta, fd_cstr_to_ip4_addr( packet.meta.addr, &_ip4 ) ? _ip4 : 0U, 0U );
  fd_bundle_tile_publish_txn( ctx, packet.data.bytes, packet.data.size, ip4, FD_TXN_M_TPU_SOURCE_BUNDLE );
  ctx->metrics.packet_received_cnt++;

  return true;
}

/* TPU endpoint packet visitor - same as above but increments TPU-specific metrics */
static bool
fd_bundle_tpu_client_visit_pb_packet(
    pb_istream_t *     istream,
    pb_field_t const * field,
    void **            arg
) {
  (void)field;
  fd_bundle_tile_t * ctx = *arg;

  packet_Packet packet = packet_Packet_init_default;
  if( FD_UNLIKELY( !pb_decode( istream, &packet_Packet_msg, &packet ) ) ) {
    ctx->metrics.decode_fail_cnt++;
    FD_LOG_WARNING(( "Protobuf decode of TPU (packet.Packet) failed" ));
    return false;
  }

  if( FD_UNLIKELY( packet.data.size == 0 ) ) {
    FD_LOG_WARNING(( "TPU endpoint delivered an empty packet, ignoring" ));
    return true;
  }

  if( FD_UNLIKELY( packet.data.size > FD_TXN_MTU ) ) {
    FD_LOG_WARNING(( "TPU endpoint delivered an oversize transaction, ignoring" ));
    return true;
  }

  uint _ip4; uint ip4 = fd_uint_if( packet.has_meta, fd_cstr_to_ip4_addr( packet.meta.addr, &_ip4 ) ? _ip4 : 0U, 0U );
  fd_bundle_tile_publish_txn( ctx, packet.data.bytes, packet.data.size, ip4, FD_TXN_M_TPU_SOURCE_HTPU );
  ctx->metrics.tpu_packet_received_cnt++;
  ctx->metrics.tpu_txn_received_cnt++;

  return true;
}

/* Handle a SubscribePacketsResponse from a SubscribePackets gRPC call. */

static void
fd_bundle_client_handle_packet_batch(
    fd_bundle_tile_t * ctx,
    pb_istream_t *     istream
) {
  block_engine_SubscribePacketsResponse res = block_engine_SubscribePacketsResponse_init_default;
  res.batch.packets = (pb_callback_t) {
    .funcs.decode = fd_bundle_client_visit_pb_packet,
    .arg          = ctx
  };
  if( FD_UNLIKELY( !pb_decode( istream, &block_engine_SubscribePacketsResponse_msg, &res ) ) ) {
    ctx->metrics.decode_fail_cnt++;
    FD_LOG_WARNING(( "Protobuf decode of (block_engine.SubscribePacketsResponse) failed" ));
    return;
  }

  fd_bundle_client_sample_rx_delay( ctx, &res.header.ts );
}

/* Handle a SubscribePacketsResponse from the TPU endpoint.
   Note: The relayer uses tpu.SubscribePacketsResponse which has a oneof msg
   containing either a heartbeat or a packet batch.

   Nanopb oneof workaround: When decoding a oneof field, nanopb memsets the union
   to zero if which_msg differs from the incoming field tag, clearing any callbacks.
   Additionally, pb_decode() calls pb_message_set_to_defaults() which resets which_msg
   to 0. To preserve our callback on batch.packets:
   1. Pre-set which_msg to batch_tag so the memset check (which_msg != tag) fails
   2. Use pb_decode_ex with PB_DECODE_NOINIT to skip the defaults reset
   If a heartbeat arrives, the memset happens, but we don't need callbacks for those.

   TODO: apparently we should be able to use submsg_callback to have a separate callback
   function that can set the fields before submessage is decoded, but ¯\_(ツ)_/¯. i'll figure
   that out later; see case PB_HTYPE_ONEOF in pb_decode.c */
static void
fd_bundle_tpu_client_handle_packet_batch(
    fd_bundle_tile_t * ctx,
    pb_istream_t *     istream
) {
  tpu_SubscribePacketsResponse res = tpu_SubscribePacketsResponse_init_default;
  res.which_msg = tpu_SubscribePacketsResponse_batch_tag;
  res.msg.batch.packets = (pb_callback_t) {
    .funcs.decode = fd_bundle_tpu_client_visit_pb_packet,
    .arg          = ctx
  };

  if( FD_UNLIKELY( !pb_decode_ex( istream, &tpu_SubscribePacketsResponse_msg, &res, PB_DECODE_NOINIT ) ) ) {
    ctx->metrics.decode_fail_cnt++;
    FD_LOG_WARNING(( "Protobuf decode of TPU (tpu.SubscribePacketsResponse) failed" ));
    return;
  }

  /* Sample RX delay for batch messages */
  if( res.which_msg == tpu_SubscribePacketsResponse_batch_tag ) {
    if( res.has_header ) {
      fd_bundle_client_sample_rx_delay( ctx, &res.header.ts );
    }
  }
}

/* Handle a BlockBuilderFeeInfoResponse from a GetBlockBuilderFeeInfo
   gRPC call. */

static void
fd_bundle_client_handle_builder_fee_info(
    fd_bundle_tile_t * ctx,
    pb_istream_t *     istream
) {
  block_engine_BlockBuilderFeeInfoResponse res = block_engine_BlockBuilderFeeInfoResponse_init_default;
  if( FD_UNLIKELY( !pb_decode( istream, &block_engine_BlockBuilderFeeInfoResponse_msg, &res ) ) ) {
    ctx->metrics.decode_fail_cnt++;
    FD_LOG_WARNING(( "Protobuf decode of (block_engine.BlockBuilderFeeInfoResponse) failed" ));
    return;
  }
  if( FD_UNLIKELY( res.commission > 100 ) ) {
    ctx->metrics.decode_fail_cnt++;
    FD_LOG_WARNING(( "BlockBuilderFeeInfoResponse commission out of range (0-100): %lu", res.commission ));
    return;
  }

  uchar decoded_builder_pubkey[ 32 ];
  if( FD_UNLIKELY( !fd_base58_decode_32( res.pubkey, decoded_builder_pubkey ) ) ) {
    FD_LOG_HEXDUMP_WARNING(( "Invalid pubkey in BlockBuilderFeeInfoResponse", res.pubkey, strnlen( res.pubkey, sizeof(res.pubkey) ) ));
    return;
  }

  ctx->builder_commission = (uchar)res.commission; /* Apply update atomically */
  fd_memcpy( ctx->builder_pubkey, decoded_builder_pubkey, sizeof(ctx->builder_pubkey) );

  long validity_duration_ns = (long)( 60e9 * 5. ); /* 5 minutes */
  ctx->builder_info_avail = 1;
  ctx->builder_info_valid_until = fd_bundle_now() + validity_duration_ns;
}

static void
fd_bundle_client_grpc_tx_complete(
    void * app_ctx,
    ulong  request_ctx
) {
  (void)app_ctx; (void)request_ctx;
}

void
fd_bundle_client_grpc_rx_start(
    void * app_ctx,
    ulong  request_ctx
) {
  fd_bundle_tile_t * ctx = app_ctx;
  switch( request_ctx ) {
  case FD_BUNDLE_CLIENT_REQ_Bundle_SubscribePackets:
    ctx->packet_subscription_live = 1;
    ctx->packet_subscription_wait = 0;
    break;
  case FD_BUNDLE_CLIENT_REQ_Bundle_SubscribeBundles:
    ctx->bundle_subscription_live = 1;
    ctx->bundle_subscription_wait = 0;
    break;
  case FD_BUNDLE_CLIENT_REQ_SubscribeBlocks:
    ctx->harmonic_block_subscription_live = 1;
    ctx->harmonic_block_subscription_wait = 0;
    FD_LOG_INFO(( "Block subscription stream started" ));
    break;
  case FD_BUNDLE_CLIENT_REQ_SubmitLeaderWindowInfo:
    /* Response handler will be called in rx_msg */
    break;
  }
}

void
fd_bundle_client_grpc_rx_msg(
    void *       app_ctx,
    void const * protobuf,
    ulong        protobuf_sz,
    ulong        request_ctx
) {
  fd_bundle_tile_t * ctx = app_ctx;
  ctx->metrics.proto_received_bytes += protobuf_sz;
  pb_istream_t istream = pb_istream_from_buffer( protobuf, protobuf_sz );
  switch( request_ctx ) {
  case FD_BUNDLE_CLIENT_REQ_Auth_GenerateAuthChallenge:
    if( FD_UNLIKELY( !fd_bundle_auther_handle_challenge_resp( &ctx->auther, protobuf, protobuf_sz ) ) ) {
      ctx->metrics.decode_fail_cnt++;
      fd_bundle_tile_backoff( ctx, fd_bundle_now() );
    }
    break;
  case FD_BUNDLE_CLIENT_REQ_Auth_GenerateAuthTokens:
    if( FD_UNLIKELY( !fd_bundle_auther_handle_tokens_resp( &ctx->auther, protobuf, protobuf_sz ) ) ) {
      ctx->metrics.decode_fail_cnt++;
      fd_bundle_tile_backoff( ctx, fd_bundle_now() );
    }
    break;
  case FD_BUNDLE_CLIENT_REQ_Bundle_SubscribeBundles:
    fd_bundle_client_handle_bundle_batch( ctx, &istream );
    break;
  case FD_BUNDLE_CLIENT_REQ_Bundle_SubscribePackets:
    fd_bundle_client_handle_packet_batch( ctx, &istream );
    break;
  case FD_BUNDLE_CLIENT_REQ_Bundle_GetBlockBuilderFeeInfo:
    fd_bundle_client_handle_builder_fee_info( ctx, &istream );
    break;
  case FD_BUNDLE_CLIENT_REQ_SubscribeBlocks:
    FD_LOG_DEBUG(( "CAVEY DEBUG: Block message received: %lu bytes", protobuf_sz ));
    fd_bundle_client_handle_block_batch( ctx, &istream );
    break;
  case FD_BUNDLE_CLIENT_REQ_SubmitLeaderWindowInfo: {
    /* Handle SubmitLeaderWindowInfoResponse (empty response) */
    block_engine_SubmitLeaderWindowInfoResponse res = block_engine_SubmitLeaderWindowInfoResponse_init_default;
    if( FD_UNLIKELY( !pb_decode( &istream, &block_engine_SubmitLeaderWindowInfoResponse_msg, &res ) ) ) {
      ctx->metrics.decode_fail_cnt++;
      FD_LOG_WARNING(( "Protobuf decode of (block_engine.SubmitLeaderWindowInfoResponse) failed: %s", istream.errmsg ));
    }
    break;
  }
  default:
    FD_LOG_ERR(( "Received unexpected gRPC message (request_ctx=%lu)", request_ctx ));
  }
}

static void
fd_bundle_client_request_failed( fd_bundle_tile_t * ctx,
                                 ulong              request_ctx ) {
  fd_bundle_tile_backoff( ctx, fd_bundle_now() );
  switch( request_ctx ) {
  case FD_BUNDLE_CLIENT_REQ_Auth_GenerateAuthChallenge:
  case FD_BUNDLE_CLIENT_REQ_Auth_GenerateAuthTokens:
    fd_bundle_auther_handle_request_fail( &ctx->auther );
    break;
  }
}

void
fd_bundle_client_grpc_rx_end(
    void *                app_ctx,
    ulong                 request_ctx,
    fd_grpc_resp_hdrs_t * resp
) {
  fd_bundle_tile_t * ctx = app_ctx;
  if( FD_UNLIKELY( resp->h2_status!=200 ) ) {
    FD_LOG_WARNING(( "gRPC request failed (HTTP status %u)", resp->h2_status ));
    /* Unary RPC wait bits are normally cleared in the switch below, which we skip. */
    if( FD_UNLIKELY( request_ctx==FD_BUNDLE_CLIENT_REQ_SubmitLeaderWindowInfo ) ) {
      ctx->submit_leader_window_info_wait = 0;
    }
    fd_bundle_client_request_failed( ctx, request_ctx );
    return;
  }

  resp->grpc_msg_len = (uint)fd_url_unescape( resp->grpc_msg, resp->grpc_msg_len );
  if( !resp->grpc_msg_len ) {
    fd_memcpy( resp->grpc_msg, "unknown error", 13 );
    resp->grpc_msg_len = 13;
  }

  switch( request_ctx ) {
  case FD_BUNDLE_CLIENT_REQ_Bundle_SubscribePackets:
    ctx->packet_subscription_live = 0;
    ctx->packet_subscription_wait = 0;
    fd_bundle_tile_backoff( ctx, fd_bundle_now() );
    ctx->defer_reset = 1;
    FD_LOG_INFO(( "SubscribePackets stream failed (gRPC status %u-%s). Reconnecting ...",
                  resp->grpc_status, fd_grpc_status_cstr( resp->grpc_status ) ));
    return;
  case FD_BUNDLE_CLIENT_REQ_Bundle_SubscribeBundles:
    ctx->bundle_subscription_live = 0;
    ctx->bundle_subscription_wait = 0;
    fd_bundle_tile_backoff( ctx, fd_bundle_now() );
    ctx->defer_reset = 1;
    FD_LOG_INFO(( "SubscribeBundles stream failed (gRPC status %u-%s). Reconnecting ...",
                  resp->grpc_status, fd_grpc_status_cstr( resp->grpc_status ) ));
    return;
  case FD_BUNDLE_CLIENT_REQ_SubscribeBlocks:
    ctx->harmonic_block_subscription_live = 0;
    ctx->harmonic_block_subscription_wait = 0;
    fd_bundle_tile_backoff( ctx, fd_bundle_now() );
    ctx->defer_reset = 1;
    FD_LOG_INFO(( "SubscribeBlocks stream failed (gRPC status %u-%s). Reconnecting ...",
                  resp->grpc_status, fd_grpc_status_cstr( resp->grpc_status ) ));
    return;
  case FD_BUNDLE_CLIENT_REQ_Bundle_GetBlockBuilderFeeInfo:
    ctx->builder_info_wait = 0;
    break;
  case FD_BUNDLE_CLIENT_REQ_SubmitLeaderWindowInfo:
    ctx->submit_leader_window_info_wait = 0;
    break;
  default:
    break;
  }

  if( FD_UNLIKELY( resp->grpc_status!=FD_GRPC_STATUS_OK ) ) {
    FD_LOG_INFO(( "gRPC request failed (gRPC status %u-%s): %.*s",
                  resp->grpc_status, fd_grpc_status_cstr( resp->grpc_status ),
                  (int)resp->grpc_msg_len, resp->grpc_msg ));
    fd_bundle_client_request_failed( ctx, request_ctx );
    if( resp->grpc_status==FD_GRPC_STATUS_UNAUTHENTICATED ||
        resp->grpc_status==FD_GRPC_STATUS_PERMISSION_DENIED ) {
      fd_bundle_auther_reset( &ctx->auther );
    }
    return;
  }
}

void
fd_bundle_client_grpc_rx_timeout(
    void * app_ctx,
    ulong  request_ctx,  /* FD_BUNDLE_CLIENT_REQ_{...} */
    int    deadline_kind /* FD_GRPC_DEADLINE_{HEADER|RX_END} */
) {
  (void)deadline_kind;
  FD_LOG_WARNING(( "Request timed out: %s", fd_bundle_request_ctx_cstr( request_ctx ) ));
  fd_bundle_tile_t * ctx = app_ctx;
  if( FD_UNLIKELY( request_ctx==FD_BUNDLE_CLIENT_REQ_SubmitLeaderWindowInfo ) ) {
    ctx->submit_leader_window_info_wait = 0;
  }
  ctx->defer_reset = 1;
}

static void
fd_bundle_client_grpc_ping_ack( void * app_ctx ) {
  fd_bundle_tile_t * ctx = app_ctx;
  long rtt_sample = fd_keepalive_rx( ctx->keepalive, fd_bundle_now() );
  if( FD_LIKELY( rtt_sample ) ) {
    fd_rtt_sample( ctx->rtt, (float)rtt_sample, 0 );
    FD_LOG_DEBUG(( "Keepalive ACK" ));
  }
  ctx->metrics.ping_ack_cnt++;
}

fd_grpc_client_callbacks_t fd_bundle_client_grpc_callbacks = {
  .conn_established = fd_bundle_client_grpc_conn_established,
  .conn_dead        = fd_bundle_client_grpc_conn_dead,
  .tx_complete      = fd_bundle_client_grpc_tx_complete,
  .rx_start         = fd_bundle_client_grpc_rx_start,
  .rx_msg           = fd_bundle_client_grpc_rx_msg,
  .rx_end           = fd_bundle_client_grpc_rx_end,
  .rx_timeout       = fd_bundle_client_grpc_rx_timeout,
  .ping_ack         = fd_bundle_client_grpc_ping_ack,
};

int
fd_bundle_client_status( fd_bundle_tile_t const * ctx ) {
  if( FD_UNLIKELY( ( !ctx->tcp_sock_connected ) |
                   ( !ctx->grpc_client        ) ) ) {
    return FD_BUNDLE_BLOCK_ENGINE_STATUS_DISCONNECTED;
  }

  fd_h2_conn_t * conn = fd_grpc_client_h2_conn( ctx->grpc_client );
  if( FD_UNLIKELY( !conn ) ) {
    return FD_BUNDLE_BLOCK_ENGINE_STATUS_DISCONNECTED; /* no conn */
  }
  if( FD_UNLIKELY( conn->flags &
      ( FD_H2_CONN_FLAGS_DEAD |
        FD_H2_CONN_FLAGS_SEND_GOAWAY ) ) ) {
    return FD_BUNDLE_BLOCK_ENGINE_STATUS_DISCONNECTED;
  }

  if( FD_UNLIKELY( conn->flags &
      ( FD_H2_CONN_FLAGS_CLIENT_INITIAL      |
        FD_H2_CONN_FLAGS_WAIT_SETTINGS_ACK_0 |
        FD_H2_CONN_FLAGS_WAIT_SETTINGS_0     |
        FD_H2_CONN_FLAGS_SERVER_INITIAL ) ) ) {
    return FD_BUNDLE_BLOCK_ENGINE_STATUS_CONNECTING; /* connection is not ready */
  }

  if( FD_UNLIKELY( ctx->auther.state != FD_BUNDLE_AUTH_STATE_DONE_WAIT ) ) {
    return FD_BUNDLE_BLOCK_ENGINE_STATUS_CONNECTING; /* not authenticated */
  }

  if( FD_UNLIKELY( ( !ctx->builder_info_avail       ) |
                   ( !ctx->packet_subscription_live ) |
                   ( !ctx->bundle_subscription_live ) ) ) {
    return FD_BUNDLE_BLOCK_ENGINE_STATUS_CONNECTING; /* not fully connected */
  }

  if( FD_UNLIKELY( fd_keepalive_is_timeout( ctx->keepalive, fd_bundle_now() ) ) ) {
    return FD_BUNDLE_BLOCK_ENGINE_STATUS_DISCONNECTED; /* possible timeout */
  }

  if( FD_UNLIKELY( !fd_grpc_client_is_connected( ctx->grpc_client ) ) ) {
    return FD_BUNDLE_BLOCK_ENGINE_STATUS_CONNECTING;
  }

  /* As far as we know, the bundle connection is alive and well. */
  return FD_BUNDLE_BLOCK_ENGINE_STATUS_CONNECTED;
}

#undef DISCONNECTED
#undef CONNECTING
#undef CONNECTED

int
fd_bundle_tpu_client_status( fd_bundle_tile_t const * ctx ) {
  /* If TPU connection is not enabled, always return disconnected */
  if( FD_UNLIKELY( !ctx->tpu_conn_enabled ) ) {
    return FD_BUNDLE_BLOCK_ENGINE_STATUS_DISCONNECTED;
  }

  if( FD_UNLIKELY( ( !ctx->tpu_tcp_sock_connected ) |
                   ( !ctx->tpu_grpc_client        ) ) ) {
    return FD_BUNDLE_BLOCK_ENGINE_STATUS_DISCONNECTED;
  }

  fd_h2_conn_t * conn = fd_grpc_client_h2_conn( ctx->tpu_grpc_client );
  if( FD_UNLIKELY( !conn ) ) {
    return FD_BUNDLE_BLOCK_ENGINE_STATUS_DISCONNECTED;
  }
  if( FD_UNLIKELY( conn->flags &
      ( FD_H2_CONN_FLAGS_DEAD |
        FD_H2_CONN_FLAGS_SEND_GOAWAY ) ) ) {
    return FD_BUNDLE_BLOCK_ENGINE_STATUS_DISCONNECTED;
  }

  if( FD_UNLIKELY( conn->flags &
      ( FD_H2_CONN_FLAGS_CLIENT_INITIAL      |
        FD_H2_CONN_FLAGS_WAIT_SETTINGS_ACK_0 |
        FD_H2_CONN_FLAGS_WAIT_SETTINGS_0     |
        FD_H2_CONN_FLAGS_SERVER_INITIAL ) ) ) {
    return FD_BUNDLE_BLOCK_ENGINE_STATUS_CONNECTING;
  }

  if( FD_UNLIKELY( ctx->tpu_auther.state != FD_BUNDLE_AUTH_STATE_DONE_WAIT ) ) {
    return FD_BUNDLE_BLOCK_ENGINE_STATUS_CONNECTING;
  }

  if( FD_UNLIKELY( !ctx->tpu_packet_subscription_live ) ) {
    return FD_BUNDLE_BLOCK_ENGINE_STATUS_CONNECTING;
  }

  if( FD_UNLIKELY( fd_keepalive_is_timeout( ctx->tpu_keepalive, fd_bundle_now() ) ) ) {
    return FD_BUNDLE_BLOCK_ENGINE_STATUS_DISCONNECTED;
  }

  if( FD_UNLIKELY( !fd_grpc_client_is_connected( ctx->tpu_grpc_client ) ) ) {
    return FD_BUNDLE_BLOCK_ENGINE_STATUS_CONNECTING;
  }

  return FD_BUNDLE_BLOCK_ENGINE_STATUS_CONNECTED;
}

FD_FN_CONST char const *
fd_bundle_request_ctx_cstr( ulong request_ctx ) {
  switch( request_ctx ) {
  case FD_BUNDLE_CLIENT_REQ_Auth_GenerateAuthChallenge:
    return "GenerateAuthChallenge";
  case FD_BUNDLE_CLIENT_REQ_Auth_GenerateAuthTokens:
    return "GenerateAuthTokens";
  case FD_BUNDLE_CLIENT_REQ_Bundle_SubscribePackets:
    return "SubscribePackets";
  case FD_BUNDLE_CLIENT_REQ_Bundle_SubscribeBundles:
    return "SubscribeBundles";
  case FD_BUNDLE_CLIENT_REQ_Bundle_GetBlockBuilderFeeInfo:
    return "GetBlockBuilderFeeInfo";
  case FD_BUNDLE_CLIENT_REQ_SubscribeBlocks:
    return "SubscribeBlocks";
  case FD_BUNDLE_CLIENT_REQ_SubmitLeaderWindowInfo:
    return "SubmitLeaderWindowInfo";
  case FD_BUNDLE_CLIENT_REQ_SubscribePacketsTPU:
    return "SubscribePacketsTPU";
  default:
    return "unknown";
  }
}

/* ========== Block mode protobuf handlers ========== */

/* Forward block transaction to tango message bus.
   Block transactions have no len=5 limit.
   For blocks, the slot is parsed from the uuid field. */
static void
fd_harmonic_block_tile_publish_block_txn(
    fd_bundle_tile_t * ctx,
    void const *       txn,
    ulong              txn_sz,
    ulong              block_txn_cnt,
    uint               source_ipv4
) {
  fd_txn_m_t * txnm = fd_chunk_to_laddr( ctx->verify_out.mem, ctx->verify_out.chunk );
  *txnm = (fd_txn_m_t) {
    .reference_slot = 0UL,
    .payload_sz     = (ushort)txn_sz,
    .txn_t_sz       = 0U,
    .source_ipv4    = source_ipv4,
    .source_tpu     = FD_TXN_M_TPU_SOURCE_HARMONIC,
    .block_engine   = {
      .block_slot     = ctx->harmonic_block_slot,  /* Intended landing slot */
      .bundle_txn_cnt = (ushort)block_txn_cnt,
      .commission     = (uchar)ctx->builder_commission
    },
  };
  memcpy( txnm->block_engine.commission_pubkey, ctx->builder_pubkey, 32UL );
  fd_memcpy( fd_txn_m_payload( txnm ), txn, txn_sz );

  ulong sz  = fd_txn_m_realized_footprint( txnm, 0, 0 );
  ulong sig = 2UL; /* Use sig=2 to distinguish block txns from bundle txns (sig=1) */

  if( FD_UNLIKELY( !ctx->stem ) ) {
    FD_LOG_CRIT(( "ctx->stem not set. This is a bug." ));
  }

  ulong tspub = (ulong)fd_frag_meta_ts_comp( fd_bundle_now() );
  fd_stem_publish( ctx->stem, ctx->verify_out.idx, sig, ctx->verify_out.chunk, sz, 0UL, 0UL, tspub );
  ctx->verify_out.chunk = fd_dcache_compact_next( ctx->verify_out.chunk, sz, ctx->verify_out.chunk0, ctx->verify_out.wmark );
  ctx->harmonic_block_txn_received_cnt++;
}

/* Called for each transaction in a block.  Counts transactions. */
static bool
fd_harmonic_block_client_visit_pb_block_txn_preflight(
    pb_istream_t *     istream,
    pb_field_t const * field,
    void **            arg
) {
  (void)istream; (void)field;
  fd_bundle_tile_t * ctx = *arg;
  ctx->harmonic_block_txn_cnt++;
  return true;
}

/* Called for each transaction in a block.  Publishes each transaction. */
static bool
fd_harmonic_block_client_visit_pb_block_txn(
    pb_istream_t *     istream,
    pb_field_t const * field,
    void **            arg
) {
  (void)field;
  fd_bundle_tile_t * ctx = *arg;

  packet_Packet packet = packet_Packet_init_default;
  if( FD_UNLIKELY( !pb_decode( istream, &packet_Packet_msg, &packet ) ) ) {
    ctx->metrics.decode_fail_cnt++;
    FD_LOG_WARNING(( "Protobuf decode of block (packet.Packet) failed" ));
    return false;
  }

  if( FD_UNLIKELY( packet.data.size == 0 ) ) {
    FD_LOG_WARNING(( "Block server delivered an empty packet, ignoring" ));
    return true;
  }

  if( FD_UNLIKELY( packet.data.size > FD_TXN_MTU ) ) {
    FD_LOG_WARNING(( "Block server delivered an oversize transaction, ignoring" ));
    return true;
  }

  uint _ip4; uint ip4 = fd_uint_if( packet.has_meta, fd_cstr_to_ip4_addr( packet.meta.addr, &_ip4 ) ? _ip4 : ctx->server_ip4_addr, ctx->server_ip4_addr );
  fd_harmonic_block_tile_publish_block_txn(
      ctx,
      packet.data.bytes, packet.data.size,
      ctx->harmonic_block_txn_cnt,
      ip4
  );

  return true;
}

/* Called for each BundleUuid in a SubscribeBundles response (used for blocks).
   Note: For blocks, there is NO len=5 limit.
   For blocks, the uuid field contains a slot number as a string. */
static bool
fd_harmonic_block_client_visit_pb_block_uuid(
    pb_istream_t *     istream,
    pb_field_t const * field,
    void **            arg
) {
  (void)field;
  fd_bundle_tile_t * ctx = *arg;

  /* Reset block state */
  ctx->harmonic_block_txn_cnt = 0UL;
  ctx->harmonic_block_slot    = 0UL;

  /* First pass: Count number of transactions and extract slot from uuid */
  pb_istream_t peek = *istream;
  bundle_BundleUuid bundle = bundle_BundleUuid_init_default;
  bundle.bundle.packets = (pb_callback_t) {
    .funcs.decode = fd_harmonic_block_client_visit_pb_block_txn_preflight,
    .arg          = ctx
  };
  if( FD_UNLIKELY( !pb_decode( &peek, &bundle_BundleUuid_msg, &bundle ) ) ) {
    ctx->metrics.decode_fail_cnt++;
    FD_LOG_WARNING(( "Protobuf decode of block (bundle.BundleUuid) failed: %s", peek.errmsg ));
    return false;
  }

  /* Parse uuid as slot number.  For blocks, uuid contains a decimal slot string. */
  if( FD_LIKELY( bundle.uuid.size>0UL && bundle.uuid.size<21UL ) ) {
    char slot_str[ 21 ];
    fd_memcpy( slot_str, bundle.uuid.bytes, bundle.uuid.size );
    slot_str[ bundle.uuid.size ] = '\0';

    char * endptr = NULL;
    ulong slot = strtoul( slot_str, &endptr, 10 );
    if( FD_UNLIKELY( endptr==slot_str || *endptr!='\0' ) ) {
      FD_LOG_WARNING(( "Invalid block slot in uuid: %s", slot_str ));
      ctx->metrics.decode_fail_cnt++;
      return true;  /* Skip this block but continue processing */
    }
    ctx->harmonic_block_slot = slot;
  } else {
    FD_LOG_WARNING(( "Invalid block uuid size: %lu", (ulong)bundle.uuid.size ));
    ctx->metrics.decode_fail_cnt++;
    return true;  /* Skip this block but continue processing */
  }

  /* No len=5 limit for blocks! */

  ctx->harmonic_block_seq++;
  bundle = (bundle_BundleUuid)bundle_BundleUuid_init_default;
  bundle.bundle.packets = (pb_callback_t) {
    .funcs.decode = fd_harmonic_block_client_visit_pb_block_txn,
    .arg          = ctx
  };

  ctx->harmonic_block_received_cnt++;

  FD_LOG_DEBUG(( "Received block slot=%lu, %lu packets",
                 ctx->harmonic_block_slot, ctx->harmonic_block_txn_cnt ));

  /* Harmonic: bundle_txn_cnt is a ushort. Reject blocks exceeding USHORT_MAX.
     In practice, blocks typically have O(≈1000) txns */
  if( FD_UNLIKELY( ctx->harmonic_block_txn_cnt > USHORT_MAX ) ) {
    FD_LOG_WARNING(( "HARMONIC: rejecting block slot=%lu with txn_cnt %lu (exceeds USHORT_MAX)",
                     ctx->harmonic_block_slot, ctx->harmonic_block_txn_cnt ));
    return true;  /* Skip this block but continue processing */
  }

  if( FD_UNLIKELY( !pb_decode( istream, &bundle_BundleUuid_msg, &bundle ) ) ) {
    ctx->metrics.decode_fail_cnt++;
    FD_LOG_WARNING(( "Protobuf decode of block (bundle.BundleUuid) failed (internal error): %s", istream->errmsg ));
    return false;
  }

  FD_LOG_DEBUG(( "CAVEY DEBUG: bundle published all %lu block txns for slot=%lu",
                  ctx->harmonic_block_txn_cnt, ctx->harmonic_block_slot ));

  return true;
}

/* Handle a SubscribeBundlesResponse for harmonic blocks.
   Note: Uses the same proto as bundles but with different semantics. */
static void
fd_bundle_client_handle_block_batch(
    fd_bundle_tile_t * ctx,
    pb_istream_t *     istream
) {
  block_engine_SubscribeBundlesResponse res = block_engine_SubscribeBundlesResponse_init_default;
  res.bundles = (pb_callback_t) {
    .funcs.decode = fd_harmonic_block_client_visit_pb_block_uuid,
    .arg          = ctx
  };
  if( FD_UNLIKELY( !pb_decode( istream, &block_engine_SubscribeBundlesResponse_msg, &res ) ) ) {
    ctx->metrics.decode_fail_cnt++;
    FD_LOG_WARNING(( "Protobuf decode of (block_engine.SubscribeBundlesResponse) for blocks failed: %s", istream->errmsg ));
    return;
  }
}

/* ========== TPU endpoint connection implementation ========== */

static void
fd_bundle_tpu_client_backoff( fd_bundle_tile_t * ctx,
                              long               now ) {
  uint iter = ctx->tpu_backoff_iter;
  if( now < ctx->tpu_backoff_reset ) iter = 0U;
  iter++;

  /* FIXME proper backoff */
  long wait_ns = (long)2e9;
  wait_ns = (long)( fd_rng_ulong( ctx->rng ) & ( (1UL<<fd_ulong_find_msb_w_default( (ulong)wait_ns, 0 ))-1UL ) );

  ctx->tpu_backoff_until = now +   wait_ns;
  ctx->tpu_backoff_reset = now + 2*wait_ns;

  ctx->tpu_backoff_iter = iter;
}

void
fd_bundle_tpu_client_reset( fd_bundle_tile_t * ctx ) {
  if( FD_UNLIKELY( ctx->tpu_tcp_sock >= 0 ) ) {
    if( FD_UNLIKELY( 0!=close( ctx->tpu_tcp_sock ) ) ) {
      FD_LOG_ERR(( "close(tpu_tcp_sock=%i) failed (%i-%s)", ctx->tpu_tcp_sock, errno, fd_io_strerror( errno ) ));
    }
    ctx->tpu_tcp_sock = -1;
    ctx->tpu_tcp_sock_connected = 0;
  }
  ctx->tpu_defer_reset = 0;

  ctx->tpu_packet_subscription_live = 0;
  ctx->tpu_packet_subscription_wait = 0;

  ctx->tpu_config_avail = 0;
  ctx->tpu_config_wait  = 0;

  memset( ctx->tpu_rtt, 0, sizeof(fd_rtt_estimate_t) );

# if FD_HAS_OPENSSL
  if( FD_UNLIKELY( ctx->tpu_ssl ) ) {
    SSL_free( ctx->tpu_ssl );
    ctx->tpu_ssl = NULL;
  }
# endif

  /* Backoff for TPU connection */
  fd_bundle_tpu_client_backoff( ctx, fd_bundle_now() );

  fd_bundle_auther_reset( &ctx->tpu_auther );
  if( ctx->tpu_grpc_client ) {
    fd_grpc_client_reset( ctx->tpu_grpc_client );
  }
}

static int
fd_bundle_tpu_client_do_connect( fd_bundle_tile_t const * ctx,
                                 uint                     ip4_addr ) {
  struct sockaddr_in addr = {
    .sin_family      = AF_INET,
    .sin_addr.s_addr = ip4_addr,
    .sin_port        = fd_ushort_bswap( ctx->tpu_server_tcp_port )
  };
  errno = 0;
  int err = connect( ctx->tpu_tcp_sock, fd_type_pun_const( &addr ), sizeof(struct sockaddr_in) );
  (void)err; /* errno is the source of truth for connect's failure mode */
  return errno;
}

static void
fd_bundle_tpu_client_create_conn( fd_bundle_tile_t * ctx ) {
  fd_bundle_tpu_client_reset( ctx );

  /* FIXME IPv6 support */
  fd_addrinfo_t hints = {0};
  hints.ai_family = AF_INET;
  fd_addrinfo_t * res = NULL;
  uchar scratch[ 4096 ];
  void * pscratch = scratch;
  int err = fd_getaddrinfo( ctx->tpu_server_fqdn, &hints, &res, &pscratch, sizeof(scratch) );
  if( FD_UNLIKELY( err ) ) {
    FD_LOG_WARNING(( "fd_getaddrinfo `%s` (TPU endpoint) failed (%d-%s)", ctx->tpu_server_fqdn, err, fd_gai_strerror( err ) ));
    fd_bundle_tpu_client_reset( ctx );
    ctx->metrics.transport_fail_cnt++;
    return;
  }
  uint const ip4_addr = ((struct sockaddr_in *)res->ai_addr)->sin_addr.s_addr;
  ctx->tpu_server_ip4_addr = ip4_addr;

  int tcp_sock = socket( AF_INET, SOCK_STREAM|SOCK_CLOEXEC, 0 );
  if( FD_UNLIKELY( tcp_sock<0 ) ) {
    FD_LOG_ERR(( "socket(AF_INET,SOCK_STREAM|SOCK_CLOEXEC,0) for TPU endpoint failed (%i-%s)", errno, fd_io_strerror( errno ) ));
  }
  ctx->tpu_tcp_sock = tcp_sock;

  if( FD_UNLIKELY( 0!=setsockopt( tcp_sock, SOL_SOCKET, SO_RCVBUF, &ctx->so_rcvbuf, sizeof(int) ) ) ) {
    FD_LOG_ERR(( "setsockopt(SOL_SOCKET,SO_RCVBUF,%i) for TPU endpoint failed (%i-%s)", ctx->so_rcvbuf, errno, fd_io_strerror( errno ) ));
  }

  int tcp_nodelay = 1;
  if( FD_UNLIKELY( 0!=setsockopt( tcp_sock, SOL_TCP, TCP_NODELAY, &tcp_nodelay, sizeof(int) ) ) ) {
    FD_LOG_ERR(( "setsockopt for TPU endpoint failed (%d-%s)", errno, fd_io_strerror( errno ) ));
  }

  if( FD_UNLIKELY( fcntl( tcp_sock, F_SETFL, O_NONBLOCK )==-1 ) ) {
    FD_LOG_ERR(( "fcntl(tpu_tcp_sock,F_SETFL,O_NONBLOCK) failed (%i-%s)", errno, fd_io_strerror( errno ) ));
  }

  char const * scheme = "http";
# if FD_HAS_OPENSSL
  if( ctx->tpu_is_ssl ) scheme = "https";
# endif

  FD_LOG_INFO(( "Connecting to TPU endpoint %s://" FD_IP4_ADDR_FMT ":%hu (%.*s)",
                scheme,
                FD_IP4_ADDR_FMT_ARGS( ip4_addr ), ctx->tpu_server_tcp_port,
                (int)ctx->tpu_server_sni_len, ctx->tpu_server_sni ));

  int connect_err = fd_bundle_tpu_client_do_connect( ctx, ip4_addr );
  if( FD_UNLIKELY( connect_err ) ) {
    if( FD_UNLIKELY( connect_err!=EINPROGRESS ) ) {
      FD_LOG_WARNING(( "connect(tpu_tcp_sock," FD_IP4_ADDR_FMT ":%u) failed (%i-%s)",
                      FD_IP4_ADDR_FMT_ARGS( ip4_addr ), ctx->tpu_server_tcp_port,
                      connect_err, fd_io_strerror( connect_err ) ));
      fd_bundle_tpu_client_reset( ctx );
      ctx->metrics.transport_fail_cnt++;
      return;
    }
  }

# if FD_HAS_OPENSSL
  if( ctx->tpu_is_ssl ) {
    BIO * bio = BIO_new_socket( ctx->tpu_tcp_sock, BIO_NOCLOSE );
    if( FD_UNLIKELY( !bio ) ) {
      FD_LOG_ERR(( "BIO_new_socket for TPU endpoint failed" ));
    }

    SSL * ssl = SSL_new( ctx->ssl_ctx );  /* Reuse same SSL context */
    if( FD_UNLIKELY( !ssl ) ) {
      FD_LOG_ERR(( "SSL_new for TPU endpoint failed" ));
    }

    SSL_set_bio( ssl, bio, bio );
    SSL_set_connect_state( ssl );

    if( FD_UNLIKELY( !SSL_set_tlsext_host_name( ssl, ctx->tpu_server_sni ) ) ) {
      FD_LOG_ERR(( "SSL_set_tlsext_host_name for TPU endpoint failed" ));
    }

    if( FD_UNLIKELY( !SSL_set1_host( ssl, ctx->tpu_server_sni ) ) ) {
      FD_LOG_ERR(( "SSL_set1_host for TPU endpoint failed" ));
    }

    ctx->tpu_ssl = ssl;
  }
# endif /* FD_HAS_OPENSSL */

  fd_grpc_client_reset( ctx->tpu_grpc_client );
  fd_keepalive_init( ctx->tpu_keepalive, ctx->rng, ctx->keepalive_interval, ctx->keepalive_interval, fd_bundle_now() );
}

static int
fd_bundle_tpu_client_drive_io( fd_bundle_tile_t * ctx,
                               int *              charge_busy ) {
# if FD_HAS_OPENSSL
  if( ctx->tpu_is_ssl ) {
    return fd_grpc_client_rxtx_ossl( ctx->tpu_grpc_client, ctx->tpu_ssl, charge_busy );
  }
# endif /* FD_HAS_OPENSSL */

  return fd_grpc_client_rxtx_socket( ctx->tpu_grpc_client, ctx->tpu_tcp_sock, charge_busy );
}

static void
fd_bundle_tpu_client_request_tpu_configs( fd_bundle_tile_t * ctx ) {
  if( FD_UNLIKELY( fd_grpc_client_request_is_blocked( ctx->tpu_grpc_client ) ) ) return;

  tpu_GetTpuConfigsRequest req = tpu_GetTpuConfigsRequest_init_default;
  static char const path[] = "/relayer.Relayer/GetTpuConfigs";
  fd_grpc_h2_stream_t * request = fd_grpc_client_request_start(
      ctx->tpu_grpc_client,
      path, sizeof(path)-1,
      FD_BUNDLE_CLIENT_REQ_GetTpuConfigs,
      &tpu_GetTpuConfigsRequest_msg, &req,
      ctx->tpu_auther.access_token, ctx->tpu_auther.access_token_sz,
      0 /* is_streaming */
  );
  if( FD_UNLIKELY( !request ) ) return;
  fd_grpc_client_deadline_set(
      request,
      FD_GRPC_DEADLINE_RX_END,
      fd_log_wallclock() + FD_BUNDLE_CLIENT_REQUEST_TIMEOUT );

  ctx->tpu_config_wait = 1;
}

static void
fd_bundle_tpu_client_subscribe_packets( fd_bundle_tile_t * ctx ) {
  if( FD_UNLIKELY( fd_grpc_client_request_is_blocked( ctx->tpu_grpc_client ) ) ) return;

  tpu_SubscribePacketsRequest req = tpu_SubscribePacketsRequest_init_default;
  static char const path[] = "/relayer.Relayer/SubscribePackets";
  fd_grpc_h2_stream_t * request = fd_grpc_client_request_start(
      ctx->tpu_grpc_client,
      path, sizeof(path)-1,
      FD_BUNDLE_CLIENT_REQ_SubscribePacketsTPU,
      &tpu_SubscribePacketsRequest_msg, &req,
      ctx->tpu_auther.access_token, ctx->tpu_auther.access_token_sz,
      0 /* is_streaming */
  );
  if( FD_UNLIKELY( !request ) ) return;
  fd_grpc_client_deadline_set(
      request,
      FD_GRPC_DEADLINE_HEADER,
      fd_log_wallclock() + FD_BUNDLE_CLIENT_REQUEST_TIMEOUT );

  ctx->tpu_packet_subscription_wait = 1;
}

void
fd_bundle_tpu_client_send_ping( fd_bundle_tile_t * ctx ) {
  if( FD_UNLIKELY( !ctx->tpu_grpc_client ) ) return;
  fd_h2_conn_t * conn = fd_grpc_client_h2_conn( ctx->tpu_grpc_client );
  if( FD_UNLIKELY( !conn ) ) return;
  if( FD_UNLIKELY( conn->flags ) ) return;
  fd_h2_rbuf_t * rbuf_tx = fd_grpc_client_rbuf_tx( ctx->tpu_grpc_client );
  if( FD_UNLIKELY( !rbuf_tx ) ) return;

  if( FD_LIKELY( fd_h2_tx_ping( conn, rbuf_tx ) ) ) {
    long now = fd_bundle_now();
    fd_keepalive_tx( ctx->tpu_keepalive, ctx->rng, now );
    FD_LOG_DEBUG(( "TPU Keepalive TX (deadline=+%gs)", (double)( ctx->tpu_keepalive->ts_deadline-now )/1e9 ));
  }
}

static int
fd_bundle_tpu_client_step_reconnect( fd_bundle_tile_t * ctx,
                                     long               now ) {
  /* Drive auth for TPU connection */
  if( FD_UNLIKELY( ctx->tpu_auther.needs_poll ) ) {
    fd_bundle_auther_poll( &ctx->tpu_auther, ctx->tpu_grpc_client, ctx->keyguard_client );
    return 1;
  }
  if( FD_UNLIKELY( ctx->tpu_auther.state!=FD_BUNDLE_AUTH_STATE_DONE_WAIT ) ) return 0;

  /* Request TPU configs (periodically refresh) */
  int const tpu_config_expired = ( ctx->tpu_config_valid_until - now )<0;
  if( FD_UNLIKELY( ( ( !ctx->tpu_config_avail ) |
                     ( tpu_config_expired     ) ) &
                   ( !ctx->tpu_config_wait      ) ) ) {
    fd_bundle_tpu_client_request_tpu_configs( ctx );
    return 1;
  }

  /* Subscribe to packets on TPU connection */
  if( FD_UNLIKELY( !ctx->tpu_packet_subscription_live && !ctx->tpu_packet_subscription_wait ) ) {
    fd_bundle_tpu_client_subscribe_packets( ctx );
    return 1;
  }

  /* Send a PING */
  if( FD_UNLIKELY( fd_keepalive_should_tx( ctx->tpu_keepalive, now ) ) ) {
    fd_bundle_tpu_client_send_ping( ctx );
    return 1;
  }

  return 0;
}

static void
fd_bundle_tpu_client_step1( fd_bundle_tile_t * ctx,
                            int *              charge_busy ) {

  /* Wait for TCP socket to connect */
  if( FD_UNLIKELY( !ctx->tpu_tcp_sock_connected ) ) {
    if( FD_UNLIKELY( ctx->tpu_tcp_sock < 0 ) ) goto reconnect_tpu;

    struct pollfd pfds[1] = {
      { .fd = ctx->tpu_tcp_sock, .events = POLLOUT }
    };
    int poll_res = fd_syscall_poll( pfds, 1, 0 );
    if( FD_UNLIKELY( poll_res<0 ) ) {
      FD_LOG_ERR(( "fd_syscall_poll(tpu_tcp_sock) failed (%i-%s)", errno, fd_io_strerror( errno ) ));
    }
    if( poll_res==0 ) return;

    if( pfds[0].revents & (POLLERR|POLLHUP) ) {
      int connect_err = fd_bundle_tpu_client_do_connect( ctx, 0 );
      FD_LOG_INFO(( "Bundle gRPC TPU endpoint connect attempt failed (%i-%s)", connect_err, fd_io_strerror( connect_err ) ));
      fd_bundle_tpu_client_reset( ctx );
      ctx->metrics.transport_fail_cnt++;
      *charge_busy = 1;
      return;
    }
    if( pfds[0].revents & POLLOUT ) {
      FD_LOG_DEBUG(( "Bundle TCP TPU socket connected" ));
      ctx->tpu_tcp_sock_connected = 1;
      *charge_busy = 1;
      return;
    }
    return;
  }

  /* gRPC conn died? */
  if( FD_UNLIKELY( !ctx->tpu_grpc_client ) ) {
    long sleep_start;
  reconnect_tpu:
    sleep_start = fd_bundle_now();
    if( FD_UNLIKELY( sleep_start < ctx->tpu_backoff_until ) ) {
      long wait_dur = ctx->tpu_backoff_until - sleep_start;
      fd_log_sleep( fd_long_min( wait_dur, 1e6 ) );
      return;
    }
    fd_bundle_tpu_client_create_conn( ctx );
    *charge_busy = 1;
    return;
  }

  /* Did a HTTP/2 PING time out */
  long check_ts = ctx->tpu_cached_ts = fd_bundle_now();
  if( FD_UNLIKELY( fd_keepalive_is_timeout( ctx->tpu_keepalive, check_ts ) ) ) {
    FD_LOG_WARNING(( "Bundle gRPC TPU endpoint timed out (HTTP/2 PING went unanswered for %.2f seconds)",
                     (double)( check_ts - ctx->tpu_keepalive->ts_last_tx )/1e9 ));
    ctx->tpu_keepalive->inflight = 0;
    ctx->tpu_defer_reset = 1;
    *charge_busy = 1;
    return;
  }

  /* Drive I/O, SSL handshake, and any inflight requests */
  if( FD_UNLIKELY( !fd_bundle_tpu_client_drive_io( ctx, charge_busy ) ||
                   ctx->tpu_defer_reset ) ) {
    fd_bundle_tpu_client_reset( ctx );
    ctx->metrics.transport_fail_cnt++;
    *charge_busy = 1;
    return;
  }

  /* Are we ready to issue a new request? */
  if( FD_UNLIKELY( fd_grpc_client_request_is_blocked( ctx->tpu_grpc_client ) ) ) return;
  long io_ts = fd_bundle_now();
  if( FD_UNLIKELY( io_ts < ctx->tpu_backoff_until ) ) return;

  *charge_busy |= fd_bundle_tpu_client_step_reconnect( ctx, io_ts );
}

void
fd_bundle_tpu_client_step( fd_bundle_tile_t * ctx,
                           int *              charge_busy ) {
  fd_bundle_tpu_client_step1( ctx, charge_busy );
}

/* ========== Callbacks for TPU endpoint gRPC client ========== */

static void
fd_bundle_tpu_client_grpc_conn_established( void * app_ctx ) {
  (void)app_ctx;
  FD_LOG_INFO(( "TPU endpoint gRPC connection established" ));
}

static void
fd_bundle_tpu_client_grpc_conn_dead( void * app_ctx,
                                     uint   h2_err,
                                     int    closed_by ) {
  fd_bundle_tile_t * ctx = app_ctx;
  FD_LOG_INFO(( "TPU endpoint gRPC connection closed %s (%u-%s)",
                closed_by ? "by peer" : "locally",
                h2_err, fd_h2_strerror( h2_err ) ));
  ctx->tpu_defer_reset = 1;
}

static void
fd_bundle_tpu_client_grpc_tx_complete( void * app_ctx,
                                       ulong  request_ctx ) {
  (void)app_ctx;
  (void)request_ctx;
}

static void
fd_bundle_tpu_client_grpc_rx_start( void * app_ctx,
                                    ulong  request_ctx ) {
  fd_bundle_tile_t * ctx = app_ctx;
  switch( request_ctx ) {
  case FD_BUNDLE_CLIENT_REQ_SubscribePacketsTPU:
    ctx->tpu_packet_subscription_live = 1;
    ctx->tpu_packet_subscription_wait = 0;
    FD_LOG_INFO(( "TPU SubscribePackets stream started" ));
    break;
  default:
    break;
  }
}

/* Handle a GetTpuConfigsResponse from the TPU endpoint. */
static void
fd_bundle_tpu_client_handle_tpu_configs(
    fd_bundle_tile_t * ctx,
    pb_istream_t *     istream
) {
  tpu_GetTpuConfigsResponse res = tpu_GetTpuConfigsResponse_init_default;
  if( FD_UNLIKELY( !pb_decode( istream, &tpu_GetTpuConfigsResponse_msg, &res ) ) ) {
    ctx->metrics.decode_fail_cnt++;
    FD_LOG_WARNING(( "Protobuf decode of (relayer.GetTpuConfigsResponse) failed" ));
    return;
  }

  /* Parse TPU address from string IP + port */
  uint tpu_ip4_addr = 0;
  if( res.has_tpu && res.tpu.ip.size>0 ) {
    /* Null-terminate the IP string */
    char ip_str[65];
    ulong len = fd_ulong_min( res.tpu.ip.size, sizeof(ip_str)-1 );
    fd_memcpy( ip_str, res.tpu.ip.bytes, len );
    ip_str[len] = '\0';
    fd_cstr_to_ip4_addr( ip_str, &tpu_ip4_addr );
  }

  uint tpu_fwd_ip4_addr = 0;
  if( res.has_tpu_forward && res.tpu_forward.ip.size>0 ) {
    char ip_str[65];
    ulong len = fd_ulong_min( res.tpu_forward.ip.size, sizeof(ip_str)-1 );
    fd_memcpy( ip_str, res.tpu_forward.ip.bytes, len );
    ip_str[len] = '\0';
    fd_cstr_to_ip4_addr( ip_str, &tpu_fwd_ip4_addr );
  }

  ctx->tpu_config_tpu_ip4_addr     = tpu_ip4_addr;
  ctx->tpu_config_tpu_port         = res.has_tpu ? (ushort)res.tpu.port : 0;
  ctx->tpu_config_tpu_fwd_ip4_addr = tpu_fwd_ip4_addr;
  ctx->tpu_config_tpu_fwd_port     = res.has_tpu_forward ? (ushort)res.tpu_forward.port : 0;
  ctx->tpu_config_avail            = 1;
  ctx->tpu_config_valid_until      = fd_bundle_now() + (long)60e9;  /* 60 second TTL */

  FD_LOG_INFO(( "TPU configs: tpu=" FD_IP4_ADDR_FMT ":%u, tpu_fwd=" FD_IP4_ADDR_FMT ":%u",
                FD_IP4_ADDR_FMT_ARGS( tpu_ip4_addr ), ctx->tpu_config_tpu_port,
                FD_IP4_ADDR_FMT_ARGS( tpu_fwd_ip4_addr ), ctx->tpu_config_tpu_fwd_port ));
}

static void
fd_bundle_tpu_client_grpc_rx_msg( void *       app_ctx,
                                  void const * protobuf,
                                  ulong        protobuf_sz,
                                  ulong        request_ctx ) {
  fd_bundle_tile_t * ctx = app_ctx;
  pb_istream_t istream = pb_istream_from_buffer( protobuf, protobuf_sz );

  switch( request_ctx ) {
  case FD_BUNDLE_CLIENT_REQ_Auth_GenerateAuthChallenge:
  if( FD_UNLIKELY( !fd_bundle_auther_handle_challenge_resp( &ctx->tpu_auther, protobuf, protobuf_sz ) ) ) {
    ctx->metrics.decode_fail_cnt++;
  }
  break;
case FD_BUNDLE_CLIENT_REQ_Auth_GenerateAuthTokens:
  if( FD_UNLIKELY( !fd_bundle_auther_handle_tokens_resp( &ctx->tpu_auther, protobuf, protobuf_sz ) ) ) {
    ctx->metrics.decode_fail_cnt++;
  }
  break;
  case FD_BUNDLE_CLIENT_REQ_SubscribePacketsTPU:
    /* Handle packets from TPU endpoint */
    fd_bundle_tpu_client_handle_packet_batch( ctx, &istream );
    break;
  case FD_BUNDLE_CLIENT_REQ_GetTpuConfigs:
    fd_bundle_tpu_client_handle_tpu_configs( ctx, &istream );
    break;
  default:
    FD_LOG_WARNING(( "Unexpected RPC response on TPU endpoint (request_ctx=%lu)", request_ctx ));
    break;
  }
}

static void
fd_bundle_tpu_client_request_failed( fd_bundle_tile_t * ctx,
                                     ulong              request_ctx ) {
  fd_bundle_tpu_client_backoff( ctx, fd_bundle_now() );
  switch( request_ctx ) {
  case FD_BUNDLE_CLIENT_REQ_Auth_GenerateAuthChallenge:
  case FD_BUNDLE_CLIENT_REQ_Auth_GenerateAuthTokens:
    fd_bundle_auther_handle_request_fail( &ctx->tpu_auther );
    break;
  }
}

static void
fd_bundle_tpu_client_grpc_rx_end( void *                app_ctx,
                                  ulong                 request_ctx,
                                  fd_grpc_resp_hdrs_t * resp ) {
  fd_bundle_tile_t * ctx = app_ctx;
  /* Handle HTTP-level failures */
  if( FD_UNLIKELY( resp->h2_status!=200 ) ) {
    FD_LOG_WARNING(( "TPU endpoint gRPC request failed (HTTP status %u)", resp->h2_status ));
    fd_bundle_tpu_client_request_failed( ctx, request_ctx );
    return;
  }

  /* Handle gRPC-level failures */
  if( FD_UNLIKELY( resp->grpc_status!=FD_GRPC_STATUS_OK ) ) {
    FD_LOG_INFO(( "TPU endpoint gRPC request failed (gRPC status %u-%s)",
                  resp->grpc_status, fd_grpc_status_cstr( resp->grpc_status ) ));
    fd_bundle_tpu_client_request_failed( ctx, request_ctx );
    if( resp->grpc_status==FD_GRPC_STATUS_UNAUTHENTICATED ||
        resp->grpc_status==FD_GRPC_STATUS_PERMISSION_DENIED ) {
      fd_bundle_auther_reset( &ctx->tpu_auther );
    }
    return;
  }

  switch( request_ctx ) {
  case FD_BUNDLE_CLIENT_REQ_SubscribePacketsTPU:
    ctx->tpu_packet_subscription_wait = 0;
    ctx->tpu_packet_subscription_live = 0;
    break;
  case FD_BUNDLE_CLIENT_REQ_GetTpuConfigs:
    ctx->tpu_config_wait = 0;
    break;
  default:
    break;
  }
}

static void
fd_bundle_tpu_client_grpc_rx_timeout( void * app_ctx,
                                      ulong  request_ctx,
                                      int    deadline_kind ) {
  fd_bundle_tile_t * ctx = app_ctx;
  (void)deadline_kind;

  FD_LOG_WARNING(( "TPU endpoint RPC timeout (request_ctx=%lu)", request_ctx ));

  switch( request_ctx ) {
  case FD_BUNDLE_CLIENT_REQ_Auth_GenerateAuthChallenge:
  case FD_BUNDLE_CLIENT_REQ_Auth_GenerateAuthTokens:
    fd_bundle_auther_handle_request_fail( &ctx->tpu_auther );
    break;
  case FD_BUNDLE_CLIENT_REQ_SubscribePacketsTPU:
    ctx->tpu_packet_subscription_wait = 0;
    break;
  case FD_BUNDLE_CLIENT_REQ_GetTpuConfigs:
    ctx->tpu_config_wait = 0;
    break;
  default:
    break;
  }

  ctx->tpu_defer_reset = 1;
}

static void
fd_bundle_tpu_client_grpc_ping_ack( void * app_ctx ) {
  fd_bundle_tile_t * ctx = app_ctx;
  long rtt_sample = fd_keepalive_rx( ctx->tpu_keepalive, fd_bundle_now() );
  if( FD_LIKELY( rtt_sample ) ) {
    fd_rtt_sample( ctx->tpu_rtt, (float)rtt_sample, 0 );
    FD_LOG_DEBUG(( "TPU Keepalive ACK" ));
  }
}

fd_grpc_client_callbacks_t fd_bundle_tpu_client_grpc_callbacks = {
  .conn_established = fd_bundle_tpu_client_grpc_conn_established,
  .conn_dead        = fd_bundle_tpu_client_grpc_conn_dead,
  .tx_complete      = fd_bundle_tpu_client_grpc_tx_complete,
  .rx_start         = fd_bundle_tpu_client_grpc_rx_start,
  .rx_msg           = fd_bundle_tpu_client_grpc_rx_msg,
  .rx_end           = fd_bundle_tpu_client_grpc_rx_end,
  .rx_timeout       = fd_bundle_tpu_client_grpc_rx_timeout,
  .ping_ack         = fd_bundle_tpu_client_grpc_ping_ack,
};
