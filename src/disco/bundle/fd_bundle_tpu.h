#ifndef HEADER_fd_src_disco_bundle_fd_bundle_tpu_h
#define HEADER_fd_src_disco_bundle_fd_bundle_tpu_h

/* fd_bundle_tpu.h defines the message format for the bundle_gossi link
   which communicates TPU address updates from the bundle tile to gossip.

   In Frankendancer: bundle → poh → Agave
   In full Firedancer: bundle → gossip tile */

#include "../../util/fd_util_base.h"

#define FD_BUNDLE_TPU_UPDATE              (1UL) /* sig value for fd_stem_publish */

#define FD_BUNDLE_TPU_UPDATE_DISCONNECTED (0)
#define FD_BUNDLE_TPU_UPDATE_CONNECTED    (1)

typedef struct {
  int    status;            /* FD_BUNDLE_TPU_UPDATE_* */
  /* TPU address from relayer (valid when status==CONNECTED) */
  uint   tpu_ip4_addr;      /* network byte order */
  ushort tpu_port;          /* host byte order */
  ushort _pad0;
  /* TPU forwards address from relayer (valid when status==CONNECTED) */
  uint   tpu_fwd_ip4_addr;  /* network byte order */
  ushort tpu_fwd_port;      /* host byte order */
  ushort _pad1;
} fd_bundle_tpu_update_t;

#endif /* HEADER_fd_src_disco_bundle_fd_bundle_tpu_h */
