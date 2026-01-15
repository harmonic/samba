#ifndef HEADER_fd_src_disco_poh_fd_opticast_h
#define HEADER_fd_src_disco_poh_fd_opticast_h

/* fd_opticast.h - Shared opticast (optimistic broadcast) logic for harmonic blocks.

   Opticast enables broadcasting harmonic block transactions to the network
   *before* local execution completes, enabling the local validator to execute
   at the same time as the rest of the network.

   Both Pack and PoH tiles use the same tspub (message publish timestamp)
   to make coordination-free decisions about entering harmonic mode. This
   shared implementation ensures consistent logic across discof and discoh. */

#include "../pack/fd_pack.h"
#include "../../ballet/bmtree/fd_bmtree.h"
#include "../../ballet/sha256/fd_sha256.h"

/* Opticast state tracking. Both discof and discoh maintain this state. */
struct fd_opticast_state {
  long  harmonic_threshold_ns;   /* Deadline for first block txn to trigger opticast */
  long  harmonic_cutoff_ns;      /* Hard timeout: reject txns arriving after this */
  ulong microblock_cnt;          /* Number of microblocks recorded via opticast */

  /* Harmonic block transactions from resolv must wait until we receive microblock
     index 0 from bank_pack. The PoH tile's before_frag/returnable_frag ensures
     microblocks are processed in order, so the first bank message IS microblock 0.
     This ensures the IB (initializer bundle / crank) is recorded first if one exists.
     This works because either:
       1. Harmonic block is being executed and the first thing back from bank is IB
       2. We went past cutoff and it doesn't matter - IB will be expected before
          any bundle anyway (IB is required to receive tips from bundles) */
  int   bank_received;
};
typedef struct fd_opticast_state fd_opticast_state_t;

FD_PROTOTYPES_BEGIN

/* fd_opticast_init: Initialize opticast state for a new leader slot.
   
   slot_end_ns: The wallclock time when the slot ends.
   
   Computes:
     harmonic_cutoff_ns    = slot_end_ns + FD_PACK_HARMONIC_BUFFER_NS
     harmonic_threshold_ns = harmonic_cutoff_ns - FD_PACK_HARMONIC_DEADLINE_NS */
static inline void
fd_opticast_init( fd_opticast_state_t * state,
                  long                  slot_end_ns ) {
  state->harmonic_cutoff_ns    = slot_end_ns + FD_PACK_HARMONIC_BUFFER_NS;
  state->harmonic_threshold_ns = state->harmonic_cutoff_ns - FD_PACK_HARMONIC_DEADLINE_NS;
  state->microblock_cnt        = 0UL;
  state->bank_received         = 0;
}

/* fd_opticast_should_record: Determine if a harmonic transaction should be recorded.
   
   txn_arrival_ns: The tspub timestamp of the transaction (nanoseconds).
   
   Decision logic (mirrors Pack's logic for coordination-free agreement):
   1. First block txn (microblock_cnt == 0):
      - If tspub < threshold: enter opticast mode, record
      - If tspub >= threshold: timeout, don't record (Pack enters SPRINT)
   2. Subsequent block txns (microblock_cnt > 0):
      - Record until hard cutoff (tspub > harmonic_cutoff_ns)
   
   Returns 1 if should record, 0 otherwise. */
static inline int
fd_opticast_should_record( fd_opticast_state_t const * state,
                           long                        txn_arrival_ns ) {
  /* Hard timeout: message arrived after cutoff */
  if( FD_UNLIKELY( txn_arrival_ns > state->harmonic_cutoff_ns ) ) {
    return 0;
  }
  
  /* Already in opticast mode: accept subsequent transactions */
  if( state->microblock_cnt > 0UL ) {
    return 1;
  }
  
  /* First block txn: check threshold to decide whether to enter opticast mode */
  return txn_arrival_ns < state->harmonic_threshold_ns;
}

/* fd_opticast_compute_merkle_hash: Compute merkle hash from transaction signatures.
   This must match what hash_transactions() in bank tile does for single-txn
   microblocks. The signature(s) are at payload+signature_off, NOT at offset 0!

   payload:        Pointer to transaction payload
   signature_off:  Offset to first signature (from parsed fd_txn_t)
   signature_cnt:  Number of signatures (from parsed fd_txn_t)
   merkle_out:     Output buffer for 32-byte merkle hash */
static inline void
fd_opticast_compute_merkle_hash( uchar const * payload,
                                 ushort        signature_off,
                                 ushort        signature_cnt,
                                 uchar       * merkle_out ) {
  /* Use a stack-allocated bmtree for single-txn hash.
     fd_bmtree_commit requires: 2 * tree_depth * 32 bytes
     For a single leaf, tree_depth=1, so we need 64 bytes minimum. */
  uchar bmtree_mem[ 256 ] __attribute__((aligned(32)));
  fd_bmtree_commit_t * bmtree = fd_bmtree_commit_init( bmtree_mem, 32UL, 1UL, 0UL );

  /* Hash all signatures, matching bank's hash_transactions */
  for( ushort j=0; j<signature_cnt; j++ ) {
    fd_bmtree_node_t node[1];
    fd_bmtree_hash_leaf( node, payload + signature_off + 64UL*j, 64UL, 1UL );
    fd_bmtree_commit_append( bmtree, node, 1UL );
  }

  uchar * root = fd_bmtree_commit_fini( bmtree );
  fd_memcpy( merkle_out, root, 32UL );
}

/* fd_opticast_mixin_hash: Mix a merkle hash into the PoH hash chain.
   
   poh_hash:    Current PoH hash (32 bytes), updated in place
   merkle_hash: Merkle hash to mix in (32 bytes) */
static inline void
fd_opticast_mixin_hash( uchar       * poh_hash,
                        uchar const * merkle_hash ) {
  uchar data[ 64 ];
  fd_memcpy( data, poh_hash, 32UL );
  fd_memcpy( data+32UL, merkle_hash, 32UL );
  fd_sha256_hash( data, 64UL, poh_hash );
}

/* fd_opticast_mixin: Record a harmonic block transaction to PoH.
   This is the shared implementation used by both discof and discoh.
   As such, it has a bunch of parameters that are passed by pointer
   which would normally live in the respective context structs.

   Parameters (passed by pointer so they can be updated):
   - state:                 opticast state (microblock_cnt is incremented)
   - microblocks_lower_bound: current count, incremented on success
   - max_microblocks:       limit for microblocks
   - poh_hash:              PoH hash (32 bytes), updated with mixin
   - hashcnt:               hash counter, incremented
   - last_hashcnt:          for computing delta
   - hashcnt_per_slot:      for detecting slot boundary
   - slot:                  current slot, may be incremented on boundary
   - last_slot/last_hashcnt_out: updated after mixin

   Parameters (input only):
   - target_slot:           slot from the transaction
   - next_leader_slot:      expected leader slot
   - payload:               transaction payload
   - signature_off:         offset to first signature (from parsed txn)
   - signature_cnt:         number of signatures (from parsed txn)

   Returns:
   - hashcnt_delta on success (> 0)
   - 0 on failure (validation failed)

   Caller is responsible for:
   - Checking source_tpu, leader bank, should_record BEFORE calling
   - Publishing to shred AFTER this returns successfully
   - Handling tick boundary (register tick, state transitions) */
static inline ulong
fd_opticast_mixin( fd_opticast_state_t * state,
                   ulong               * microblocks_lower_bound,
                   ulong                 max_microblocks,
                   uchar               * poh_hash,
                   ulong               * hashcnt,
                   ulong                 last_hashcnt,
                   ulong                 hashcnt_per_slot,
                   ulong               * slot,
                   ulong               * last_slot_out,
                   ulong               * last_hashcnt_out,
                   ulong                 target_slot,
                   ulong                 next_leader_slot,
                   uchar const         * payload,
                   ushort                signature_off,
                   ushort                signature_cnt ) {
  /* Validation */
  if( FD_UNLIKELY( target_slot != next_leader_slot || target_slot != *slot ) ) {
    FD_LOG_WARNING(( "OPTICAST: slot mismatch (target=%lu, next_leader=%lu, current=%lu)",
                     target_slot, next_leader_slot, *slot ));
    return 0UL;
  }

  if( FD_UNLIKELY( *microblocks_lower_bound >= max_microblocks ) ) {
    FD_LOG_WARNING(( "OPTICAST: exceeds max microblocks" ));
    return 0UL;
  }

  /* Update counters */
  *microblocks_lower_bound += 1UL;
  state->microblock_cnt++;

  /* Compute merkle hash and mix into PoH */
  uchar merkle_hash[ 32 ];
  fd_opticast_compute_merkle_hash( payload, signature_off, signature_cnt, merkle_hash );
  fd_opticast_mixin_hash( poh_hash, merkle_hash );

  /* Update hashcnt */
  (*hashcnt)++;
  FD_TEST( *hashcnt > last_hashcnt );
  ulong hashcnt_delta = *hashcnt - last_hashcnt;

  /* Handle slot boundary crossing */
  if( FD_UNLIKELY( !(*hashcnt % hashcnt_per_slot) ) ) {
    (*slot)++;
    *hashcnt = 0UL;
    FD_LOG_WARNING(( "OPTICAST: mixin crossed slot boundary (rare but permissible)" ));
  }

  *last_slot_out    = *slot;
  *last_hashcnt_out = *hashcnt;

  return hashcnt_delta;
}

/* fd_opticast_publish: Publish an opticast microblock to shred tile.
   This is the shared implementation used by both discof and discoh.
   
   Parameters:
   - dst:               Output buffer (from fd_chunk_to_laddr)
   - slot:              Current slot
   - reset_slot:        Slot when PoH was last reset
   - hashcnt:           Current hash count
   - hashcnt_per_tick:  Hashes per tick
   - ticks_per_slot:    Ticks per slot
   - hashcnt_delta:     Delta from fd_opticast_mixin
   - poh_hash:          Current PoH hash (32 bytes)
   - parent_slot:       Parent slot number
   - parent_block_id:   Parent block ID (32 bytes)
   - payload:           Transaction payload
   - payload_sz:        Size of payload
   
   Returns: Total size of the published entry batch */
static inline ulong
fd_opticast_publish( uchar             * dst,
                     ulong               slot,
                     ulong               reset_slot,
                     ulong               hashcnt,
                     ulong               hashcnt_per_tick,
                     ulong               ticks_per_slot,
                     ulong               hashcnt_delta,
                     uchar const       * poh_hash,
                     ulong               parent_slot,
                     uchar const       * parent_block_id,
                     uchar const       * payload,
                     ulong               payload_sz ) {
  FD_TEST( slot >= reset_slot );
  
  fd_entry_batch_meta_t * meta = (fd_entry_batch_meta_t *)dst;
  ulong parent_offset = 1UL + slot - reset_slot;
  meta->parent_offset = parent_offset;
  meta->reference_tick = (hashcnt / hashcnt_per_tick) % ticks_per_slot;
  meta->block_complete = !hashcnt;
  
  /* Check if parent block ID is valid based on slot relationship */
  meta->parent_block_id_valid = (parent_slot == (slot - parent_offset));
  if( FD_LIKELY( meta->parent_block_id_valid ) ) {
    fd_memcpy( meta->parent_block_id, parent_block_id, 32UL );
  }
  
  dst += sizeof(fd_entry_batch_meta_t);
  fd_entry_batch_header_t * header = (fd_entry_batch_header_t *)dst;
  header->hashcnt_delta = hashcnt_delta;
  fd_memcpy( header->hash, poh_hash, 32UL );
  header->txn_cnt = 1UL;  /* Single transaction per opticast mixin */
  
  dst += sizeof(fd_entry_batch_header_t);
  fd_memcpy( dst, payload, payload_sz );
  
  return sizeof(fd_entry_batch_meta_t) + sizeof(fd_entry_batch_header_t) + payload_sz;
}

FD_PROTOTYPES_END

#endif /* HEADER_fd_src_disco_poh_fd_opticast_h */

