/**
 * @file wake.h
 * @brief Commit-time wake-up of delivery workers (replaces per-insert NOTIFY).
 */
#ifndef ULAK_WAKE_H
#define ULAK_WAKE_H

#include "fmgr.h"
#include "postgres.h"

/**
 * @brief Record that the current transaction queued a message.
 *
 * The matching worker latch (or every latch when @p all_partitions is set)
 * is set from a transaction callback once the commit is visible. When
 * ulak.wake_notify is on, a throttled NOTIFY 'ulak_new_msg' is queued too.
 *
 * @param message_id     Queue row id (partition key for unkeyed messages).
 * @param ordering_key   Ordering key or NULL; keyed messages hash to a partition.
 * @param all_partitions Wake every worker (batch inserts, unknown rows).
 */
extern void ulak_wake_mark(int64 message_id, const char *ordering_key, bool all_partitions);

/**
 * @brief Make the queue trigger's ulak._wake_workers() call a no-op.
 *
 * ulak_send() inserts one row and knows its id, so it marks the exact
 * partition itself instead of letting the statement trigger wake all workers.
 * Reset automatically at transaction end.
 */
extern void ulak_wake_set_skip_trigger(bool skip);

/** SQL entry point: ulak._wake_workers(p_id bigint, p_ordering_key text). */
extern Datum ulak_wake_workers_sql(PG_FUNCTION_ARGS);

#endif /* ULAK_WAKE_H */
