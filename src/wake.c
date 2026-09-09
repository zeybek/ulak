/**
 * @file wake.c
 * @brief Wake delivery workers when a transaction that queued messages commits.
 *
 * Every INSERT into ulak.queue used to NOTIFY 'ulak_new_msg'. NOTIFY is only
 * delivered at commit, and to keep notifications in commit order PostgreSQL
 * serialises every notifying transaction behind one database-wide
 * AccessExclusiveLock from PreCommit_Notify() until the commit is complete
 * (src/backend/commands/async.c). With many concurrent senders that lock caps
 * the notifying commits at a few thousand per second no matter how many
 * clients there are, and their commit latency grows linearly with the client
 * count. Sending a message therefore capped the application's own commit rate.
 *
 * The extension does not need NOTIFY: the workers' latches live in shared
 * memory (PGPROC), so a sender records which partitions received rows and a
 * transaction callback sets those latches once the commit is visible —
 * CallXactCallbacks(XACT_EVENT_COMMIT) runs after RecordTransactionCommit()
 * and ProcArrayEndTransaction(). No lock, no queue, and a busy worker costs
 * nothing (SetLatch only signals a process that is actually sleeping).
 *
 * NOTIFY is still queued when ulak.wake_notify is on, for external worker
 * processes that share the queue with the extension and can only LISTEN; it
 * is throttled per session by ulak.notify_throttle_ms.
 */

#include "postgres.h"

#include "access/xact.h"
#include "catalog/pg_collation.h"
#include "commands/async.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/fmgrprotos.h"
#include "utils/timestamp.h"

#include "config/guc.h"
#include "shmem.h"
#include "wake.h"

/* Partitions that received rows in the current transaction (bit = worker_id) */
static uint32 pending_mask = 0;
/* Set by ulak_send() while its INSERT runs; see ulak_wake_set_skip_trigger() */
static bool skip_trigger = false;
static bool callback_registered = false;
/* Last NOTIFY queued by this backend (ulak.wake_notify throttle) */
static TimestampTz last_notify_at = 0;

/**
 * @brief Transaction callback: set the marked workers' latches after commit.
 */
static void wake_xact_callback(XactEvent event, void *arg) {
    (void)arg;

    switch (event) {
    case XACT_EVENT_COMMIT:
    case XACT_EVENT_PARALLEL_COMMIT:
        if (pending_mask != 0)
            ulak_wake_workers(MyDatabaseId, pending_mask);
        pending_mask = 0;
        skip_trigger = false;
        break;

    case XACT_EVENT_ABORT:
    case XACT_EVENT_PARALLEL_ABORT:
    case XACT_EVENT_PREPARE:
        /*
         * Nothing became visible. For PREPARE the rows appear at COMMIT
         * PREPARED, possibly from another backend; ulak.poll_interval covers
         * them. (NOTIFY refuses PREPARE outright, so this is still a gain.)
         */
        pending_mask = 0;
        skip_trigger = false;
        break;

    default:
        break;
    }
}

/**
 * @brief Same partition formula as the claim query in batch_processor.c:
 *        (ordering_key IS NULL ? id : abs(hashtext(ordering_key))) % workers.
 */
static int wake_partition_of(int64 message_id, const char *ordering_key) {
    int workers = ulak_workers > 0 ? ulak_workers : 1;
    int64 value;

    if (ordering_key == NULL) {
        value = message_id;
    } else {
        /* hashtext needs a collation; the column uses the database default */
        int32 hash = DatumGetInt32(DirectFunctionCall1Coll(hashtext, DEFAULT_COLLATION_OID,
                                                           CStringGetTextDatum(ordering_key)));
        value = hash < 0 ? -(int64)hash : (int64)hash;
    }
    if (value < 0)
        value = -value;

    return (int)(value % workers);
}

/**
 * @brief Queue a NOTIFY for external LISTENers, at most once per throttle window.
 */
static void wake_maybe_notify(void) {
    TimestampTz now = GetCurrentTimestamp();

    if (ulak_notify_throttle_ms > 0 && last_notify_at != 0 &&
        now < TimestampTzPlusMilliseconds(last_notify_at, ulak_notify_throttle_ms))
        return;

    last_notify_at = now;
    Async_Notify("ulak_new_msg", NULL);
}

void ulak_wake_mark(int64 message_id, const char *ordering_key, bool all_partitions) {
    int workers = ulak_workers > 0 ? ulak_workers : 1;

    if (!callback_registered) {
        RegisterXactCallback(wake_xact_callback, NULL);
        callback_registered = true;
    }

    if (all_partitions || workers >= 32)
        pending_mask |= (workers >= 32) ? 0xFFFFFFFFu : ((1u << workers) - 1u);
    else
        pending_mask |= 1u << wake_partition_of(message_id, ordering_key);

    if (ulak_wake_notify)
        wake_maybe_notify();
}

void ulak_wake_set_skip_trigger(bool skip) { skip_trigger = skip; }

PG_FUNCTION_INFO_V1(ulak_wake_workers_sql);
Datum ulak_wake_workers_sql(PG_FUNCTION_ARGS) {
    if (skip_trigger)
        PG_RETURN_VOID();

    if (PG_ARGISNULL(0)) {
        ulak_wake_mark(0, NULL, true);
    } else {
        char *key = PG_ARGISNULL(1) ? NULL : text_to_cstring(PG_GETARG_TEXT_PP(1));

        ulak_wake_mark(PG_GETARG_INT64(0), key, false);
        if (key != NULL)
            pfree(key);
    }

    PG_RETURN_VOID();
}
