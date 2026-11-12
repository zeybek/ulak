/**
 * @file nats_callback.c
 * @brief NATS error classification.
 */

#include "nats_dispatcher.h"
#include "nats_internal.h"

/**
 * @brief Classify a NATS error as retryable or permanent.
 * @param status NATS status code
 * @return "[PERMANENT]" or "[RETRYABLE]" string constant
 */
const char *nats_classify_error(natsStatus status) {
    switch (status) {
    /* Permanent errors -- no point retrying */
    case NATS_CONNECTION_AUTH_FAILED:
    case NATS_NOT_PERMITTED:
    case NATS_INVALID_ARG:
    case NATS_MAX_PAYLOAD:
    case NATS_PROTOCOL_ERROR:
        return "[PERMANENT]";

    /* Retryable errors -- transient failures */
    case NATS_TIMEOUT:
    case NATS_CONNECTION_CLOSED:
    case NATS_CONNECTION_DISCONNECTED:
    case NATS_NO_SERVER:
    case NATS_STALE_CONNECTION:
    case NATS_IO_ERROR:
    case NATS_SLOW_CONSUMER:
    default:
        return "[RETRYABLE]";
    }
}

/**
 * @brief Classify a JetStream error code as retryable or permanent.
 * @param js_err_code JetStream error code
 * @return "[PERMANENT]" or "[RETRYABLE]" string constant
 */
/**
 * @brief Classify a failed JetStream publish (sync or async).
 *
 * "No responders" means no stream captures the subject (or JetStream is not
 * running). A synchronous js_PublishMsg reports it as JSNotEnabledErr (10076),
 * an async PubAck as NATS_NO_RESPONDERS with no code; both are retryable, so
 * the message waits for the stream instead of going to the DLQ on one path
 * and being retried on the other.
 *
 * THREAD SAFETY: called from the cnats AckHandler thread; constants only.
 *
 * @param status natsStatus of the publish
 * @param js_err_code JetStream error code (0 if none)
 * @return "[PERMANENT]" or "[RETRYABLE]" string constant
 */
const char *nats_classify_publish_error(natsStatus status, int js_err_code) {
    if (status == NATS_NO_RESPONDERS)
        return "[RETRYABLE]";
    return (js_err_code != 0) ? nats_classify_js_error(js_err_code) : nats_classify_error(status);
}

const char *nats_classify_js_error(int js_err_code) {
    switch (js_err_code) {
    /* Permanent JetStream errors */
    case 10059: /* JSStreamNotFoundErr */
    case 10076: /* JSNotEnabledErr */
    case 10003: /* JSBadRequestErr */
    case 10025: /* JSStreamNameExistErr */
        return "[PERMANENT]";

    /* Retryable JetStream errors */
    case 10023: /* JSInsufficientResourcesErr */
    case 10074: /* JSClusterNoPeersErr */
    case 10077: /* JSStreamStoreFailedErr */
    default:
        return "[RETRYABLE]";
    }
}
