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
