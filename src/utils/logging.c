/**
 * @file logging.c
 * @brief Logging infrastructure implementation
 *
 * Clean Architecture: Infrastructure Layer
 * Provides configurable logging functionality
 */

#include "utils/logging.h"
#include <stdarg.h>
#include <string.h>
#include "config/guc.h"

/** Log Level String Constants */
#define LOG_LEVEL_DEBUG_STR "debug"
#define LOG_LEVEL_INFO_STR "info"
#define LOG_LEVEL_WARNING_STR "warning"
#define LOG_LEVEL_ERROR_STR "error"

/** Maximum log message length */
#define MAX_LOG_MESSAGE_LENGTH 1024

/**
 * @brief Log a message at the specified level using printf-style formatting.
 * @param level Log level string ("debug", "info", "warning", "error")
 * @param message Format string (printf-compatible)
 * @param ... Format arguments
 */
void ulak_log(const char *level, const char *message, ...) {
    LogLevel requested_level;
    va_list args;
    char buffer[MAX_LOG_MESSAGE_LENGTH];

    /* Convert string level to enum */
    if (strcmp(level, LOG_LEVEL_DEBUG_STR) == 0)
        requested_level = LOG_LEVEL_DEBUG;
    else if (strcmp(level, LOG_LEVEL_INFO_STR) == 0)
        requested_level = LOG_LEVEL_INFO;
    else if (strcmp(level, LOG_LEVEL_ERROR_STR) == 0)
        requested_level = LOG_LEVEL_ERROR;
    else
        requested_level = LOG_LEVEL_WARNING; /* "warning" and unknown levels */

    /* Check if we should log this level */
    if (!ulak_should_log(requested_level)) {
        return;
    }

    va_start(args, message);
    vsnprintf(buffer, sizeof(buffer), message, args);
    va_end(args);

    /* Log with appropriate PostgreSQL log level */
    switch (requested_level) {
    case LOG_LEVEL_DEBUG:
        elog(DEBUG1, "[ulak] %s", buffer);
        break;
    case LOG_LEVEL_INFO:
        elog(INFO, "[ulak] %s", buffer);
        break;
    case LOG_LEVEL_WARNING:
        elog(WARNING, "[ulak] %s", buffer);
        break;
    case LOG_LEVEL_ERROR:
        /* Use WARNING instead of ERROR to avoid crashing background worker
         * PostgreSQL's elog(ERROR) throws an exception which terminates the worker */
        elog(WARNING, "[ulak] ERROR: %s", buffer);
        break;
    default:
        elog(WARNING, "[ulak] %s", buffer);
        break;
    }
}

/**
 * @brief Log a debug-level message.
 * @param message Format string (printf-compatible)
 * @param ... Format arguments
 */
void ulak_log_debug(const char *message, ...) {
    va_list args;
    char buffer[MAX_LOG_MESSAGE_LENGTH];

    if (!ulak_should_log(LOG_LEVEL_DEBUG)) {
        return;
    }

    va_start(args, message);
    vsnprintf(buffer, sizeof(buffer), message, args);
    va_end(args);

    elog(DEBUG1, "[ulak] %s", buffer);
}

/**
 * @brief Log an info-level message.
 * @param message Format string (printf-compatible)
 * @param ... Format arguments
 */
void ulak_log_info(const char *message, ...) {
    va_list args;
    char buffer[MAX_LOG_MESSAGE_LENGTH];

    if (!ulak_should_log(LOG_LEVEL_INFO)) {
        return;
    }

    va_start(args, message);
    vsnprintf(buffer, sizeof(buffer), message, args);
    va_end(args);

    elog(INFO, "[ulak] %s", buffer);
}

/**
 * @brief Log a warning-level message.
 * @param message Format string (printf-compatible)
 * @param ... Format arguments
 */
void ulak_log_warning(const char *message, ...) {
    va_list args;
    char buffer[MAX_LOG_MESSAGE_LENGTH];

    if (!ulak_should_log(LOG_LEVEL_WARNING)) {
        return;
    }

    va_start(args, message);
    vsnprintf(buffer, sizeof(buffer), message, args);
    va_end(args);

    elog(WARNING, "[ulak] %s", buffer);
}

/**
 * @brief Log an error-level message (uses WARNING to avoid worker crash).
 * @param message Format string (printf-compatible)
 * @param ... Format arguments
 */
void ulak_log_error(const char *message, ...) {
    va_list args;
    char buffer[MAX_LOG_MESSAGE_LENGTH];

    va_start(args, message);
    vsnprintf(buffer, sizeof(buffer), message, args);
    va_end(args);

    /* Use WARNING instead of ERROR to avoid crashing background worker */
    elog(WARNING, "[ulak] ERROR: %s", buffer);
}

/**
 * @brief Check whether a given log level should be emitted.
 * @param level Log level to check
 * @return true if level is at or above the configured threshold
 */
bool ulak_should_log(LogLevel level) { return level <= (LogLevel)ulak_log_level; }

/**
 * @brief Convert a LogLevel enum to its string representation.
 * @param level Log level enum
 * @return Static string for the level name
 */
const char *ulak_log_level_to_string(LogLevel level) {
    switch (level) {
    case LOG_LEVEL_ERROR:
        return LOG_LEVEL_ERROR_STR;
    case LOG_LEVEL_WARNING:
        return LOG_LEVEL_WARNING_STR;
    case LOG_LEVEL_INFO:
        return LOG_LEVEL_INFO_STR;
    case LOG_LEVEL_DEBUG:
        return LOG_LEVEL_DEBUG_STR;
    default:
        return LOG_LEVEL_WARNING_STR;
    }
}

/**
 * @brief Convert a string to a LogLevel enum.
 * @param str Log level name string
 * @return Corresponding LogLevel, or LOG_LEVEL_WARNING for unknown/NULL
 */
LogLevel ulak_string_to_log_level(const char *str) {
    if (!str)
        return LOG_LEVEL_WARNING;

    if (strcmp(str, LOG_LEVEL_ERROR_STR) == 0)
        return LOG_LEVEL_ERROR;
    if (strcmp(str, LOG_LEVEL_WARNING_STR) == 0)
        return LOG_LEVEL_WARNING;
    if (strcmp(str, LOG_LEVEL_INFO_STR) == 0)
        return LOG_LEVEL_INFO;
    if (strcmp(str, LOG_LEVEL_DEBUG_STR) == 0)
        return LOG_LEVEL_DEBUG;

    return LOG_LEVEL_WARNING; /* default */
}
