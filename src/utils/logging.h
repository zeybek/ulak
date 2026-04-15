/**
 * @file logging.h
 * @brief Logging infrastructure with GUC-controlled verbosity.
 *
 * Clean Architecture: Infrastructure Layer.
 * Provides level-filtered logging functions (debug, info, warning, error)
 * that map to PostgreSQL's elog/ereport system. The active level is
 * controlled by the ulak.log_level GUC parameter.
 */

#ifndef ULAK_UTILS_LOGGING_H
#define ULAK_UTILS_LOGGING_H

#include "config/guc.h"
#include "ulak.h"

/* Use LogLevel from config module to avoid duplication */

/** @name Logging functions */
/** @{ */

/**
 * @brief Log a message at the specified level.
 * @param level    Log level string ("debug", "info", "warning", "error").
 * @param message  printf-style format string.
 * @param ...      Format arguments.
 */
extern void ulak_log(const char *level, const char *message, ...) pg_attribute_printf(2, 3);

/**
 * @brief Log a debug-level message.
 * @param message  printf-style format string.
 * @param ...      Format arguments.
 */
extern void ulak_log_debug(const char *message, ...) pg_attribute_printf(1, 2);

/**
 * @brief Log an info-level message.
 * @param message  printf-style format string.
 * @param ...      Format arguments.
 */
extern void ulak_log_info(const char *message, ...) pg_attribute_printf(1, 2);

/**
 * @brief Log a warning-level message.
 * @param message  printf-style format string.
 * @param ...      Format arguments.
 */
extern void ulak_log_warning(const char *message, ...) pg_attribute_printf(1, 2);

/**
 * @brief Log an error-level message.
 * @param message  printf-style format string.
 * @param ...      Format arguments.
 */
extern void ulak_log_error(const char *message, ...) pg_attribute_printf(1, 2);
/** @} */

/** @name Log level checking */
/** @{ */

/**
 * @brief Check if a message at the given level should be logged.
 * @param level  Log level to check against current GUC setting.
 * @return true if messages at this level are enabled.
 */
extern bool ulak_should_log(LogLevel level);
/** @} */

/** @name Log level conversion */
/** @{ */

/**
 * @brief Convert a LogLevel enum to its string representation.
 * @param level  Log level enum value.
 * @return Static string: "debug", "info", "warning", or "error".
 */
extern const char *ulak_log_level_to_string(LogLevel level);

/**
 * @brief Parse a log level string to its enum value.
 * @param str  Log level name ("debug", "info", "warning", "error").
 * @return Corresponding LogLevel enum value.
 */
extern LogLevel ulak_string_to_log_level(const char *str);
/** @} */

/** @brief External configuration access for the current log level GUC. */
extern int ulak_log_level;

#endif /* ULAK_UTILS_LOGGING_H */
