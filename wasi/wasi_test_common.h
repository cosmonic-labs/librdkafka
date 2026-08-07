/*
 * Shared configuration helper for the WASI test components.
 *
 * The tests take their broker settings from the environment so one binary
 * covers the PLAINTEXT, SASL_PLAINTEXT and SSL runs. Every rd_kafka_conf_t a
 * test builds must go through apply_security_conf(), including short-lived
 * internal ones — a seed producer that skips it cannot authenticate and the
 * test fails for a reason that has nothing to do with what it is testing.
 *
 *   BROKERS            bootstrap servers
 *   SECURITY_PROTOCOL  e.g. SSL, SASL_PLAINTEXT
 *   SASL_MECHANISM / SASL_USERNAME / SASL_PASSWORD
 *   SSL_CA_LOCATION    CA file path inside a wasmtime preopen
 */
#ifndef _WASI_TEST_COMMON_H_
#define _WASI_TEST_COMMON_H_

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "rdkafka.h"

static const char *wasi_env_or(const char *name, const char *fallback) {
        const char *v = getenv(name);
        return (v && *v) ? v : fallback;
}

/**
 * @brief Copy one environment variable into \p conf as \p key, if set.
 * @returns 0 on success (including "not set"), -1 if librdkafka rejected it.
 */
static int wasi_conf_set_env(rd_kafka_conf_t *conf,
                             const char *key,
                             const char *env,
                             char *errstr,
                             size_t errstr_size) {
        const char *v = getenv(env);

        if (!v || !*v)
                return 0;

        if (rd_kafka_conf_set(conf, key, v, errstr, errstr_size) !=
            RD_KAFKA_CONF_OK) {
                printf("[conf] %s rejected: %s\n", key, errstr);
                return -1;
        }

        printf("[conf] %s=%s\n", key,
               strstr(key, "password") ? "***" : v);
        return 0;
}

/**
 * @brief Apply bootstrap.servers plus whatever security settings the
 *        environment specifies.
 * @returns 0 on success, -1 on a rejected value.
 */
static int apply_security_conf(rd_kafka_conf_t *conf,
                               const char *brokers,
                               char *errstr,
                               size_t errstr_size) {
        if (rd_kafka_conf_set(conf, "bootstrap.servers", brokers, errstr,
                              errstr_size) != RD_KAFKA_CONF_OK) {
                printf("[conf] bootstrap.servers rejected: %s\n", errstr);
                return -1;
        }

        return wasi_conf_set_env(conf, "security.protocol", "SECURITY_PROTOCOL",
                                 errstr, errstr_size) ||
                       wasi_conf_set_env(conf, "sasl.mechanism",
                                         "SASL_MECHANISM", errstr,
                                         errstr_size) ||
                       wasi_conf_set_env(conf, "sasl.username", "SASL_USERNAME",
                                         errstr, errstr_size) ||
                       wasi_conf_set_env(conf, "sasl.password", "SASL_PASSWORD",
                                         errstr, errstr_size) ||
                       wasi_conf_set_env(conf, "ssl.ca.location",
                                         "SSL_CA_LOCATION", errstr,
                                         errstr_size)
                   ? -1
                   : 0;
}

#endif /* _WASI_TEST_COMMON_H_ */
