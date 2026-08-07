/*
 * Producer smoke test for the single-threaded (cooperative) WASI build.
 *
 * Exercises the whole client lifecycle against a real broker: connect,
 * ApiVersions, Metadata, the bootstrap -> leader handover, Produce, the
 * delivery report, and a clean destroy.
 *
 * Configured entirely from the environment so one binary covers the PLAINTEXT,
 * SASL_PLAINTEXT and SSL runs rather than needing three builds:
 *
 *   BROKERS            bootstrap servers      (default 127.0.0.1:9092)
 *   TOPIC              topic to produce to    (default wasi-producer-topic)
 *   SECURITY_PROTOCOL  e.g. SSL, SASL_PLAINTEXT   (optional)
 *   SASL_MECHANISM / SASL_USERNAME / SASL_PASSWORD  (optional)
 *   SSL_CA_LOCATION    CA file, inside a wasmtime preopen (optional)
 *
 * Exits 0 only if the broker acknowledged the message.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "rdkafka.h"
#include "wasi_test_common.h"

static int delivered = 0, failed = 0;

static void dr_cb(rd_kafka_t *rk, const rd_kafka_message_t *m, void *opaque) {
        if (m->err) {
                failed++;
                printf("[dr] FAILED: %s\n", rd_kafka_err2str(m->err));
        } else {
                delivered++;
                printf("[dr] delivered partition=%" PRId32 " offset=%" PRId64
                       "\n",
                       m->partition, m->offset);
        }
}

int main(void) {
        char errstr[512];
        const char *brokers = wasi_env_or("BROKERS", "127.0.0.1:9092");
        const char *topic   = wasi_env_or("TOPIC", "wasi-producer-topic");
        rd_kafka_conf_t *conf;
        rd_kafka_t *rk;
        const char *val = "hello-from-single-threaded-librdkafka";
        int i;

        /* Unbuffered: stdout is block-buffered under wasmtime, so a trap would
         * otherwise discard the progress markers that say how far we got. */
        setvbuf(stdout, NULL, _IONBF, 0);


        conf = rd_kafka_conf_new();
        rd_kafka_conf_set_dr_msg_cb(conf, dr_cb);

        if (apply_security_conf(conf, brokers, errstr, sizeof(errstr))) {
                rd_kafka_conf_destroy(conf);
                return 1;
        }

        printf("[1] rd_kafka_new() brokers=%s topic=%s\n", brokers, topic);
        rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
        if (!rk) {
                printf("[1] FAILED: %s\n", errstr);
                return 1;
        }
        printf("[2] client handle created: %s\n", rd_kafka_name(rk));

        if (rd_kafka_producev(rk, RD_KAFKA_V_TOPIC(topic),
                              RD_KAFKA_V_KEY("k1", 2),
                              RD_KAFKA_V_VALUE((void *)val, strlen(val)),
                              RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
                              RD_KAFKA_V_END)) {
                printf("[3] producev FAILED\n");
                rd_kafka_destroy(rk);
                return 1;
        }
        printf("[3] queued\n");

        /* Each poll is a scheduler pump: the handlers only advance while the
         * application is inside librdkafka. */
        for (i = 0; i < 3000 && !delivered && !failed; i++)
                rd_kafka_poll(rk, 10);

        printf("[4] delivered=%d failed=%d\n", delivered, failed);

        rd_kafka_flush(rk, 3000);
        rd_kafka_destroy(rk);
        printf("[5] destroyed cleanly\n");

        return delivered && !failed ? 0 : 1;
}
