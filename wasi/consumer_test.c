/*
 * Consumer-group smoke test for the single-threaded (cooperative) WASI build.
 *
 * Covers the parts most likely to break under cooperative scheduling, because
 * they interleave at pump points rather than running on their own thread:
 * coordinator discovery, JoinGroup/SyncGroup, the rebalance callback,
 * heartbeats, Fetch, OffsetCommit, and LeaveGroup on close.
 *
 * Seeds its own messages first so the test is self-contained.
 *
 *   BROKERS   bootstrap servers   (default 127.0.0.1:9092)
 *   TOPIC     topic, needs >= 1 partition  (default wasi-consumer-topic)
 *   GROUP     consumer group id   (default wasi-consumer-group)
 *
 * Exits 0 only if every seeded message came back and partitions were assigned.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "rdkafka.h"
#include "wasi_test_common.h"

#define SEED_COUNT 6

static int assigned = 0, revoked = 0, consumed = 0;

static void rebalance_cb(rd_kafka_t *rk,
                         rd_kafka_resp_err_t err,
                         rd_kafka_topic_partition_list_t *parts,
                         void *opaque) {
        int i;

        if (err == RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS) {
                printf("[rebalance] ASSIGN %d partition(s)\n", parts->cnt);
                for (i = 0; i < parts->cnt; i++)
                        printf("            %s [%" PRId32 "]\n",
                               parts->elems[i].topic,
                               parts->elems[i].partition);
                assigned += parts->cnt;
                rd_kafka_assign(rk, parts);
        } else {
                printf("[rebalance] REVOKE %d partition(s)\n", parts->cnt);
                revoked += parts->cnt;
                rd_kafka_assign(rk, NULL);
        }
}

static int produce_seed(const char *brokers, const char *topic, int n) {
        char errstr[512];
        rd_kafka_conf_t *conf = rd_kafka_conf_new();
        rd_kafka_t *p;
        int i;

        /* The seed producer needs the same security settings as the consumer:
         * on a SASL or SSL listener an unconfigured producer cannot connect,
         * and the test would fail for a reason unrelated to consumer groups. */
        if (apply_security_conf(conf, brokers, errstr, sizeof(errstr))) {
                rd_kafka_conf_destroy(conf);
                return -1;
        }

        p = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
        if (!p) {
                printf("[seed] producer FAILED: %s\n", errstr);
                return -1;
        }

        for (i = 0; i < n; i++) {
                char val[64];
                snprintf(val, sizeof(val), "wasi-msg-%d", i);
                rd_kafka_producev(p, RD_KAFKA_V_TOPIC(topic),
                                  RD_KAFKA_V_VALUE(val, strlen(val)),
                                  RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
                                  RD_KAFKA_V_END);
        }

        rd_kafka_flush(p, 10000);
        rd_kafka_destroy(p);
        printf("[seed] produced %d message(s)\n", n);
        return 0;
}

int main(void) {
        char errstr[512];
        const char *brokers = wasi_env_or("BROKERS", "127.0.0.1:9092");
        const char *topic   = wasi_env_or("TOPIC", "wasi-consumer-topic");
        const char *group   = wasi_env_or("GROUP", "wasi-consumer-group");
        rd_kafka_conf_t *conf;
        rd_kafka_t *c;
        rd_kafka_topic_partition_list_t *sub;
        rd_kafka_resp_err_t err;
        int i;

        setvbuf(stdout, NULL, _IONBF, 0);

        if (produce_seed(brokers, topic, SEED_COUNT) != 0)
                return 1;

        conf = rd_kafka_conf_new();
        if (apply_security_conf(conf, brokers, errstr, sizeof(errstr)))
                return 1;
        rd_kafka_conf_set(conf, "group.id", group, errstr, sizeof(errstr));
        rd_kafka_conf_set(conf, "auto.offset.reset", "earliest", errstr,
                          sizeof(errstr));
        rd_kafka_conf_set(conf, "enable.auto.commit", "false", errstr,
                          sizeof(errstr));
        rd_kafka_conf_set_rebalance_cb(conf, rebalance_cb);

        printf("[1] creating consumer, group=%s\n", group);
        c = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
        if (!c) {
                printf("[1] FAILED: %s\n", errstr);
                return 1;
        }
        rd_kafka_poll_set_consumer(c);
        printf("[2] consumer created: %s\n", rd_kafka_name(c));

        sub = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(sub, topic, RD_KAFKA_PARTITION_UA);
        err = rd_kafka_subscribe(c, sub);
        rd_kafka_topic_partition_list_destroy(sub);
        if (err) {
                printf("[3] subscribe FAILED: %s\n", rd_kafka_err2str(err));
                rd_kafka_destroy(c);
                return 1;
        }
        printf("[3] subscribed; joining group...\n");

        for (i = 0; i < 4000 && consumed < SEED_COUNT; i++) {
                rd_kafka_message_t *m = rd_kafka_consumer_poll(c, 50);

                if (!m)
                        continue;

                if (m->err) {
                        if (m->err != RD_KAFKA_RESP_ERR__PARTITION_EOF)
                                printf("[poll] err: %s\n",
                                       rd_kafka_message_errstr(m));
                } else {
                        printf("[consume] %.*s @ %s[%" PRId32
                               "] offset %" PRId64 "\n",
                               (int)m->len, (char *)m->payload,
                               rd_kafka_topic_name(m->rkt), m->partition,
                               m->offset);
                        consumed++;
                }

                rd_kafka_message_destroy(m);
        }

        printf("[4] consumed=%d assigned=%d\n", consumed, assigned);

        err = rd_kafka_commit(c, NULL, 0 /*sync*/);
        printf("[5] commit: %s\n", err ? rd_kafka_err2str(err) : "OK");

        rd_kafka_consumer_close(c);
        printf("[6] consumer closed (revoked=%d)\n", revoked);
        rd_kafka_destroy(c);
        printf("[7] destroyed cleanly\n");

        return consumed >= SEED_COUNT && assigned > 0 && !err ? 0 : 1;
}
