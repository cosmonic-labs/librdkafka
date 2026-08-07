/*
 * cosmonic:librdkafka/host-plugin — the Kafka capability, backed by librdkafka
 * built with the cooperative (thread-less) WASI scheduler.
 *
 * One long-lived, host-scoped instance serves every workload that binds to it.
 * Two things follow from that and drive the whole design:
 *
 *  - Kafka authorises per principal, so a shared client would hand every
 *    workload the same ACLs. Each workload gets its own rd_kafka_t, opened from
 *    the brokers and credentials delivered to on_workload_bind, and keyed by
 *    workload id. Capability calls carry no binding identity, so calls are
 *    correlated back with wasmcloud:host/identity.
 *
 *  - librdkafka's threads are cooperative tasks here, and they only advance
 *    when something pumps them. Capability calls pump as a side effect, but
 *    between calls nothing would — and a Kafka client that stops running loses
 *    its consumer group to the session timeout. wasi:cli/run is that something.
 */

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "host_plugin.h"
#include "rdkafka.h"
#include "rdwasm.h"

/* ------------------------------------------------------------------------ */
/* Per-workload client registry                                             */
/* ------------------------------------------------------------------------ */

/* Bounded rather than growable: a host serves a known set of workloads, and a
 * fixed table keeps bind/unbind allocation-free on the hot path. */
#define MAX_WORKLOADS 64
#define MAX_ID_LEN    128

struct workload_client {
        char id[MAX_ID_LEN];
        rd_kafka_t *rk;      /* NULL until first use; config is kept from bind */
        rd_kafka_conf_t *conf; /* owned until rd_kafka_new consumes it */
        bool in_use;
};

static struct workload_client g_clients[MAX_WORKLOADS];

static struct workload_client *client_find(const char *id) {
        for (int i = 0; i < MAX_WORKLOADS; i++)
                if (g_clients[i].in_use && !strcmp(g_clients[i].id, id))
                        return &g_clients[i];
        return NULL;
}

static struct workload_client *client_alloc(const char *id) {
        for (int i = 0; i < MAX_WORKLOADS; i++) {
                if (g_clients[i].in_use)
                        continue;
                snprintf(g_clients[i].id, sizeof(g_clients[i].id), "%s", id);
                g_clients[i].rk = NULL;
                g_clients[i].conf = NULL;
                g_clients[i].in_use = true;
                return &g_clients[i];
        }
        return NULL;
}

static void client_release(struct workload_client *c) {
        if (!c)
                return;
        if (c->rk) {
                /* Flush before destroying: queued records are otherwise
                 * discarded, and a workload that got an ack for a record the
                 * broker never saw is the worst outcome here. */
                rd_kafka_flush(c->rk, 5000);
                rd_kafka_destroy(c->rk);
                c->rk = NULL;
        }
        if (c->conf) {
                rd_kafka_conf_destroy(c->conf);
                c->conf = NULL;
        }
        c->in_use = false;
        c->id[0] = '\0';
}

/**
 * @brief The client belonging to the workload currently calling.
 *
 * Identity comes from the host rather than from the call, because a capability
 * call carries no binding information — this is the only thing separating one
 * tenant's traffic from another's.
 */
static struct workload_client *client_for_caller(void) {
        host_plugin_string_t id;
        wasmcloud_host_identity_get_workload_id(&id);
        if (!id.len)
                return NULL;

        char buf[MAX_ID_LEN];
        size_t n = id.len < sizeof(buf) - 1 ? id.len : sizeof(buf) - 1;
        memcpy(buf, id.ptr, n);
        buf[n] = '\0';
        host_plugin_string_free(&id);

        return client_find(buf);
}

/* Open the rd_kafka_t lazily, on first use, from the config captured at bind.
 * Deferred because bind should validate configuration, not pay for a broker
 * connection that a workload may never make. */
static rd_kafka_t *client_handle(struct workload_client *c, char *errstr,
                                 size_t errstr_size) {
        if (!c)
                return NULL;
        if (c->rk)
                return c->rk;
        if (!c->conf) {
                snprintf(errstr, errstr_size, "no configuration bound");
                return NULL;
        }
        /* rd_kafka_new takes ownership of conf on success *and* on failure. */
        c->rk = rd_kafka_new(RD_KAFKA_PRODUCER, c->conf, errstr, errstr_size);
        c->conf = NULL;
        return c->rk;
}

/* ------------------------------------------------------------------------ */
/* Error mapping                                                            */
/* ------------------------------------------------------------------------ */

/* librdkafka's classification is not derivable from the code alone, so carry
 * it across rather than inferring it on the far side. */
static void fill_error(exports_cosmonic_kafka_producer_error_t *out,
                       rd_kafka_resp_err_t code, const char *msg, bool fatal,
                       bool retriable) {
        out->code = (exports_cosmonic_kafka_types_error_code_t)0; /* mapped below */
        host_plugin_string_dup(&out->message, msg ? msg : rd_kafka_err2str(code));
        out->fatal = fatal;
        out->retriable = retriable;
        out->txn_requires_abort = false;
}

/* ------------------------------------------------------------------------ */
/* wasi:cli/run — the scheduler pump                                        */
/* ------------------------------------------------------------------------ */

/**
 * Drive the cooperative scheduler for the life of the plugin.
 *
 * Without this the client only advances while a capability call is in flight.
 * Consumer group heartbeats, metadata refresh and produce retries all happen
 * between calls, so a plugin that only pumps on demand loses its group
 * membership to `session.timeout.ms` and then rejoins on the next call —
 * visible to users as duplicate delivery after every idle period.
 */
host_plugin_callback_code_t exports_wasi_cli_run_run(void) {
        for (;;) {
                rd_wasm_pump();

                /* Yield so the store's executor can serve capability calls
                 * between passes. Without a suspension point this loop starves
                 * the very calls it exists to support. */
                wasi_clocks_monotonic_clock_wait_for(1000000 /* 1ms */);
        }

        /* Not reached; the host stops the plugin. */
}

/**
 * Resumption point for the run loop's suspensions.
 *
 * The loop above suspends on the clock, so the runtime re-enters here when the
 * timer fires. Returning YIELD keeps the task alive; returning EXIT would end
 * the pump and, with it, every bound workload's group membership.
 */
host_plugin_callback_code_t exports_wasi_cli_run_run_callback(
    host_plugin_event_t *event) {
        (void)event;
        rd_wasm_pump();
        return HOST_PLUGIN_CALLBACK_CODE_YIELD;
}

/* ------------------------------------------------------------------------ */
/* wasmcloud:host/workload-lifecycle                                        */
/* ------------------------------------------------------------------------ */

/**
 * A workload is binding. Capture its brokers and credentials.
 *
 * Returning an error here fails the workload's deployment, which is what we
 * want for bad configuration: fail at deploy, not at the first send.
 */
host_plugin_callback_code_t exports_wasmcloud_host_workload_lifecycle_on_workload_bind(
    exports_wasmcloud_host_workload_lifecycle_workload_info_t *workload) {
        exports_wasmcloud_host_workload_lifecycle_result_void_string_t ret;
        char errstr[512];
        char id[MAX_ID_LEN];

        size_t n = workload->id.len < sizeof(id) - 1 ? workload->id.len
                                                     : sizeof(id) - 1;
        memcpy(id, workload->id.ptr, n);
        id[n] = '\0';

        /* Idempotent: the host replays binds after a plugin restart, so a
         * second bind for the same workload must replace rather than leak. */
        struct workload_client *c = client_find(id);
        if (c)
                client_release(c);

        c = client_alloc(id);
        if (!c) {
                ret.is_err = true;
                host_plugin_string_dup(&ret.val.err, "too many bound workloads");
                exports_wasmcloud_host_workload_lifecycle_on_workload_bind_return(ret);
                return HOST_PLUGIN_CALLBACK_CODE_EXIT;
        }

        c->conf = rd_kafka_conf_new();

        /* Every interface-level config key is passed to librdkafka verbatim, so
         * any mechanism it supports works unchanged: SASL/PLAIN, SCRAM,
         * OAUTHBEARER, GSSAPI, or mTLS. Secrets arrive here already merged by
         * the host (config -> configFrom -> secretFrom). */
        for (size_t i = 0; i < workload->interfaces.len; i++) {
                exports_wasmcloud_host_workload_lifecycle_interface_binding_t *b =
                    &workload->interfaces.ptr[i];
                for (size_t j = 0; j < b->config.len; j++) {
                        char key[256], val[1024];
                        host_plugin_string_t *k = &b->config.ptr[j].f0;
                        host_plugin_string_t *v = &b->config.ptr[j].f1;

                        size_t kn = k->len < sizeof(key) - 1 ? k->len : sizeof(key) - 1;
                        size_t vn = v->len < sizeof(val) - 1 ? v->len : sizeof(val) - 1;
                        memcpy(key, k->ptr, kn); key[kn] = '\0';
                        memcpy(val, v->ptr, vn); val[vn] = '\0';

                        if (rd_kafka_conf_set(c->conf, key, val, errstr,
                                              sizeof(errstr)) != RD_KAFKA_CONF_OK) {
                                client_release(c);
                                ret.is_err = true;
                                host_plugin_string_dup(&ret.val.err, errstr);
                                exports_wasmcloud_host_workload_lifecycle_on_workload_bind_return(ret);
                                return HOST_PLUGIN_CALLBACK_CODE_EXIT;
                        }
                }
        }

        ret.is_err = false;
        exports_wasmcloud_host_workload_lifecycle_on_workload_bind_return(ret);
        return HOST_PLUGIN_CALLBACK_CODE_EXIT;
}

host_plugin_callback_code_t exports_wasmcloud_host_workload_lifecycle_on_workload_bind_callback(
    host_plugin_event_t *event) {
        (void)event;
        return HOST_PLUGIN_CALLBACK_CODE_EXIT;
}

/**
 * The workload is gone. Best-effort: must tolerate ids never bound.
 */
host_plugin_callback_code_t exports_wasmcloud_host_workload_lifecycle_on_workload_unbind(
    exports_wasmcloud_host_workload_lifecycle_workload_id_t *id) {
        char buf[MAX_ID_LEN];
        size_t n = id->len < sizeof(buf) - 1 ? id->len : sizeof(buf) - 1;
        memcpy(buf, id->ptr, n);
        buf[n] = '\0';

        client_release(client_find(buf));

        exports_wasmcloud_host_workload_lifecycle_on_workload_unbind_return();
        return HOST_PLUGIN_CALLBACK_CODE_EXIT;
}

host_plugin_callback_code_t exports_wasmcloud_host_workload_lifecycle_on_workload_unbind_callback(
    host_plugin_event_t *event) {
        (void)event;
        return HOST_PLUGIN_CALLBACK_CODE_EXIT;
}
