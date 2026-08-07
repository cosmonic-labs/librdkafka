/*
 * librdkafka - Apache Kafka C library
 *
 * Cooperative task scheduler for single-threaded WASI targets.
 * See rdwasm.h for why this exists.
 */

#ifdef __wasi__

#include "rdwasm.h"

#include <stddef.h>

/* Upper bound on concurrent tasks: the main handler, the optional background
 * handler, and one per broker (including the internal broker). A cluster large
 * enough to exhaust this would already be past what a single-threaded client
 * can serve, so a fixed table avoids allocating during rd_kafka_new(). */
#define RD_WASM_MAX_TASKS 64

struct rd_wasm_task {
        rd_wasm_step_t step;
        void *arg;
        /* Client handle this task belongs to, so a handle being destroyed can
         * wait for exactly its own tasks. */
        void *owner;
        int active;
};

static struct rd_wasm_task rd_wasm_tasks[RD_WASM_MAX_TASKS];

/* Re-entrancy guard. Any blocking wait in librdkafka funnels into a pump point,
 * including waits reached from inside a step; without this a stalled broker
 * would recurse pump -> step -> pump until the stack is gone. */
static int rd_wasm_in_pump;

int rd_wasm_task_add(rd_wasm_step_t step, void *arg, void *owner) {
        int i;

        for (i = 0; i < RD_WASM_MAX_TASKS; i++) {
                if (rd_wasm_tasks[i].active)
                        continue;
                rd_wasm_tasks[i].step   = step;
                rd_wasm_tasks[i].arg    = arg;
                rd_wasm_tasks[i].owner  = owner;
                rd_wasm_tasks[i].active = 1;
                return 0;
        }

        return -1;
}

int rd_wasm_owner_task_count(void *owner) {
        int i;
        int cnt = 0;

        for (i = 0; i < RD_WASM_MAX_TASKS; i++)
                if (rd_wasm_tasks[i].active && rd_wasm_tasks[i].owner == owner)
                        cnt++;

        return cnt;
}

void rd_wasm_join_owner(void *owner) {
        /* Bounded by the caller having already set the terminate flag that
         * every step tests, so each of owner's tasks retires within a pass or
         * two. Not bounded by other owners' tasks, which is the point. */
        while (rd_wasm_owner_task_count(owner) > 0)
                rd_wasm_pump();
}

void rd_wasm_task_remove(void *arg) {
        int i;

        for (i = 0; i < RD_WASM_MAX_TASKS; i++) {
                if (rd_wasm_tasks[i].active && rd_wasm_tasks[i].arg == arg)
                        rd_wasm_tasks[i].active = 0;
        }
}

void rd_wasm_pump(void) {
        int i;

        if (rd_wasm_in_pump)
                return;

        rd_wasm_in_pump = 1;

        for (i = 0; i < RD_WASM_MAX_TASKS; i++) {
                /* Re-read `active` each iteration: a step may retire itself or
                 * another task (a broker being decommissioned, say) while we
                 * are walking the table. */
                if (!rd_wasm_tasks[i].active)
                        continue;

                if (rd_wasm_tasks[i].step(rd_wasm_tasks[i].arg))
                        rd_wasm_tasks[i].active = 0;
        }

        rd_wasm_in_pump = 0;
}

int rd_wasm_task_count(void) {
        int i;
        int cnt = 0;

        for (i = 0; i < RD_WASM_MAX_TASKS; i++)
                if (rd_wasm_tasks[i].active)
                        cnt++;

        return cnt;
}

#endif /* __wasi__ */
