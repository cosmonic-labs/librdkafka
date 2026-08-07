/*
 * librdkafka - Apache Kafka C library
 *
 * Cooperative task scheduler for single-threaded WASI targets.
 *
 * WASI components have no threads: `thrd_create` resolves to a wasi-libc stub
 * that returns ENOTSUP, so `rd_kafka_new()` fails before returning a handle.
 * Rather than rearchitect librdkafka, this replaces preemptive threads with
 * cooperative tasks, which works because librdkafka is already timeout-driven
 * end to end:
 *
 *   rd_kafka_broker_thread_main
 *     while (!terminating) { switch (rkb->rkb_state) ...
 *       rd_kafka_broker_serve(rkb, timeout_ms)
 *         while (!rd_timeout_expired(abs_timeout))
 *           rd_kafka_broker_ops_io_serve(rkb, deadline)
 *             poll(pfd, cnt, tmout)
 *
 * Every layer takes a deadline and returns when it expires, so passing a zero
 * timeout turns the whole stack into one non-blocking pass. Each former thread
 * main is split into an `init` that runs once and a `step` that performs one
 * such pass, and the scheduler round-robins the steps.
 *
 * This is deliberately a bridge, not a destination. When WASIp3 cooperative
 * threads stabilise, `thrd_create` can map onto a real cooperative task and the
 * init/step split becomes unnecessary — but the task registry and the pump
 * points stay the same shape.
 */
#ifndef _RDWASM_H_
#define _RDWASM_H_

#ifdef __wasi__

/**
 * @brief One slice of a cooperative task.
 *
 * Must not block. Returns 0 while the task still has work to do, non-zero once
 * it has run to completion (the equivalent of a thread main returning).
 */
typedef int (*rd_wasm_step_t)(void *arg);

/**
 * @brief Register a cooperative task.
 *
 * @returns 0 on success, -1 if the task table is full.
 *
 * @remark \p arg doubles as the task's identity for rd_wasm_task_remove().
 */
int rd_wasm_task_add(rd_wasm_step_t step, void *arg, void *owner);

/**
 * @brief Number of live tasks belonging to \p owner (an rd_kafka_t).
 */
int rd_wasm_owner_task_count(void *owner);

/**
 * @brief Drive the scheduler until every task belonging to \p owner has
 *        retired.
 *
 * Required before a client handle is freed. A task is a bare function pointer
 * plus a pointer into that handle, so a task outliving its handle is a
 * use-after-free the next time anything pumps — which, with two clients in one
 * process, is the very next call the surviving client makes.
 *
 * Only waits on \p owner's tasks, never on the whole table: the caller is
 * frequently itself a task of some other owner, and waiting for the table to
 * empty would wait on itself.
 */
void rd_wasm_join_owner(void *owner);

/**
 * @brief Unregister the task registered with \p arg. Safe to call for an
 *        unknown \p arg, and safe to call from inside that task's own step.
 */
void rd_wasm_task_remove(void *arg);

/**
 * @brief Run one slice of every registered task.
 *
 * Re-entrant calls are no-ops: a step that itself reaches a pump point (any
 * blocking wait in librdkafka funnels into one) must not recurse back into the
 * scheduler, or a single broker stall would grow the stack without bound.
 */
void rd_wasm_pump(void);

/**
 * @brief Number of tasks that have not yet run to completion.
 */
int rd_wasm_task_count(void);

#endif /* __wasi__ */
#endif /* _RDWASM_H_ */
