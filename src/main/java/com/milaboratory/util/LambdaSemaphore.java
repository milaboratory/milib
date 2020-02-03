/*
 * Copyright 2019 MiLaboratory, LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.milaboratory.util;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

public final class LambdaSemaphore {
    final ConcurrentLinkedQueue<Runnable> queue = new ConcurrentLinkedQueue<>();

    /** (number of queued objects) : 32 bits | (number of permits) : 32 bits */
    final AtomicLong state;
    /** Number of permits this object was initialized with */
    final int initialPermits;

    public LambdaSemaphore(int initialPermits) {
        this.initialPermits = initialPermits;
        this.state = new AtomicLong(encode(0, initialPermits));
    }

    private static long encode(int queued, int permits) {
        return ((long) queued) << 32 | (0xFFFFFFFFL & permits);
    }

    /** Extracts number of permits from 64-bit state encoded value */
    private static int permits(long state) {
        return (int) (0xFFFFFFFFL & state);
    }

    /** Extracts number of queued lambdas from 64-bit state encoded value */
    private static int queued(long state) {
        return (int) (state >>> 32);
    }

    private static boolean hasPermitsAndPendingRequests(long state) {
        return permits(state) > 0 && queued(state) > 0;
    }

    private void executePending() {
        long stateValue;
        while (hasPermitsAndPendingRequests(stateValue = state.get())) {
            if (state.compareAndSet(stateValue, stateValue - encode(1, 1))) {
                Runnable next = queue.poll();
                if (next == null)
                    throw new InternalError("Impossible state.");
                next.run();
            }
        }
    }

    /**
     * Consumes a permit from this semaphore (if available) and executes the Runnable right away,
     * if no permits available, the Runnable action is added to the execution queue.
     */
    public void acquire(Runnable callback) {
        queue.offer(callback);
        state.addAndGet(encode(1, 0));
        executePending();
    }

    /** Adds a permit to this semaphore, and executes first pending action from the queue if there are any. */
    public void release() {
        state.addAndGet(encode(0, 1));
        executePending();
    }

    /** Returns the number of permits this objects was initially created with. */
    public int getInitialPermits() {
        return initialPermits;
    }
}
