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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Lock-free primitive allowing to execute specific callback after a certain point is reached in one of the thread.
 *
 * Lambda is executed either in the thread invoking {@link #open()} or in the thread invoking {@link #setCallback(Runnable)}.
 */
public final class LambdaLatch {
    /**
     * Closed, and callback is not set.
     */
    private static final int STATE_CLOSED = 0;
    /**
     * Opened, callback not yet set.
     *
     * Callback execution from the {@link #setCallback(Runnable)} thread.
     */
    private static final int STATE_OPENED = 1;
    /**
     * Armed, callback set but latch is still closed.
     *
     * Callback execution from the {@link #open()} thread.
     */
    private static final int STATE_ARMED = 2;
    /**
     * Done, callback successfully executed.
     */
    private static final int STATE_DONE = 3;

    /**
     * Atomic state of the latch
     */
    private final AtomicInteger state;
    /**
     * Callback
     */
    private final AtomicReference<Runnable> callback = new AtomicReference<>();

    public LambdaLatch() {
        this.state = new AtomicInteger(STATE_CLOSED);
    }

    public LambdaLatch(boolean opened) {
        this.state = new AtomicInteger(opened ? STATE_OPENED : STATE_CLOSED);
    }

    /**
     * Atomically opens the latch.
     *
     * If {@link #setCallback(Runnable)} was previously called, executes the callback from current thread.
     */
    public void open() {
        if (state.compareAndSet(STATE_CLOSED, STATE_OPENED))
            return;
        if (state.compareAndSet(STATE_ARMED, STATE_DONE))
            run();
    }

    /**
     * Atomically sets the callback. Executes it in current thread if latch is opened.
     *
     * Only one call to either {@link #setCallback(Runnable)} or {@link #await()} is allowed for an instance of this
     * class.
     *
     * @param callback callback to execute on latch open
     */
    public void setCallback(Runnable callback) {
        if (!this.callback.compareAndSet(null, callback))
            throw new IllegalStateException("Callback already set.");
        if (state.compareAndSet(STATE_CLOSED, STATE_ARMED))
            return;
        if (state.compareAndSet(STATE_OPENED, STATE_DONE))
            run();
    }

    /**
     * Awaits this latch to be opened.
     *
     * Only one call to either {@link #setCallback(Runnable)} or {@link #await()} is allowed for an instance of this
     * class.
     *
     * @throws InterruptedException
     */
    public void await() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        setCallback(latch::countDown);
        latch.await();
    }

    private void run() {
        try {
            callback.get().run();
        } catch (RuntimeException e) {
            throw new LambdaExecutionException(e);
        }
    }
}
