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
package com.milaboratory.primitivio.blocks;

public class PrimitivIOBlocksStatsAbstract {
    public final long
            wallClockTime,
            blockCount,
            objectCount;
    public final int
            ongoingSerdes,
            ongoingIOOps,
            pendingOps,
            concurrency;

    public PrimitivIOBlocksStatsAbstract(long wallClockTime, long blockCount, long objectCount,
                                         int ongoingSerdes, int ongoingIOOps, int pendingOps,
                                         int concurrency) {
        this.wallClockTime = wallClockTime;
        this.blockCount = blockCount;
        this.objectCount = objectCount;
        this.ongoingSerdes = ongoingSerdes;
        this.ongoingIOOps = ongoingIOOps;
        this.pendingOps = pendingOps;
        this.concurrency = concurrency;
    }

    public long getWallClockTime() {
        return wallClockTime;
    }

    public long getBlockCount() {
        return blockCount;
    }

    public long getObjectCount() {
        return objectCount;
    }

    public int getOngoingSerdes() {
        return ongoingSerdes;
    }

    public int getOngoingIOOps() {
        return ongoingIOOps;
    }

    public int getPendingOps() {
        return pendingOps;
    }

    public int getConcurrency() {
        return concurrency;
    }
}
