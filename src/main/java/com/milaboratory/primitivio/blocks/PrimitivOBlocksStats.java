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

import static com.milaboratory.util.FormatUtils.*;

public final class PrimitivOBlocksStats extends PrimitivIOBlocksStatsAbstract {
    public final long
            totalSerializationNanos,
            serializationNanos,
            checksumNanos,
            compressionNanos,
            ioDelayNanos,
            uncompressedBytes,
            compressedBytes,
            outputSize,
            concurrencyOverhead;

    public PrimitivOBlocksStats(long wallClockTime,
                                long totalSerializationNanos, long serializationNanos, long checksumNanos,
                                long compressionNanos, long ioDelayNanos, long uncompressedBytes,
                                long compressedBytes,
                                long concurrencyOverhead, long outputSize, long blockCount,
                                long objectCount,
                                int ongoingSerdes, int ongoingIOOps, int pendingOps,
                                int concurrency) {
        super(wallClockTime, blockCount, objectCount, ongoingSerdes, ongoingIOOps, pendingOps, concurrency);
        this.totalSerializationNanos = totalSerializationNanos;
        this.serializationNanos = serializationNanos;
        this.checksumNanos = checksumNanos;
        this.compressionNanos = compressionNanos;
        this.ioDelayNanos = ioDelayNanos;
        this.concurrencyOverhead = concurrencyOverhead;
        this.uncompressedBytes = uncompressedBytes;
        this.compressedBytes = compressedBytes;
        this.outputSize = outputSize;
    }

    public long getAverageUncompressedObjectSize() {
        return uncompressedBytes / objectCount;
    }

    @Override
    public String toString() {
        long totalTimeNano = totalSerializationNanos + ioDelayNanos + concurrencyOverhead;
        int concurrency = this.concurrency;
        if (concurrency == 0)
            concurrency = 1;
        long concurrencyAdjustedNanos = (totalSerializationNanos + ioDelayNanos) / this.concurrency + concurrencyOverhead;
        return "Wall clock time: " + nanoTimeToString(wallClockTime) + "\n" +
                "Total CPU time: " + nanoTimeToString(totalSerializationNanos) + "\n" +
                "Serialization time: " + nanoTimeToString(serializationNanos) + " (" + percent(serializationNanos, totalSerializationNanos) + ")\n" +
                "Checksum calculation time: " + nanoTimeToString(checksumNanos) + " (" + percent(checksumNanos, totalSerializationNanos) + ")\n" +
                "Compression time: " + nanoTimeToString(compressionNanos) + " (" + percent(compressionNanos, totalSerializationNanos) + ")\n" +
                "Total IO delay: " + nanoTimeToString(ioDelayNanos) + "\n" +
                "Concurrency overhead: " + nanoTimeToString(concurrencyOverhead) + "\n" +
                "Uncompressed size: " + bytesToString(uncompressedBytes) + "\n" +
                "Output size: " + bytesToString(outputSize) + " (compression = " + percent(outputSize, uncompressedBytes) + ")\n" +
                "IO speed: " + bytesToStringDiv(NANOSECONDS_IN_SECOND * outputSize, ioDelayNanos) + "/s\n" +
                "Concurrency adjusted uncompressed speed: " + bytesToStringDiv(NANOSECONDS_IN_SECOND * uncompressedBytes, concurrencyAdjustedNanos) + "/s\n" +
                "Actual uncompressed speed: " + bytesToStringDiv(NANOSECONDS_IN_SECOND * uncompressedBytes, wallClockTime) + "/s\n" +
                "Actual speed: " + bytesToStringDiv(NANOSECONDS_IN_SECOND * outputSize, wallClockTime) + "/s\n" +
                "Objects: " + objectCount + "\n" +
                "Average object size uncompressed: " + bytesToStringDiv(uncompressedBytes, objectCount) + "\n" +
                "Average object size compressed: " + bytesToStringDiv(outputSize, objectCount) + "\n" +
                "Blocks: " + blockCount + " (~" + bytesToStringDiv(outputSize, blockCount) + " each)\n" +
                "Ongoing and pending ops (Serde / IO / Pending): " + ongoingSerdes + " / " + ongoingIOOps + " / " + pendingOps;
    }
}
