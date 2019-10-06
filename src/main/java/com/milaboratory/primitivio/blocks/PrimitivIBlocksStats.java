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

public final class PrimitivIBlocksStats extends PrimitivIOBlocksStatsAbstract {
    protected final long totalDeserializationNanos;
    protected final long deserializationNanos;
    protected final long checksumNanos;
    protected final long decompressionNanos;
    protected final long ioDelayNanos;
    protected final long uncompressedBytes;
    protected final long inputSize;

    public PrimitivIBlocksStats(long wallClockTime, long totalDeserializationNanos, long deserializationNanos, long checksumNanos,
                                long decompressionNanos, long ioDelayNanos, long uncompressedBytes,
                                long inputSize, long blockCount, long objectCount,
                                int ongoingSerdes, int ongoingIOOps, int pendingOps,
                                int concurrency) {
        super(wallClockTime, blockCount, objectCount, ongoingSerdes, ongoingIOOps, pendingOps, concurrency);
        this.totalDeserializationNanos = totalDeserializationNanos;
        this.deserializationNanos = deserializationNanos;
        this.checksumNanos = checksumNanos;
        this.decompressionNanos = decompressionNanos;
        this.ioDelayNanos = ioDelayNanos;
        this.uncompressedBytes = uncompressedBytes;
        this.inputSize = inputSize;
    }

    @Override
    public String toString() {
        // long totalTimeNano = totalSerializationNanos + ioDelayNanos + concurrencyOverhead;
        long concurrencyAdjustedNanos = (totalDeserializationNanos + ioDelayNanos) / concurrency;
        return "Wall clock time: " + nanoTimeToString(wallClockTime) + "\n" +
                "Total CPU time: " + nanoTimeToString(totalDeserializationNanos) + "\n" +
                "Serialization time: " + nanoTimeToString(deserializationNanos) + " (" + percent(deserializationNanos, totalDeserializationNanos) + ")\n" +
                "Checksum calculation time: " + nanoTimeToString(checksumNanos) + " (" + percent(checksumNanos, totalDeserializationNanos) + ")\n" +
                "Compression time: " + nanoTimeToString(decompressionNanos) + " (" + percent(decompressionNanos, totalDeserializationNanos) + ")\n" +
                "Total IO delay: " + nanoTimeToString(ioDelayNanos) + "\n" +
                "Input size: " + bytesToString(inputSize) + "\n" +
                "Decompressed size: " + bytesToString(uncompressedBytes) + " (compression = " + percent(inputSize, uncompressedBytes) + ")\n" +
                "IO speed: " + bytesToString(NANOSECONDS_IN_SECOND * inputSize / ioDelayNanos) + "/s\n" +
                "Concurrency adjusted uncompressed speed: " + bytesToString(NANOSECONDS_IN_SECOND * uncompressedBytes / concurrencyAdjustedNanos) + "/s\n" +
                "Actual uncompressed speed: " + bytesToString(NANOSECONDS_IN_SECOND * uncompressedBytes / wallClockTime) + "/s\n" +
                "Actual speed: " + bytesToString(NANOSECONDS_IN_SECOND * inputSize / wallClockTime) + "/s\n" +
                "Objects: " + objectCount + "\n" +
                "Average object size uncompressed: " + bytesToString(uncompressedBytes / objectCount) + "\n" +
                "Average object size compressed: " + bytesToString(inputSize / objectCount) + "\n" +
                "Blocks: " + blockCount + " (~" + bytesToString(inputSize / blockCount) + " each)\n" +
                "Ongoing and pending ops (Serde / IO / Pending): " + ongoingSerdes + " / " + ongoingIOOps + " / " + pendingOps;
    }
}
