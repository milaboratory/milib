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

public final class PrimitivOBlocksStat {
    private final long
            wallClockTime,
            totalSerializationNanos,
            serializationNanos,
            checksumNanos,
            compressionNanos,
            ioDelayNanos,
            uncompressedBytes,
            outputSize,
            concurrencyOverhead,
            blockCount;
    private final int concurrency;

    public PrimitivOBlocksStat(long wallClockTime,
                               long totalSerializationNanos, long serializationNanos, long checksumNanos,
                               long compressionNanos, long ioDelayNanos, long uncompressedBytes,
                               long concurrencyOverhead,
                               long outputSize, long blockCount,
                               int concurrency) {
        this.wallClockTime = wallClockTime;
        this.totalSerializationNanos = totalSerializationNanos;
        this.serializationNanos = serializationNanos;
        this.checksumNanos = checksumNanos;
        this.compressionNanos = compressionNanos;
        this.ioDelayNanos = ioDelayNanos;
        this.concurrencyOverhead = concurrencyOverhead;
        this.uncompressedBytes = uncompressedBytes;
        this.outputSize = outputSize;
        this.blockCount = blockCount;
        this.concurrency = concurrency;
    }

    @Override
    public String toString() {
        long totalTimeNano = totalSerializationNanos + ioDelayNanos + concurrencyOverhead;
        long concurrencyAdjustedNanos = (totalSerializationNanos + ioDelayNanos) / concurrency + concurrencyOverhead;
        return "Total CPU time: " + nanoTimeToString(totalSerializationNanos) + "\n" +
                "Serialization time: " + nanoTimeToString(serializationNanos) + " (" + percent(serializationNanos, totalSerializationNanos) + ")\n" +
                "Checksum calculation time: " + nanoTimeToString(checksumNanos) + " (" + percent(checksumNanos, totalSerializationNanos) + ")\n" +
                "Compression time: " + nanoTimeToString(compressionNanos) + " (" + percent(compressionNanos, totalSerializationNanos) + ")\n" +
                "Total IO delay: " + nanoTimeToString(ioDelayNanos) + "\n" +
                "Concurrency overhead: " + nanoTimeToString(concurrencyOverhead) + "\n" +
                "Uncompressed size: " + bytesToString(uncompressedBytes) + "\n" +
                "Output size: " + bytesToString(outputSize) + "\n" +
                "Wall clock time: " + nanoTimeToString(wallClockTime) + "\n" +
                "IO speed: " + bytesToString(NANOSECONDS_IN_SECOND * outputSize / ioDelayNanos) + "/s\n" +
                "Concurrency adjusted uncompressed speed: " + bytesToString(NANOSECONDS_IN_SECOND * uncompressedBytes / concurrencyAdjustedNanos) + "/s\n" +
                "Actual uncompressed speed: " + bytesToString(NANOSECONDS_IN_SECOND * uncompressedBytes / wallClockTime) + "/s\n" +
                "Actual speed: " + bytesToString(NANOSECONDS_IN_SECOND * outputSize / wallClockTime) + "/s\n" +
                "Blocks: " + blockCount + " (~" + bytesToString(outputSize / blockCount) + " each)";
    }
}
