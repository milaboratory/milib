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

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;

public final class PrimitivIOBlocksUtil {
    private PrimitivIOBlocksUtil() {
    }

    public static ExecutorService defaultExecutorService() {
        return ForkJoinPool.commonPool();
    }

    private static final LZ4Factory lz4Factory = LZ4Factory.fastestInstance();
    private static final LZ4Compressor lz4FastCompressor = lz4Factory.fastCompressor();
    private static final LZ4Compressor lz4HighCompressor = lz4Factory.highCompressor();
    private static final LZ4FastDecompressor lz4Decompressor = lz4Factory.fastDecompressor();

    public static LZ4Compressor highLZ4Compressor() {
        return lz4HighCompressor;
    }

    public static LZ4Compressor fastLZ4Compressor() {
        return lz4FastCompressor;
    }

    public static LZ4Compressor defaultLZ4Compressor() {
        return fastLZ4Compressor();
    }

    public static LZ4FastDecompressor defaultLZ4Decompressor() {
        return lz4Decompressor;
    }

    public static LZ4Compressor getCompressor(boolean highCompression) {
        return highCompression
                ? highLZ4Compressor()
                : fastLZ4Compressor();
    }
}
