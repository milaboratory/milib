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
package com.milaboratory.util.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousByteChannel;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

public final class IOUtil {
    private IOUtil() {
    }

    public static void writeIntBE(int val, byte[] buffer, int offset) {
        buffer[offset] = (byte) (val >>> 24);
        buffer[offset + 1] = (byte) (val >>> 16);
        buffer[offset + 2] = (byte) (val >>> 8);
        buffer[offset + 3] = (byte) val;
    }

    public static void writeLongBE(long val, byte[] buffer, int offset) {
        buffer[offset] = (byte) (val >>> 56);
        buffer[offset + 1] = (byte) (val >>> 48);
        buffer[offset + 2] = (byte) (val >>> 40);
        buffer[offset + 3] = (byte) (val >>> 32);
        buffer[offset + 4] = (byte) (val >>> 24);
        buffer[offset + 5] = (byte) (val >>> 16);
        buffer[offset + 6] = (byte) (val >>> 8);
        buffer[offset + 7] = (byte) val;
    }

    public static int readIntBE(byte[] buffer, int offset) {
        int value = 0;
        for (int i = 0; i < 4; ++i) {
            value <<= 8;
            value |= 0xFF & buffer[offset + i];
        }
        return value;
    }

    public static int readIntBE(ByteBuffer buffer) {
        int value = 0;
        for (int i = 0; i < 4; ++i) {
            value <<= 8;
            value |= 0xFF & buffer.get();
        }
        return value;
    }

    public static long readLongBE(byte[] buffer, int offset) {
        long value = 0;
        for (int i = 0; i < 8; ++i) {
            value <<= 8;
            value |= 0xFF & buffer[offset++];
        }
        return value;
    }

    public static long readLongBE(ByteBuffer buffer) {
        long value = 0;
        for (int i = 0; i < 8; ++i) {
            value <<= 8;
            value |= 0xFF & buffer.get();
        }
        return value;
    }
}
