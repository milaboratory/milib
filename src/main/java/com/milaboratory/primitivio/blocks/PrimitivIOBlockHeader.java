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

import java.nio.ByteBuffer;
import java.util.Arrays;

import static com.milaboratory.util.io.IOUtil.*;

/**
 * * Header (17 bytes total):
 * * [ 1 byte : bit0 = (0 = last block ; 1 = data or special block); bit1 = (0 = raw ; 1 = compressed); bit2 = (0 = data block ; 1 = special block) ]
 * * ( [ 4 bytes : int : number of objects ]
 * * [ 4 bytes : int : rawDataSize ]
 * * [ 4 bytes : int : compressedDataSize / blockSize ]
 * * [ 4 bytes : int : checksum for the raw data ] )
 * * |
 * * ( [ 16 bytes : special block ] )
 */
public final class PrimitivIOBlockHeader {
    /**
     * Header size in bytes
     */
    public static final int
            HEADER_SIZE = 17,
            NUMBER_OF_OBJECTS_OFFSET = 1,
            UNCOMPRESSED_DATA_SIZE_OFFSET = 5,
            DATA_SIZE_OFFSET = 9,
            CHECKSUM_OFFSET = 13;

    private static final byte[] LAST_HEADER = new byte[HEADER_SIZE];

    private byte[] headerBytes;

    private PrimitivIOBlockHeader(byte[] headerBytes) {
        if ((headerBytes[0] & 0xF8) != 0)
            throw new IllegalArgumentException("Illegal first byte.");
        if (headerBytes.length != HEADER_SIZE)
            throw new IllegalArgumentException();
        this.headerBytes = headerBytes;
        checkHeaderCorrectness();
    }

    public void checkHeaderCorrectness() {
        if (!isSpecial() && !isLastBlock())
            if (getNumberOfObjects() < 0 || getUncompressedDataSize() < getDataSize())
                throw new IllegalArgumentException("Malformed block header.");
    }

    public boolean isLastBlock() {
        return headerBytes[0] == 0;
    }

    public boolean isCompressed() {
        return (headerBytes[0] & 0x2) != 0;
    }

    public PrimitivIOBlockHeader setCompressed() {
        headerBytes[0] |= 0x2;
        return this;
    }

    public boolean isSpecial() {
        return (headerBytes[0] & 0x4) != 0;
    }

    public long getSpecialLong(int index) {
        if (!isSpecial())
            throw new IllegalStateException("Block is not special");
        if (index < 0 || index >= 2)
            throw new IndexOutOfBoundsException();
        return readLongBE(headerBytes, 1 + index * 8);
    }

    public PrimitivIOBlockHeader setSpecialLong(int index, long value) {
        if (!isSpecial())
            throw new IllegalStateException("Block is not special");
        if (index < 0 || index >= 2)
            throw new IndexOutOfBoundsException();
        writeLongBE(value, headerBytes, 1 + index * 8);
        return this;
    }

    public int getSpecialInt(int index) {
        if (!isSpecial())
            throw new IllegalStateException("Block is not special");
        if (index < 0 || index >= 4)
            throw new IndexOutOfBoundsException();
        return readIntBE(headerBytes, 1 + index * 4);
    }

    public PrimitivIOBlockHeader setSpecialInt(int index, int value) {
        if (!isSpecial())
            throw new IllegalStateException("Block is not special");
        if (index < 0 || index >= 4)
            throw new IndexOutOfBoundsException();
        writeIntBE(value, headerBytes, 1 + index * 4);
        return this;
    }

    public byte getSpecialByte(int index) {
        if (!isSpecial())
            throw new IllegalStateException("Block is not special");
        if (index < 0 || index >= 16)
            throw new IndexOutOfBoundsException();
        return headerBytes[1 + index];
    }

    public PrimitivIOBlockHeader setSpecialByte(int index, byte value) {
        if (!isSpecial())
            throw new IllegalStateException("Block is not special");
        if (index < 0 || index >= 16)
            throw new IndexOutOfBoundsException();
        headerBytes[1 + index] = value;
        return this;
    }

    public int getNumberOfObjects() {
        return readIntBE(headerBytes, NUMBER_OF_OBJECTS_OFFSET);
    }

    public PrimitivIOBlockHeader setNumberOfObjects(int value) {
        writeIntBE(value, headerBytes, NUMBER_OF_OBJECTS_OFFSET);
        return this;
    }

    public int getUncompressedDataSize() {
        return readIntBE(headerBytes, UNCOMPRESSED_DATA_SIZE_OFFSET);
    }

    public PrimitivIOBlockHeader setUncompressedDataSize(int value) {
        writeIntBE(value, headerBytes, UNCOMPRESSED_DATA_SIZE_OFFSET);
        return this;
    }

    public int getDataSize() {
        return readIntBE(headerBytes, DATA_SIZE_OFFSET);
    }

    public PrimitivIOBlockHeader setDataSize(int value) {
        writeIntBE(value, headerBytes, DATA_SIZE_OFFSET);
        return this;
    }

    public int getChecksum() {
        return readIntBE(headerBytes, CHECKSUM_OFFSET);
    }

    public PrimitivIOBlockHeader setChecksum(int value) {
        writeIntBE(value, headerBytes, CHECKSUM_OFFSET);
        return this;
    }

    private static PrimitivIOBlockHeader header(byte firstByte) {
        final byte[] headerBytes = new byte[HEADER_SIZE];
        headerBytes[0] = firstByte;
        return new PrimitivIOBlockHeader(headerBytes);
    }

    public static PrimitivIOBlockHeader dataBlockHeader() {
        return header((byte) 0x1);
    }

    public static PrimitivIOBlockHeader specialHeader() {
        return header((byte) 0x5);
    }

    public static PrimitivIOBlockHeader lastHeader() {
        return new PrimitivIOBlockHeader(LAST_HEADER.clone());
    }

    public static PrimitivIOBlockHeader readHeaderNoCopy(byte[] headerBytes) {
        return new PrimitivIOBlockHeader(headerBytes);
    }

    public static PrimitivIOBlockHeader readHeader(byte[] buffer, int offset) {
        if (buffer.length < offset + 17)
            throw new IllegalArgumentException("Wrong buffer size");
        return new PrimitivIOBlockHeader(Arrays.copyOfRange(buffer, offset, offset + HEADER_SIZE));
    }

    public void writeTo(byte[] buffer, int offset) {
        if (buffer.length < offset + 17)
            throw new IllegalArgumentException("Wrong buffer size");
        System.arraycopy(headerBytes, 0, buffer, offset, HEADER_SIZE);
    }

    public ByteBuffer asByteBuffer() {
        return ByteBuffer.wrap(headerBytes);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PrimitivIOBlockHeader)) return false;
        PrimitivIOBlockHeader that = (PrimitivIOBlockHeader) o;
        return Arrays.equals(headerBytes, that.headerBytes);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(headerBytes);
    }

    @Override
    public String toString() {
        if (!isSpecial())
            return "NormalHeader{" +
                    "numberOfObjects=" + getNumberOfObjects() + "," +
                    "uncompressedDataSize=" + getUncompressedDataSize() + "," +
                    "dataSize=" + getDataSize() + "," +
                    "checksum=" + getChecksum() + "}";
        else
            return "SpecialBlock{" + Arrays.toString(Arrays.copyOfRange(headerBytes, 1, HEADER_SIZE)) + "}";
    }
}
