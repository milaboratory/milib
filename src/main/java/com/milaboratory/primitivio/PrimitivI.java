/*
 * Copyright 2015 MiLaboratory.com
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
package com.milaboratory.primitivio;

import java.io.*;
import java.util.ArrayList;

public final class PrimitivI implements DataInput, AutoCloseable {
    final DataInput input;
    final SerializersManager manager;
    final ArrayList<Object> references = new ArrayList<>(), putKnownAfterReset = new ArrayList<>();
    int knownReferencesCount = 0;
    int depth = 0;

    public PrimitivI(InputStream input) {
        this(new DataInputStream(input),
                new SerializersManager());
    }

    public PrimitivI(DataInput input) {
        this(input, new SerializersManager());
    }

    public PrimitivI(DataInput input, SerializersManager manager) {
        this.input = input;
        this.manager = manager;
    }

    public SerializersManager getSerializersManager() {
        return manager;
    }

    public void putKnownReference(Object ref) {
        if (depth > 0) {
            putKnownAfterReset.add(ref);
        } else {
            references.add(ref);
            ++knownReferencesCount;
        }
    }

    public void readReference(Object ref) {
        int id = readVarInt();
        if (id != references.size())
            throw new RuntimeException("wrong reference id.");
        references.add(ref);
    }

    private void reset() {
        for (int i = references.size() - 1; i >= knownReferencesCount; --i)
            references.remove(i);
        if (!putKnownAfterReset.isEmpty()) {
            for (Object ref : putKnownAfterReset)
                putKnownReference(ref);
            putKnownAfterReset.clear();
        }
    }

    public <T> T readObject(Class<T> type) {
        Serializer serializer = manager.getSerializer(type);
        if (serializer.isReference()) {
            int id = readVarInt();
            if (id == PrimitivO.NULL_ID) {
                return null;
            } else if (id == PrimitivO.NEW_OBJECT_ID) {
                boolean readReferenceAfter = !serializer.handlesReference();

                ++depth;
                try {
                    T obj = (T) serializer.read(this);

                    if (readReferenceAfter)
                        readReference(obj);

                    return obj;
                } finally {
                    --depth;
                    if (depth == 0)
                        reset();
                }
            } else {
                Object obj = references.get(id - PrimitivO.ID_OFFSET);
                if (!type.isInstance(obj))
                    throw new RuntimeException("Wrong file format.");
                return (T) obj;
            }
        } else {
            ++depth;
            try {
                return (T) serializer.read(this);
            } finally {
                --depth;
                if (depth == 0)
                    reset();
            }
        }
    }

    public int readVarInt() {
        int value = 0, tmp;
        int shift = 0;
        do {
            tmp = readByte();
            value |= (tmp & 0x7F) << (shift);
            shift += 7;
        } while ((tmp & 0x80) != 0);
        return value;
    }

    @Override
    public void readFully(byte[] b) {
        try {
            input.readFully(b);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void readFully(byte[] b, int off, int len) {
        try {
            input.readFully(b, off, len);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int skipBytes(int n) {
        try {
            return input.skipBytes(n);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean readBoolean() {
        try {
            return input.readBoolean();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte readByte() {
        try {
            return input.readByte();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int readUnsignedByte() {
        try {
            return input.readUnsignedByte();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public short readShort() {
        try {
            return input.readShort();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int readUnsignedShort() {
        try {
            return input.readUnsignedShort();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public char readChar() {
        try {
            return input.readChar();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int readInt() {
        try {
            return input.readInt();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long readLong() {
        try {
            return input.readLong();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public float readFloat() {
        try {
            return input.readFloat();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public double readDouble() {
        try {
            return input.readDouble();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String readLine() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String readUTF() {
        try {
            return input.readUTF();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        try {
            if (input instanceof Closeable)
                ((Closeable) input).close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
