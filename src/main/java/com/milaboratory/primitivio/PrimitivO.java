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

import gnu.trove.impl.Constants;
import gnu.trove.map.custom_hash.TObjectIntCustomHashMap;
import gnu.trove.strategy.IdentityHashingStrategy;

import java.io.*;
import java.util.ArrayList;

public final class PrimitivO implements DataOutput, AutoCloseable {
    static final int NULL_ID = 0;
    static final int NEW_OBJECT_ID = 1;
    static final int ID_OFFSET = 2;
    private static final float RELOAD_FACTOR = 0.5f;
    final DataOutput output;
    final SerializersManager manager;
    final ArrayList<Object> putKnownAfterReset = new ArrayList<>();
    final TObjectIntCustomHashMap<Object> knownReferences = new TObjectIntCustomHashMap<>(IdentityHashingStrategy.INSTANCE,
            Constants.DEFAULT_CAPACITY, Constants.DEFAULT_LOAD_FACTOR, Integer.MIN_VALUE);
    final ArrayList<Object> addedReferences = new ArrayList<>();
    int depth = 0;
    TObjectIntCustomHashMap<Object> currentReferences = null;

    public PrimitivO(OutputStream output) {
        this(new DataOutputStream(output), new SerializersManager());
    }

    public PrimitivO(DataOutput output) {
        this(output, new SerializersManager());
    }

    public PrimitivO(DataOutput output, SerializersManager manager) {
        this.output = output;
        this.manager = manager;
    }

    private void ensureCurrentReferencesInitialized() {
        if (currentReferences == null)
            currentReferences = new TObjectIntCustomHashMap<>(IdentityHashingStrategy.INSTANCE, knownReferences);
    }

    public void putKnownReference(Object object) {
        if (depth > 0)
            putKnownAfterReset.add(object);
        else {
            // Assigning this reference next available id (0 assigned to null)
            knownReferences.put(object, knownReferences.size());
            currentReferences = null;
        }
    }

    private void reset() {
        if (currentReferences != null && currentReferences.size() != knownReferences.size()) {
            if ((currentReferences.size() - knownReferences.size()) * RELOAD_FACTOR > knownReferences.size())
                currentReferences = null;
            else
                for (Object ref : addedReferences)
                    currentReferences.remove(ref);
            //Resetting list of references added in this serialization round
            addedReferences.clear();
        }
        if (!putKnownAfterReset.isEmpty()) {
            for (Object ref : putKnownAfterReset)
                putKnownReference(ref);
            putKnownAfterReset.clear();
        }
    }

    private int addCurrentReference(Object ref) {
        int id = currentReferences.size();
        currentReferences.put(ref, id);
        addedReferences.add(ref);
        return id;
    }

    public void writeReference(Object ref) {
        int id = addCurrentReference(ref);
        writeVarInt(id);
    }

    public void writeObject(Object object, Class<?> type) {
        Serializer serializer = manager.getSerializer(type);

        if (object == null)
            if (serializer.isReference())
                writeNull();
            else
                throw new IllegalArgumentException("Non-reference type can't be null.");
        else {
            if (depth == 0)
                ensureCurrentReferencesInitialized();

            boolean writeIdAfter = false;
            if (serializer.isReference()) {
                int id = currentReferences.get(object);
                if (id != Integer.MIN_VALUE) {
                    writeObjectReference(id);
                    return;
                } else {
                    // Write just new object header to tell the reader that this object has no id yet
                    writeNewObject();
                    writeIdAfter = !serializer.handlesReference();
                }
            }

            ++depth;
            try {
                serializer.write(this, object);
                if (writeIdAfter)
                    writeReference(object);
            } finally {
                --depth;
                if (depth == 0)
                    reset();
            }
        }
    }

    private void writeNull() {
        writeByte(NULL_ID);
    }

    private void writeNewObject() {
        writeByte(NEW_OBJECT_ID);
    }

    private void writeObjectReference(int value) {
        writeVarInt(value + ID_OFFSET);
    }

    public void writeVarInt(int value) {
        do {
            int toWrite = value & 0x7F;
            value >>>= 7;
            if (value != 0)
                toWrite |= 0x80;
            writeByte(toWrite);
        } while (value != 0);
    }

    public void writeObject(Object object) {
        if (object == null)
            writeByte(0);
        else
            writeObject(object, object.getClass());
    }

    @Override
    public void write(int b) {
        try {
            output.write(b);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void write(byte[] b) {
        try {
            output.write(b);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void write(byte[] b, int off, int len) {
        try {
            output.write(b, off, len);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void writeBoolean(boolean v) {
        try {
            output.writeBoolean(v);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void writeByte(int v) {
        try {
            output.writeByte(v);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void writeShort(int v) {
        try {
            output.writeShort(v);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void writeChar(int v) {
        try {
            output.writeChar(v);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void writeInt(int v) {
        try {
            output.writeInt(v);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void writeLong(long v) {
        try {
            output.writeLong(v);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void writeFloat(float v) {
        try {
            output.writeFloat(v);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void writeDouble(double v) {
        try {
            output.writeDouble(v);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void writeBytes(String s) {
        try {
            output.writeBytes(s);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void writeChars(String s) {
        try {
            output.writeChars(s);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void writeUTF(String s) {
        try {
            output.writeUTF(s);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        try {
            if (output instanceof Closeable)
                ((Closeable) output).close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
