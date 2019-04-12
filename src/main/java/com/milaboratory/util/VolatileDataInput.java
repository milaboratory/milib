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

import java.io.DataInput;
import java.io.IOException;

public final class VolatileDataInput implements DataInput {
    private volatile DataInput internal = null;

    public void setInternal(DataInput internal) {
        this.internal = internal;
    }

    @Override
    public void readFully(byte[] b) throws IOException {internal.readFully(b);}

    @Override
    public void readFully(byte[] b, int off, int len) throws IOException {internal.readFully(b, off, len);}

    @Override
    public int skipBytes(int n) throws IOException {return internal.skipBytes(n);}

    @Override
    public boolean readBoolean() throws IOException {return internal.readBoolean();}

    @Override
    public byte readByte() throws IOException {return internal.readByte();}

    @Override
    public int readUnsignedByte() throws IOException {return internal.readUnsignedByte();}

    @Override
    public short readShort() throws IOException {return internal.readShort();}

    @Override
    public int readUnsignedShort() throws IOException {return internal.readUnsignedShort();}

    @Override
    public char readChar() throws IOException {return internal.readChar();}

    @Override
    public int readInt() throws IOException {return internal.readInt();}

    @Override
    public long readLong() throws IOException {return internal.readLong();}

    @Override
    public float readFloat() throws IOException {return internal.readFloat();}

    @Override
    public double readDouble() throws IOException {return internal.readDouble();}

    @Override
    public String readLine() throws IOException {return internal.readLine();}

    @Override
    public String readUTF() throws IOException {return internal.readUTF();}
}
