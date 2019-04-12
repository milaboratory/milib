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

import java.io.DataOutput;
import java.io.IOException;

public final class VolatileDataOutput implements DataOutput {
    private DataOutput internal = null;

    public void setInternal(DataOutput internal) {
        this.internal = internal;
    }

    @Override
    public void write(int b) throws IOException {internal.write(b);}

    @Override
    public void write(byte[] b) throws IOException {internal.write(b);}

    @Override
    public void write(byte[] b, int off, int len) throws IOException {internal.write(b, off, len);}

    @Override
    public void writeBoolean(boolean v) throws IOException {internal.writeBoolean(v);}

    @Override
    public void writeByte(int v) throws IOException {internal.writeByte(v);}

    @Override
    public void writeShort(int v) throws IOException {internal.writeShort(v);}

    @Override
    public void writeChar(int v) throws IOException {internal.writeChar(v);}

    @Override
    public void writeInt(int v) throws IOException {internal.writeInt(v);}

    @Override
    public void writeLong(long v) throws IOException {internal.writeLong(v);}

    @Override
    public void writeFloat(float v) throws IOException {internal.writeFloat(v);}

    @Override
    public void writeDouble(double v) throws IOException {internal.writeDouble(v);}

    @Override
    public void writeBytes(String s) throws IOException {internal.writeBytes(s);}

    @Override
    public void writeChars(String s) throws IOException {internal.writeChars(s);}

    @Override
    public void writeUTF(String s) throws IOException {internal.writeUTF(s);}
}
