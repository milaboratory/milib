/*
 * MiTCR <http://milaboratory.com>
 *
 * Copyright (c) 2010-2013:
 *     Bolotin Dmitriy     <bolotin.dmitriy@gmail.com>
 *     Chudakov Dmitriy    <chudakovdm@mail.ru>
 *
 * MiTCR is free software: you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation, either version 3 of the License, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package com.milaboratory.core.io;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;

import java.io.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public enum CompressionType {
    None, GZIP, BZIP2;

    public static CompressionType detectCompressionType(File file) {
        return detectCompressionType(file.getName());
    }

    public static CompressionType detectCompressionType(String fileName) {
        fileName = fileName.trim().toLowerCase();

        if (fileName.endsWith(".gz"))
            return GZIP;
        else if (fileName.endsWith(".bz2"))
            return BZIP2;
        return None;
    }

    public InputStream createInputStream(InputStream is) throws IOException {
        return createInputStream(this, is, 2048);
    }

    public InputStream createInputStream(InputStream is, int buffer) throws IOException {
        return createInputStream(this, is, buffer);
    }

    public OutputStream createOutputStream(OutputStream os) throws IOException {
        return createOutputStream(this, os, 2048);
    }

    public OutputStream createOutputStream(OutputStream os, int buffer) throws IOException {
        return createOutputStream(this, os, buffer);
    }

    private static InputStream createInputStream(CompressionType ct, InputStream is, int buffer) throws IOException {
        switch (ct) {
            case None:
                return is;
            case GZIP:
                return new GZIPInputStream(is, buffer);
            case BZIP2:
                CompressorStreamFactory factory = new CompressorStreamFactory();
                try {
                    return factory.createCompressorInputStream(CompressorStreamFactory.BZIP2, new BufferedInputStream(is));
                } catch (CompressorException e) {
                    throw new IOException(e);
                }
        }
        throw new NullPointerException();
    }

    private static OutputStream createOutputStream(CompressionType ct, OutputStream os, int buffer) throws IOException {
        switch (ct) {
            case None:
                return os;
            case GZIP:
                return new GZIPOutputStream(os, buffer);
            case BZIP2:
                CompressorStreamFactory factory = new CompressorStreamFactory();
                try {
                    return factory.createCompressorOutputStream(CompressorStreamFactory.BZIP2, new BufferedOutputStream(os));
                } catch (CompressorException e) {
                    throw new IOException(e);
                }
        }
        throw new NullPointerException();
    }
}
