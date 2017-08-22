/*
 * Copyright 2017 MiLaboratory.com
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
package com.milaboratory.core.io.sequence;

/**
 * @author Aleksandr Popov
 */
public abstract class AbstractMultiWriter<R extends MultiRead> implements SequenceWriter<R> {
    private final SingleSequenceWriter[] writers;

    public AbstractMultiWriter(SingleSequenceWriter... writers) {
        for (SingleSequenceWriter writer : writers)
            if (writer == null)
                throw new NullPointerException();
        this.writers = writers;
    }

    @Override
    public void write(R multiRead) {
        for (int i = 0; i < multiRead.numberOfReads(); i++)
            writers[i].write(multiRead.getRead(i));
    }

    @Override
    public void flush() {
        RuntimeException ex = null;

        for (SingleSequenceWriter writer : writers)
            try {
                writer.flush();
            } catch (RuntimeException e) {
                ex = e;
            }

        if (ex != null)
            throw ex;
    }

    @Override
    public void close() {
        RuntimeException ex = null;

        for (SingleSequenceWriter writer : writers)
            try {
                writer.close();
            } catch (RuntimeException e) {
                ex = e;
            }

        if (ex != null)
            throw ex;
    }
}
