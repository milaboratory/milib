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
package com.milaboratory.util;

import cc.redberry.pipe.OutputPort;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;

/**
 * Created by dbolotin on 04/04/2017.
 */
public interface ObjectSerializer<O> {
    /**
     * Implementation may close stream.
     *
     * @param data   objects
     * @param stream output stream
     */
    void write(Collection<O> data, OutputStream stream);

    OutputPort<O> read(InputStream stream);
}
