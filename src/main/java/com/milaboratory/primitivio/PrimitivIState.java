/*
 * Copyright 2018 MiLaboratory.com
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

import org.apache.commons.io.input.NullInputStream;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.InputStream;
import java.util.ArrayList;

/**
 * Immutable object that holds PrimitivI stream state (known objects, known references and serialization manager).
 *
 * PrimitivI always return new instance of the object. This object preserves no references to the original stream.
 */
public final class PrimitivIState {
    public static final PrimitivIState INITIAL = new PrimitivIState(new SerializersManager(), new ArrayList<>(),
            new ArrayList<>());

    private final SerializersManager manager;

    private final ArrayList<Object> knownReferences;

    private final ArrayList<Object> knownObjects;

    public PrimitivIState(SerializersManager manager, ArrayList<Object> knownReferences, ArrayList<Object> knownObjects) {
        this.manager = manager.clone();
        this.knownReferences = new ArrayList<>(knownReferences);
        this.knownObjects = new ArrayList<>(knownObjects);
    }

    public PrimitivI createPrimitivI() {
        return createPrimitivI(new NullInputStream(0));
    }

    public PrimitivI createPrimitivI(DataInput output) {
        return new PrimitivI(output, manager.clone(), new ArrayList<>(knownReferences), new ArrayList<>(knownObjects));
    }

    public PrimitivI createPrimitivI(InputStream input) {
        return createPrimitivI((DataInput) new DataInputStream(input));
    }
}
