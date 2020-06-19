/*
 * Copyright 2020 MiLaboratory, LLC
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

import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.custom_hash.TObjectIntCustomHashMap;

import java.util.ArrayList;

public final class PrimitivIOStateBuilder implements HasPrimitivIOState {
    private final SerializersManager manager = new SerializersManager();
    private final ArrayList<Object> knownReferences = new ArrayList<>();
    private final ArrayList<Object> knownObjects = new ArrayList<>();

    public void registerCustomSerializer(Class<?> type, Serializer<?> customSerializer) {
        manager.registerCustomSerializer(type, customSerializer);
    }

    @Override
    public void putKnownReference(Object ref) {
        knownReferences.add(ref);
    }

    @Override
    public void putKnownObject(Object ref) {
        knownObjects.add(ref);
    }

    public PrimitivIState getIState() {
        return new PrimitivIState(manager, knownReferences, knownObjects); // all params cloned in constructor
    }

    public PrimitivOState getOState() {
        TObjectIntCustomHashMap<Object> knownReferences = PrimitivOState.newKnownReferenceHashMap();
        for (int i = 0; i < this.knownReferences.size(); i++)
            knownReferences.put(this.knownReferences.get(i), i);

        TObjectIntMap<Object> knownObjects = PrimitivOState.newKnownObjectHashMap();
        for (int i = 0; i < this.knownObjects.size(); i++)
            knownReferences.put(this.knownObjects.get(i), i);

        return new PrimitivOState(manager, knownReferences, knownObjects); // all params cloned in constructor
    }
}
