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
package com.milaboratory.core.mutations;

import com.milaboratory.core.sequence.Alphabet;
import com.milaboratory.primitivio.PrimitivI;
import com.milaboratory.primitivio.PrimitivO;
import com.milaboratory.primitivio.Serializer;

class IO {
    private IO() {
    }

    public static class MutationsSerializer implements Serializer<Mutations> {
        @Override
        public void write(PrimitivO output, Mutations object) {
            output.writeObject(object.alphabet);
            output.writeObject(object.mutations);
        }

        @Override
        public Mutations read(PrimitivI input) {
            Alphabet alphabet = input.readObject(Alphabet.class);
            return new Mutations(alphabet, input.readObject(int[].class), true);
        }

        @Override
        public boolean isReference() {
            return false;
        }

        @Override
        public boolean handlesReference() {
            return false;
        }
    }
}
