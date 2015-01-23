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
package com.milaboratory.core.sequence;

import gnu.trove.map.hash.TCharObjectHashMap;

import static java.lang.Character.toLowerCase;
import static java.lang.Character.toUpperCase;

final class WildcardMapBuilder {
    final TCharObjectHashMap<WildcardSymbol> map = new TCharObjectHashMap<>();

    WildcardMapBuilder addWildcard(char c, byte... codes) {
        char uC = toUpperCase(c);
        WildcardSymbol wcSymbol = new WildcardSymbol(uC, codes);
        map.put(toLowerCase(c), wcSymbol);
        map.put(uC, wcSymbol);
        return this;
    }

    WildcardMapBuilder addAlphabet(Alphabet alphabet) {
        for (byte i = 0; i < alphabet.size(); ++i) {
            char c = alphabet.symbolFromCode(i);
            map.put(c, new WildcardSymbol(c, i));
        }
        return this;
    }

    TCharObjectHashMap<WildcardSymbol> get() {
        return map;
    }
}
