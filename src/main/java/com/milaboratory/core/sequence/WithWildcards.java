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

import java.util.Collection;

/**
 * Interface specifies that alphabet contains wild cards.
 *
 * @see com.milaboratory.core.sequence.NucleotideAlphabet
 * @see com.milaboratory.core.sequence.WildcardSymbol
 */
public interface WithWildcards {
    /**
     * Returns a collection of all wildcards defined for this.
     *
     * @return a collection of all wildcards defined for this.
     */
    Collection<WildcardSymbol> getAllWildcards();

    /**
     * Returns a wildcard object for specified letter.
     *
     * @param symbol symbol
     * @return wildcard object for specified letter
     */
    WildcardSymbol getWildcardFor(char symbol);
}
