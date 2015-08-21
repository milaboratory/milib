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
package com.milaboratory.core.alignment;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.milaboratory.core.sequence.Alphabet;
import com.milaboratory.core.sequence.Sequence;

import java.util.Arrays;

/**
 * AbstractAlignmentScoring - abstract scoring system class used for alignment procedure.
 *
 * @param <S> type of sequences to be aligned using scoring system
 */
public class AbstractAlignmentScoring<S extends Sequence<S>> implements AlignmentScoring<S>, java.io.Serializable {
    /**
     * Link to alphabet
     */
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    protected final Alphabet<S> alphabet;

    /**
     * Substitution matrix
     */
    @JsonSerialize(using = ScoringMatrixIO.Serializer.class)
    @JsonDeserialize(using = ScoringMatrixIO.Deserializer.class)
    protected final int[] subsMatrix;

    /**
     * Flag indicating whether substitution matrix has the same value on main diagonal or not
     */
    final boolean uniformBasicMatch;

    /**
     * Abstract class constructor. Used in deserialization.
     *
     * <p>Initializes substitution matrix to {@code null} and uniformBasicMatch to {@code true}</p>
     *
     * @param alphabet alphabet to be used by scoring system
     */
    protected AbstractAlignmentScoring(Alphabet<S> alphabet) {
        this.alphabet = alphabet;
        this.subsMatrix = null;
        this.uniformBasicMatch = true;
    }

    /**
     * Abstract class constructor. <p>Initializes uniformBasicMatch to {@code true}</p>
     *
     * @param alphabet   alphabet to be used by scoring system
     * @param subsMatrix substitution matrix
     */
    public AbstractAlignmentScoring(Alphabet<S> alphabet, int[] subsMatrix) {
        int size = alphabet.size();

        //For deserialization see ScoringMatrixIO.Deserializer
        if (subsMatrix.length == 2)
            subsMatrix = ScoringUtils.getSymmetricMatrix(subsMatrix[0], subsMatrix[1], alphabet);
        else {
            //Normal arguments check
            if (subsMatrix.length != size * size)
                throw new IllegalArgumentException();
            subsMatrix = subsMatrix.clone();
        }

        this.alphabet = alphabet;
        this.subsMatrix = subsMatrix;

        // Setting uniformity of match score flag
        int val = getScore((byte) 0, (byte) 0);
        boolean e = true;
        for (byte i = (byte) (alphabet.basicSize() - 1); i > 0; --i)
            if (getScore(i, i) != val) {
                e = false;
                break;
            }
        this.uniformBasicMatch = e;
    }

    /**
     * Returns score value for specified alphabet letter codes
     *
     * @param from code of letter which is to be replaced
     * @param to   code of letter which is replacing
     * @return score value
     */
    public int getScore(byte from, byte to) {
        return subsMatrix[from * alphabet.size() + to];
    }

    /**
     * Returns alphabet
     *
     * @return alphabet
     */
    public Alphabet<S> getAlphabet() {
        return alphabet;
    }

    /**
     * Returns @code{true} if @code{getScore(i, i)} returns the same score for all basic letters values of @code{i}.
     *
     * @return @code{true} if @code{getScore(i, i)} returns the same score for all basic letters values of @code{i}
     */
    public boolean uniformBasicMatchScore() {
        return uniformBasicMatch;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AbstractAlignmentScoring that = (AbstractAlignmentScoring) o;

        if (getAlphabet() != ((AbstractAlignmentScoring) o).getAlphabet())
            return false;

        return Arrays.equals(subsMatrix, that.subsMatrix);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(subsMatrix) + 31 * getAlphabet().hashCode();
    }
}
