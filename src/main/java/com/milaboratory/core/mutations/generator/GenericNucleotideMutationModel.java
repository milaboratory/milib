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
package com.milaboratory.core.mutations.generator;

import com.milaboratory.core.mutations.Mutation;
import com.milaboratory.core.sequence.Wildcard;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.commons.math3.random.Well19937c;

import java.util.Arrays;

import static com.milaboratory.core.sequence.NucleotideSequence.ALPHABET;
import static java.lang.System.currentTimeMillis;

public final class GenericNucleotideMutationModel implements NucleotideMutationModel, java.io.Serializable {
    private final RandomGenerator generator;
    private final double[] events;
    private final double insertionProbability;

    public GenericNucleotideMutationModel(SubstitutionModel substitutionModel, double deletionProbability, double insertionProbability) {
        this(substitutionModel, deletionProbability, insertionProbability, currentTimeMillis());
    }

    public GenericNucleotideMutationModel(SubstitutionModel substitutionModel, double deletionProbability, double insertionProbability, long seed) {
        this.generator = new Well19937c(seed);
        this.insertionProbability = insertionProbability;

        this.events = new double[4 * 5];
        int i, j;
        double noEventProbability, sum;
        for (i = 0; i < 4; ++i) {
            sum = insertionProbability;
            events[i * 5] = (sum += deletionProbability);

            noEventProbability = 1.0 - sum - substitutionModel.getTotalSubstitutionProbability(i);

            for (j = 0; j < 4; ++j)
                if (i != j)
                    events[i * 5 + 1 + j] = (sum += substitutionModel.getProbability(i, j));
                else
                    events[i * 5 + 1 + j] = (sum += noEventProbability);

            assert sum <= 1.0001 & sum >= 0.9999;
        }
    }

    GenericNucleotideMutationModel(double[] events, double insertionProbability) {
        this(events, insertionProbability, currentTimeMillis());
    }

    GenericNucleotideMutationModel(double[] events, double insertionProbability, long seed) {
        this.generator = new Well19937c(seed);
        this.events = events;
        this.insertionProbability = insertionProbability;
    }

    @Override
    public int generateMutation(int position, int inputLetter) {
        double r = generator.nextDouble();

        if (insertionProbability > r)
            return Mutation.createInsertion(position, generator.nextInt(4));

        int basicInputLetter;
        Wildcard wildcard = null;
        if (inputLetter < ALPHABET.basicSize())
            basicInputLetter = inputLetter;
        else {
            wildcard = ALPHABET.codeToWildcard((byte)inputLetter);
            basicInputLetter = wildcard.getMatchingCode(generator.nextInt(wildcard.basicSize()));
        }
        if (basicInputLetter >= 0) {
            int event = Arrays.binarySearch(events, basicInputLetter * 5, (basicInputLetter + 1) * 5, r);
            if (event < 0)
                event = -event - 1;

            event -= basicInputLetter * 5;

            if (event == 0)
                return Mutation.createDeletion(position, inputLetter);

            if ((wildcard == null) ? ((event - 1) != inputLetter) : !wildcard.matches((byte)(event - 1)))
                return Mutation.createSubstitution(position, inputLetter, (event - 1));
        }

        return Mutation.NON_MUTATION;
    }

    @Override
    public GenericNucleotideMutationModel multiplyProbabilities(double factor) {
        double[] newEvents = new double[4 * 5];
        for (int i = 0; i < 20; ++i)
            newEvents[i] = events[i] * factor;

        int j;
        double delta;
        for (int i = 0; i < 4; ++i) {
            delta = newEvents[(i + 1) * 5 - 1] - 1.0;
            for (j = i + 1; j < 5; ++j)
                newEvents[i * 5 + j] -= delta;
        }

        return new GenericNucleotideMutationModel(newEvents, insertionProbability * factor, generator.nextLong());
    }

    @Override
    public void reseed(long seed) {
        generator.setSeed(seed);
    }

    @Override
    public NucleotideMutationModel clone() {
        return new GenericNucleotideMutationModel(events, insertionProbability);
    }
}
