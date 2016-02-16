/*
 * Copyright 2016 MiLaboratory.com
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

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY, isGetterVisibility = JsonAutoDetect.Visibility.NONE,
        getterVisibility = JsonAutoDetect.Visibility.NONE)
public final class VariantsAssemblerParameters {
    private int minimalPairCount;
    private int minimalClonesInAllele;
    private AggregatedMutations.MutationsFilter mutationsFilter;
    private float minimalAssignmentScore;
    private float mutationMatchWeight;
    private float nullMutationMatchWeight;
    private int maxDepth;

    @JsonCreator
    public VariantsAssemblerParameters(@JsonProperty("minimalPairCount") int minimalPairCount,
                                       @JsonProperty("minimalClonesInAllele") int minimalClonesInAllele,
                                       @JsonProperty("mutationsFilter") AggregatedMutations.MutationsFilter mutationsFilter,
                                       @JsonProperty("minimalAssignmentScore") float minimalAssignmentScore,
                                       @JsonProperty("mutationMatchWeight") float mutationMatchWeight,
                                       @JsonProperty("nullMutationMatchWeight") float nullMutationMatchWeight,
                                       @JsonProperty("maxDepth") int maxDepth) {
        this.minimalPairCount = minimalPairCount;
        this.minimalClonesInAllele = minimalClonesInAllele;
        this.mutationsFilter = mutationsFilter;
        this.minimalAssignmentScore = minimalAssignmentScore;
        this.mutationMatchWeight = mutationMatchWeight;
        this.nullMutationMatchWeight = nullMutationMatchWeight;
        this.maxDepth = maxDepth;
    }

    public int getMinimalPairCount() {
        return minimalPairCount;
    }

    public VariantsAssemblerParameters setMinimalPairCount(int minimalPairCount) {
        this.minimalPairCount = minimalPairCount;
        return this;
    }

    public AggregatedMutations.MutationsFilter getMutationsFilter() {
        return mutationsFilter;
    }

    public VariantsAssemblerParameters setMutationsFilter(AggregatedMutations.MutationsFilter mutationsFilter) {
        this.mutationsFilter = mutationsFilter;
        return this;
    }

    public float getMutationMatchWeight() {
        return mutationMatchWeight;
    }

    public VariantsAssemblerParameters setMutationMatchWeight(float mutationMatchWeight) {
        this.mutationMatchWeight = mutationMatchWeight;
        return this;
    }

    public float getNullMutationMatchWeight() {
        return nullMutationMatchWeight;
    }

    public VariantsAssemblerParameters setNullMutationMatchWeight(float nullMutationMatchWeight) {
        this.nullMutationMatchWeight = nullMutationMatchWeight;
        return this;
    }

    public int getMinimalClonesInAllele() {
        return minimalClonesInAllele;
    }

    public VariantsAssemblerParameters setMinimalClonesInAllele(int minimalClonesInAllele) {
        this.minimalClonesInAllele = minimalClonesInAllele;
        return this;
    }

    public float getMinimalAssignmentScore() {
        return minimalAssignmentScore;
    }

    public VariantsAssemblerParameters setMinimalAssignmentScore(float minimalAssignmentScore) {
        this.minimalAssignmentScore = minimalAssignmentScore;
        return this;
    }

    public int getMaxDepth() {
        return maxDepth;
    }

    public VariantsAssemblerParameters setMaxDepth(int maxDepth) {
        this.maxDepth = maxDepth;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof VariantsAssemblerParameters)) return false;

        VariantsAssemblerParameters that = (VariantsAssemblerParameters) o;

        if (minimalPairCount != that.minimalPairCount) return false;
        if (minimalClonesInAllele != that.minimalClonesInAllele) return false;
        if (Float.compare(that.minimalAssignmentScore, minimalAssignmentScore) != 0) return false;
        if (Float.compare(that.mutationMatchWeight, mutationMatchWeight) != 0) return false;
        if (Float.compare(that.nullMutationMatchWeight, nullMutationMatchWeight) != 0) return false;
        return mutationsFilter.equals(that.mutationsFilter);

    }

    @Override
    public int hashCode() {
        int result = minimalPairCount;
        result = 31 * result + minimalClonesInAllele;
        result = 31 * result + mutationsFilter.hashCode();
        result = 31 * result + (minimalAssignmentScore != +0.0f ? Float.floatToIntBits(minimalAssignmentScore) : 0);
        result = 31 * result + (mutationMatchWeight != +0.0f ? Float.floatToIntBits(mutationMatchWeight) : 0);
        result = 31 * result + (nullMutationMatchWeight != +0.0f ? Float.floatToIntBits(nullMutationMatchWeight) : 0);
        return result;
    }
}
