package com.milaboratory.core.mutations;

import com.milaboratory.core.sequence.Sequence;

/**
 * @author Dmitry Bolotin
 * @author Stanislav Poslavsky
 */
public final class AssignedVariants<S extends Sequence<S>> {
    public final Mutations<S>[] alleles;
    public final VariantsAssembler.AlleleAssignmentResult[] assignments;

    public AssignedVariants(Mutations<S>[] alleles, VariantsAssembler.AlleleAssignmentResult[] assignments) {
        this.alleles = alleles;
        this.assignments = assignments;
    }
}
