package com.milaboratory.core.tree;

import com.milaboratory.core.mutations.Mutations;
import com.milaboratory.core.mutations.MutationsBuilder;
import com.milaboratory.core.sequence.Sequence;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Created by dbolotin on 25/06/14.
 */
public final class NeighborhoodIterator<O, S extends Sequence<? extends S>> {
    //Reference sequence
    final S reference;

    //Penalty & other restrictions
    final TreeSearchParameters parameters;
    final byte[][] branchingSequences;
    final MutationGuide guide;

    //Runtime data
    int branchingSequenceIndex = 0, lastEnumerator;
    SequenceTreeMap.Node<O> root;
    BranchingEnumerator<O, S>[] branchingEnumerators = new BranchingEnumerator[1];

    /**
     * Constrictor for root NeighborhoodIterator iterator.
     *
     * @param reference reference sequence
     * @param root      root node of the tree
     */
    public NeighborhoodIterator(S reference, TreeSearchParameters parameters,
                                MutationGuide guide, SequenceTreeMap.Node<O> root) {
        this.parameters = parameters;
        this.guide = guide;
        this.reference = reference;
        this.root = root;
        this.branchingSequences = parameters.getDifferencesCombination();
        this.branchingEnumerators[0] = new BranchingEnumerator<>(reference, guide);

        setupBranchingEnumerators();
    }

    /**
     * Ensures capacity for storing BranchingEnumerators.
     *
     * @param newSize desired size
     */
    private void ensureCapacity(int newSize) {
        int oldSize;
        if ((oldSize = branchingEnumerators.length) < newSize) {
            branchingEnumerators = Arrays.copyOfRange(branchingEnumerators, 0, newSize);
            for (int i = oldSize; i < newSize; ++i)
                branchingEnumerators[i] = new BranchingEnumerator<>(reference, guide);
        }
    }

    /**
     * Setts up BranchingEnumerators for current branching sequence
     */
    private void setupBranchingEnumerators() {
        //Getting required sequence of differences (mutations)
        final byte[] bSequence = branchingSequences[branchingSequenceIndex];

        //Ensure number of branching enumerators
        ensureCapacity(bSequence.length);

        //Setting up initial branching enumerators
        byte previous = -1, current;
        for (int i = 0; i < bSequence.length; ++i) {
            current = bSequence[i];

            branchingEnumerators[i].setup(current,
                    (previous == 1 && current == 2) || // prevents insertion right after deletion
                            (previous == 2 && current == 1) || // prevents deletion right after insertion
                            (previous == 2 && current == 0) // prevents mismatch right after insertion
            );

            previous = bSequence[i];
        }

        branchingEnumerators[0].reset(0, root);

        lastEnumerator = bSequence.length - 1;
    }

    public O next() {
        SequenceTreeMap.Node<O> n = nextNode();
        return n == null ? null : n.object;
    }

    public SequenceTreeMap.Node<O> nextNode() {
        if (branchingSequenceIndex == branchingSequences.length)
            return null;

        SequenceTreeMap.Node<O> n;

        while (true) {

            if (lastEnumerator == -1) {
                --lastEnumerator;
                if ((n = traverseToTheEnd(root, 0)) != null && n.object != null)
                    return n;
            }

            int i = lastEnumerator;

            INNER:
            while (i >= 0) {
                for (; i < lastEnumerator; ++i)
                    if ((n = branchingEnumerators[i].next()) != null)
                        branchingEnumerators[i + 1].reset(branchingEnumerators[i].getNextPositionAfterBranching(), n);
                    else {
                        --i;
                        continue INNER;
                    }

                assert i == lastEnumerator;

                if ((n = branchingEnumerators[i].next()) != null)
                    if ((n = traverseToTheEnd(n, branchingEnumerators[i].getNextPositionAfterBranching())) != null && n.object != null)
                        return n;
                    else
                        continue;
                else
                    --i;
            }

            if ((++branchingSequenceIndex) >= branchingSequences.length ||
                    getPenalty() > parameters.getMaxPenalty()) {
                branchingSequenceIndex = branchingSequences.length;
                return null;
            } else
                setupBranchingEnumerators();
        }
    }

    public SequenceTreeMap.Node<O> traverseToTheEnd(SequenceTreeMap.Node<O> node, int position) {
        while (position < reference.size())
            if ((node = node.links[reference.codeAt(position++)]) == null)
                break;

        return node;
    }

    public byte[] getCurrentBranchingSequence() {
        return branchingSequences[branchingSequenceIndex];
    }

    public int getMutationsCount() {
        return branchingSequences[branchingSequenceIndex].length;
    }

    public byte getType(int i) {
        return branchingSequences[branchingSequenceIndex][i];
    }

    public Mutations<S> getCurrentMutations() {
        if (lastEnumerator < 0)
            return (Mutations)new Mutations(reference.getAlphabet());

        MutationsBuilder<S> builder = (MutationsBuilder)(new MutationsBuilder(reference.getAlphabet())
                .ensureCapacity(lastEnumerator + 1));

        for (int i = 0; i <= lastEnumerator; ++i) {
            BranchingEnumerator<O, S> currentBE = branchingEnumerators[i];
            int position = currentBE.getPosition();
            switch (getCurrentBranchingSequence()[i]) {
                case 0:
                    builder.appendSubstitution(
                            position,
                            reference.codeAt(position),
                            currentBE.code);
                    break;
                case 1:
                    builder.appendDeletion(position,
                            reference.codeAt(position));
                    break;
                case 2:
                    builder.appendInsertion(position, currentBE.code);
                    break;
                default:
                    throw new RuntimeException();
            }
        }

        return builder.createAndDestroy();
    }

    public int getPosition(int i) {
        return branchingEnumerators[i].getPosition();
    }

    public byte getCode(int i) {
        return branchingEnumerators[i].code;
    }

    public int getMismatches() {
        int ret = 0;

        for (byte b : getCurrentBranchingSequence())
            if (b == 0)
                ++ret;

        return ret;
    }

    public int getDeletions() {
        int ret = 0;

        for (byte b : getCurrentBranchingSequence())
            if (b == 1)
                ++ret;

        return ret;
    }

    public int getInsertions() {
        int ret = 0;

        for (byte b : getCurrentBranchingSequence())
            if (b == 2)
                ++ret;

        return ret;
    }

    public int[] getIntroducedDifferences() {
        int[] ret = new int[3];

        for (byte b : getCurrentBranchingSequence())
            ++ret[b];

        return ret;
    }

    public double getPenalty() {
        double p = 0.0;

        //Getting required sequence of differences (mutations)
        final byte[] bSequence = branchingSequences[branchingSequenceIndex];

        //Calculating penalty
        for (int i = bSequence.length - 1; i >= 0; --i)
            p += parameters.getPenalty(bSequence[i]);

        return p;
    }

    public Iterable<O> it() {
        return new Iterable<O>() {
            @Override
            public Iterator<O> iterator() {
                return new NeighbourhoodIteratorWrapper<>(NeighborhoodIterator.this);
            }
        };
    }

    private static final class NeighbourhoodIteratorWrapper<O, S extends Sequence<? extends S>> implements java.util.Iterator<O> {
        final NeighborhoodIterator<O, S> iterator;
        O next;

        private NeighbourhoodIteratorWrapper(NeighborhoodIterator<O, S> iterator) {
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() {
            return (next = iterator.next()) != null;
        }

        @Override
        public O next() {
            return next;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}