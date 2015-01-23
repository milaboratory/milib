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

import com.milaboratory.core.sequence.NucleotideSequence;

import java.util.ArrayList;
import java.util.List;

/**
 * <p>KAligner - class which performs comprehensive alignment of sequence.</p>
 * <p/>
 * <p>Complete alignment of sequence performed using {@link com.milaboratory.core.alignment.KMapper#addReference(com.milaboratory.core.sequence.NucleotideSequence, int, int)}
 * method from which preliminary hits are obtained and used by {@link #align(com.milaboratory.core.sequence.NucleotideSequence)},
 * {@link #align(com.milaboratory.core.sequence.NucleotideSequence, int, int)},
 * {@link #align(com.milaboratory.core.sequence.NucleotideSequence, int, int, boolean)}
 * methods.</p>
 * <p/>
 * <p>All settings are stored in {@link #parameters} property.</p>
 */
public class KAligner {
    /**
     * Link to KMapper
     */
    final KMapper mapper;
    /**
     * Parameters of alignment
     */
    final KAlignerParameters parameters;
    /**
     * Base records for reference sequences
     */
    final List<NucleotideSequence> sequences = new ArrayList<>();
    /**
     * Flag indicating how to load final alignments - at first request or immediately after obtaining {@link com.milaboratory.core.alignment.KAlignmentResult}
     */
    final boolean lazyResults;

    /**
     * <p>Creates new KAligner.</p>
     * <p>Sets {@link #lazyResults} flag to {@code false}, which means that all alignments inside {@link com.milaboratory.core.alignment.KAlignmentResult}
     * obtained by {@link com.milaboratory.core.alignment.KAligner#align(com.milaboratory.core.sequence.NucleotideSequence, int, int, boolean)} method
     * will be loaded immediately.
     * </p>
     *
     * @param parameters parameters from which new KAligner needs to be created
     */
    public KAligner(KAlignerParameters parameters) {
        this(parameters, false);
    }

    /**
     * <p>Creates new KAligner.</p>
     *
     * @param parameters  parameters from which new KAligner needs to be created
     * @param lazyResults {@code true} if all alignments inside {@link com.milaboratory.core.alignment.KAlignmentResult}
     *                    obtained by {@link com.milaboratory.core.alignment.KAligner#align(com.milaboratory.core.sequence.NucleotideSequence, int, int, boolean)} method
     *                    need to be loaded at first request
     */
    public KAligner(KAlignerParameters parameters, boolean lazyResults) {
        this.mapper = KMapper.createFromParameters(parameters);
        this.parameters = parameters.clone();
        this.lazyResults = lazyResults;
    }

    /**
     * Adds new reference sequence to the base of this aligner and returns index assigned to it.
     *
     * @param sequence sequence
     * @return index assigned to the sequence
     */
    public int addReference(NucleotideSequence sequence) {
        return addReference(sequence, 0, sequence.size());
    }

    /**
     * Adds new reference sequence to the base of this mapper and returns index assigned to it.
     * <p/>
     * <p>User can specify a part of a sequence to be indexed by {@link com.milaboratory.core.alignment.KMapper},
     * but {@link com.milaboratory.core.alignment.KAligner} stores whole adding sequences.</p>
     *
     * @param sequence sequence
     * @param offset   offset of subsequence to be indexed by {@link com.milaboratory.core.alignment.KMapper}
     * @param length   length of subsequence to be indexed by {@link com.milaboratory.core.alignment.KMapper}
     * @return index assigned to the sequence
     */
    public int addReference(NucleotideSequence sequence, int offset, int length) {
        int id = mapper.addReference(sequence, offset, length);
        assert sequences.size() == id;
        sequences.add(sequence);
        return id;
    }

    /**
     * Returns sequence by its id (order number) in a base.
     *
     * @param id id of sequence to be returned
     * @return sequence
     */
    public NucleotideSequence getReference(int id) {
        return sequences.get(id);
    }

    /**
     * Performs a comprehensive alignment of a sequence.
     * <p/>
     * <p>The procedure consists of 2 steps:</p>
     * <ul>
     * <li>1. Obtaining {@link com.milaboratory.core.alignment.KMappingResult} from {@link com.milaboratory.core.alignment.KMapper}
     * using {@link com.milaboratory.core.alignment.KMapper#align(com.milaboratory.core.sequence.NucleotideSequence)} which contains preliminary hits
     * </li>
     * <li>2. Using {@link com.milaboratory.core.alignment.KMappingResult} from step 1, obtaining {@link com.milaboratory.core.alignment.KAlignmentResult}
     * by {@link #align(com.milaboratory.core.sequence.NucleotideSequence, int, int, boolean)} method,
     * where all hit alignments may be loaded lazily (at first request) or immediately (depends on {@link #lazyResults} flag value)
     * </li>
     * </ul>
     *
     * @param sequence sequence to be aligned
     * @return a list of hits found in target sequence
     */
    public KAlignmentResult align(NucleotideSequence sequence) {
        return align(sequence, 0, sequence.size());
    }

    /**
     * Performs a comprehensive alignment of a sequence.
     * <p/>
     * <p>The procedure consists of 2 steps:</p>
     * <ul>
     * <li>1. Obtaining {@link com.milaboratory.core.alignment.KMappingResult} from {@link com.milaboratory.core.alignment.KMapper}
     * using {@link com.milaboratory.core.alignment.KMapper#align(com.milaboratory.core.sequence.NucleotideSequence)} which contains preliminary hits
     * </li>
     * <li>2. Using {@link com.milaboratory.core.alignment.KMappingResult} from step 1, obtaining {@link com.milaboratory.core.alignment.KAlignmentResult}
     * by {@link #align(com.milaboratory.core.sequence.NucleotideSequence, int, int, boolean)} method,
     * where all hit alignments may be loaded lazily (at first request) or immediately (depends on {@link #lazyResults} flag value)
     * </li>
     * </ul>
     *
     * @param sequence sequence to be aligned
     * @param from     first nucleotide to be aligned (inclusive)
     * @param to       last nucleotide to be aligned (exclusive)
     * @return a list of hits found in target sequence
     */
    public KAlignmentResult align(NucleotideSequence sequence, int from, int to) {
        return align(sequence, from, to, true);
    }

    /**
     * Performs a comprehensive alignment of a sequence.
     * <p/>
     * <p>The procedure consists of 2 steps:</p>
     * <ul>
     * <li>1. Obtaining {@link com.milaboratory.core.alignment.KMappingResult} from {@link com.milaboratory.core.alignment.KMapper}
     * using {@link com.milaboratory.core.alignment.KMapper#align(com.milaboratory.core.sequence.NucleotideSequence)} which contains preliminary hits
     * </li>
     * <li>2. Using {@link com.milaboratory.core.alignment.KMappingResult} from step 1, obtaining {@link com.milaboratory.core.alignment.KAlignmentResult}
     * by {@link #align(com.milaboratory.core.sequence.NucleotideSequence, int, int, boolean)} method,
     * where all hit alignments may be loaded lazily (at first request) or immediately (depends on {@link #lazyResults} flag value)
     * </li>
     * </ul>
     *
     * @param sequence        sequence to be aligned
     * @param from            first nucleotide to be aligned by {@link com.milaboratory.core.alignment.KMapper} (inclusive)
     * @param to              last nucleotide to be aligned by {@link com.milaboratory.core.alignment.KMapper}  (exclusive)
     * @param restrictToRange {@code} true if hits alignments from obtained {@link com.milaboratory.core.alignment.KAlignmentResult} should be
     *                        restricted by the same range ({@code from} - {@code to})
     * @return a list of hits found in target sequence
     */
    public KAlignmentResult align(NucleotideSequence sequence, int from, int to, boolean restrictToRange) {
        KMappingResult kResult = mapper.align(sequence, from, to);

        KAlignmentResult result;
        if (restrictToRange)
            result = new KAlignmentResult(this, kResult, sequence, from, to);
        else
            result = new KAlignmentResult(this, kResult, sequence, 0, sequence.size());

        if (!lazyResults)
            result.calculateAllAlignments();
        else
            result.sortAccordingToMapperScores();

        return result;
    }
}
