package com.milaboratory.core.alignment;

import com.milaboratory.core.io.util.IOTestUtil;
import com.milaboratory.util.GlobalObjectMappers;
import org.junit.Test;

import static com.milaboratory.core.sequence.SequenceQuality.BAD_QUALITY_VALUE;
import static com.milaboratory.core.sequence.SequenceQuality.GOOD_QUALITY_VALUE;
import static org.junit.Assert.*;

public class PatternAndTargetAlignmentScoringTest {
    @Test
    public void serializationTest() throws Exception {
        PatternAndTargetAlignmentScoring expected = new PatternAndTargetAlignmentScoring(0, -1,
                -1, true, GOOD_QUALITY_VALUE, BAD_QUALITY_VALUE, -1);
        String s = GlobalObjectMappers.PRETTY.writeValueAsString(expected);
        PatternAndTargetAlignmentScoring scoring = GlobalObjectMappers.ONE_LINE.readValue(s,
                PatternAndTargetAlignmentScoring.class);
        assertEquals(expected, scoring);
        IOTestUtil.assertJavaSerialization(expected);
        IOTestUtil.assertJavaSerialization(scoring);
    }
}
