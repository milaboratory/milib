package com.milaboratory.core.alignment.kaligner1;

import com.milaboratory.core.alignment.batch.BatchAlignerWithBaseParameters;

/**
 * @author Stanislav Poslavsky
 */
public interface AbstractKAlignerParameters extends BatchAlignerWithBaseParameters {
    boolean isFloatingLeftBound();

    boolean isFloatingRightBound();

    float getRelativeMinScore();

    int getMaxHits();

    @Override
    AbstractKAlignerParameters clone();
}
