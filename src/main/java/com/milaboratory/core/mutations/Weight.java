package com.milaboratory.core.mutations;

/**
 * @author Dmitry Bolotin
 * @author Stanislav Poslavsky
 */
public interface Weight {
    int weight(int position);

    Weight ONE = new Weight() {
        @Override
        public int weight(int position) {
            return 1;
        }
    };
}
