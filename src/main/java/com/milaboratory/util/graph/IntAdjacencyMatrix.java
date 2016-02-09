package com.milaboratory.util.graph;

import java.util.Arrays;

/**
 * @author Dmitry Bolotin
 * @author Stanislav Poslavsky
 */
public final class IntAdjacencyMatrix {
    final int[] matrix;
    final int size;

    public IntAdjacencyMatrix(int size) {
        this(size, 0);
    }

    public IntAdjacencyMatrix(int size, int initialValue) {
        if (((long) size) * size >= Integer.MAX_VALUE)
            throw new RuntimeException("This is to big...");
        this.matrix = new int[size * size];
        this.size = size;
        if (initialValue != 0)
            Arrays.fill(this.matrix, initialValue);
    }


    public void set(int i, int j, int value) {
        matrix[i * size + j] = value;
    }

    public int get(int i, int j) {
        return matrix[i * size + j];
    }

    public int increment(int i, int j) {
        return ++matrix[i * size + j];
    }

    public int decrement(int i, int j) {
        return --matrix[i * size + j];
    }

    public int adjust(int i, int j, int delta) {
        return (matrix[i * size + j] += delta);
    }

    public AdjacencyMatrix filter(int threshold) {
        AdjacencyMatrix adj = new AdjacencyMatrix(size);
        for (int i = 0; i < size; i++)
            for (int j = 0; j < size; j++)
                if (get(i, j) >= threshold)
                    adj.setConnected(i, j);
        return adj;
    }
}
