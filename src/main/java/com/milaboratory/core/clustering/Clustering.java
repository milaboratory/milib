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
package com.milaboratory.core.clustering;

import com.milaboratory.core.sequence.Alphabet;
import com.milaboratory.core.sequence.Sequence;
import com.milaboratory.core.tree.NeighborhoodIterator;
import com.milaboratory.core.tree.SequenceTreeMap;
import com.milaboratory.core.tree.TreeSearchParameters;
import com.milaboratory.util.CanReportProgress;
import com.milaboratory.util.Factory;

import java.util.*;

import static com.milaboratory.core.tree.SequenceTreeMap.Node;

public final class Clustering<T, S extends Sequence<S>> implements CanReportProgress {
    final Collection<T> inputObjects;
    final SequenceExtractor<T, S> sequenceExtractor;
    final ClusteringStrategy<T, S> strategy;
    final List<Cluster<T>> clusters = new ArrayList<>();
    volatile int progress;

    public Clustering(Collection<T> inputObjects, SequenceExtractor<T, S> sequenceExtractor, ClusteringStrategy<T, S> strategy) {
        this.inputObjects = inputObjects;
        this.sequenceExtractor = sequenceExtractor;
        this.strategy = strategy;
    }

    @Override
    public double getProgress() {
        return (1.0 * progress) / inputObjects.size();
    }

    @Override
    public boolean isFinished() {
        return progress == inputObjects.size();
    }

    public List<Cluster<T>> performClustering() {
        try {
            if (inputObjects.isEmpty())
                return clusters;

            final Comparator<Cluster<T>> clusterComparator = getComparatorOfClusters(strategy, sequenceExtractor);
            // For performance
            final TreeSearchParameters params = strategy.getSearchParameters();
            final int maxDepth = strategy.getMaxClusterDepth();

            final List<T> objects = new ArrayList<>(inputObjects);
            Collections.sort(objects, getComparatorOfObjectsRegardingSequences(strategy, sequenceExtractor));

            @SuppressWarnings("unchecked")
            Alphabet<S> alphabet = sequenceExtractor.getSequence(objects.get(0)).getAlphabet();

            final Factory<T[]> arrayFactory = new Factory<T[]>() {
                @Override
                public T[] create() {
                    return (T[]) new Object[1];
                }
            };

            final SequenceTreeMap<S, T[]> tree = new SequenceTreeMap<>(alphabet);
            for (T object : objects) {
                T[] array = tree.createIfAbsent(sequenceExtractor.getSequence(object), arrayFactory);
                if (array[0] == null)
                    array[0] = object;
                else {
                    array = Arrays.copyOf(array, array.length + 1);
                    array[array.length - 1] = object;
                    tree.put(sequenceExtractor.getSequence(object), array);
                }
            }

            Node<T[]> current;

            final HashSet<Node<T[]>> processedNodes = new HashSet<>();
            ArrayList<Cluster<T>> previousLayer = new ArrayList<>(), nextLayer = new ArrayList<>(), tmp;

            T[] temp;
            boolean inTree;
            for (int i = 0; i < objects.size(); ++i) {
                this.progress = i;
                T object = objects.get(i);

                //checking whether object is already clusterized
                if ((temp = tree.get(sequenceExtractor.getSequence(object))) == null)
                    continue;
                inTree = false;
                for (T t : temp)
                    if (t == object) {
                        inTree = true;
                        break;
                    }
                if (!inTree)
                    continue;
                //<-object in not yet clusterized

                Cluster<T> tempCluster = new Cluster<>(object);
                clusters.add(tempCluster);
                previousLayer.clear();
                previousLayer.add(tempCluster);

                for (int depth = 0; depth < maxDepth; ++depth) {

                    nextLayer.clear();
                    for (Cluster<T> previousCluster : previousLayer) {

                        NeighborhoodIterator<S, T[]> iterator = tree
                                .getNeighborhoodIterator(sequenceExtractor
                                        .getSequence(previousCluster.head), params, null);
                        processedNodes.clear();

                        while ((current = iterator.nextNode()) != null) {
                            if (!processedNodes.add(current))
                                continue;

                            T[] currentObjects = current.getObject();
                            T matchedObject = null;
                            boolean allNulls = true;
                            for (int j = 0; j < currentObjects.length; j++) {
                                if (currentObjects[j] == null)
                                    continue;
                                matchedObject = currentObjects[j];

                                if (strategy.compare(previousCluster.head, matchedObject) <= 0
                                        || !strategy.canAddToCluster(previousCluster, matchedObject, iterator)) {
                                    allNulls = false;
                                    continue;
                                }

                                nextLayer.add(tempCluster = new Cluster<>(matchedObject, previousCluster));
                                previousCluster.add(tempCluster);
                                currentObjects[j] = null;
                            }
                            assert matchedObject != null;
                            if (allNulls)
                                tree.remove(sequenceExtractor.getSequence(matchedObject));
                        }

                        if (previousCluster.children != null)
                            Collections.sort(previousCluster.children, clusterComparator);
                    }

                    Collections.sort(nextLayer, clusterComparator);
                    tmp = nextLayer;
                    nextLayer = previousLayer;
                    previousLayer = tmp;
                }
            }

            return clusters;
        } finally {
            progress = inputObjects.size();
        }
    }

    public List<Cluster<T>> getClusters() {
        if (progress != inputObjects.size())
            throw new IllegalStateException("Not yet clustered.");
        return clusters;
    }

    static <T, S extends Sequence> Comparator<Cluster<T>>
    getComparatorOfClusters(final Comparator<T> objectComparator, final SequenceExtractor<T, S> extractor) {
        return new Comparator<Cluster<T>>() {
            @Override
            public int compare(Cluster<T> o1, Cluster<T> o2) {
                int i = objectComparator.compare(o2.head, o1.head);
                return i == 0 ?
                        extractor.getSequence(o2.head).compareTo(extractor.getSequence(o1.head))
                        : i;
            }
        };
    }

    static <T, S extends Sequence> Comparator<T>
    getComparatorOfObjectsRegardingSequences(final Comparator<T> objectComparator, final SequenceExtractor<T, S> extractor) {
        return new Comparator<T>() {
            @Override
            public int compare(T o1, T o2) {
                int i = objectComparator.compare(o2, o1);
                return i == 0 ?
                        extractor.getSequence(o2).compareTo(extractor.getSequence(o1))
                        : i;
            }
        };
    }


    public static <T, S extends Sequence<S>> List<Cluster<T>> performClustering(Collection<T> inputObjects,
                                                                                SequenceExtractor<T, S> sequenceExtractor,
                                                                                ClusteringStrategy<T, S> strategy) {
        return new Clustering<T, S>(inputObjects, sequenceExtractor, strategy).performClustering();
    }

}
