package io.arabesque.computation;

import io.arabesque.embedding.EdgeInducedEmbedding;
import io.arabesque.embedding.Embedding;

public abstract class EdgeInducedMHSamplingComputation<E extends EdgeInducedEmbedding> extends MHSamplingComputation<E> {
    @Override
    protected final int getInitialNumWords() {
        return getMainGraph().getNumberEdges();
    }

    @Override
    public Class<? extends Embedding> getEmbeddingClass() {
        return EdgeInducedEmbedding.class;
    }
}
