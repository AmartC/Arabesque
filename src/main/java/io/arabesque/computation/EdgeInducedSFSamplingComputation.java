package io.arabesque.computation;

import io.arabesque.embedding.EdgeInducedEmbedding;
import io.arabesque.embedding.Embedding;

public abstract class EdgeInducedSFSamplingComputation<E extends EdgeInducedEmbedding> extends SFSamplingComputation<E> {
    @Override
    protected final int getInitialNumWords() {
        return getMainGraph().getNumberEdges();
    }

    @Override
    public Class<? extends Embedding> getEmbeddingClass() {
        return EdgeInducedEmbedding.class;
    }
}
