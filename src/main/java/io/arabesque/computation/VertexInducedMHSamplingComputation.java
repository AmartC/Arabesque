package io.arabesque.computation;

import io.arabesque.embedding.Embedding;
import io.arabesque.embedding.VertexInducedEmbedding;

public abstract class VertexInducedMHSamplingComputation<E extends VertexInducedEmbedding> extends MHSamplingComputation<E> {
    @Override
    protected final int getInitialNumWords() {
        return getMainGraph().getNumberVertices();
    }

    @Override
    public Class<? extends Embedding> getEmbeddingClass() {
        return VertexInducedEmbedding.class;
    }
}
