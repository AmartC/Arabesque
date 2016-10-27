package io.arabesque.computation;

import io.arabesque.embedding.Embedding;
import io.arabesque.embedding.VertexInducedEmbedding;

public abstract class VertexInducedSFSamplingComputation<E extends VertexInducedEmbedding> extends SFSamplingComputation<E> {
    @Override
    protected final int getInitialNumWords() {
        return getMainGraph().getNumberVertices();
    }

    @Override
    public Class<? extends Embedding> getEmbeddingClass() {
        return VertexInducedEmbedding.class;
    }
}
