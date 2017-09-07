package io.arabesque.gmlib.density;

import io.arabesque.computation.VertexInducedComputation;
import io.arabesque.conf.Configuration;
import io.arabesque.embedding.VertexInducedEmbedding;

public class SubgraphDensityComputation extends VertexInducedComputation<VertexInducedEmbedding> {
    private static final String MAXSIZE = "arabesque.density.maxsize";
    private static final String MAXMISSING = "arabesque.density.maxmissingedges";
    private static final int MAXSIZE_DEFAULT = 4;
    private static final int MAX_MISSING_EDGES = 2;

    int maxsize;
    int max_num_edges_missing;
    int num_edges;

    @Override
    public void init() {
        super.init();
        maxsize = Configuration.get().getInteger(MAXSIZE, MAXSIZE_DEFAULT);
        max_num_edges_missing = Configuration.get().getInteger(MAXMISSING, MAX_MISSING_EDGES);
        num_edges = 0;
    }

    @Override
    public boolean filter(VertexInducedEmbedding embedding) {
        return isValidEmbedding(embedding);
    }

    public boolean isValidEmbedding(VertexInducedEmbedding embedding) {
        num_edges += embedding.getNumEdgesAddedWithExpansion();
        int max_possible_edges = (embedding.getNumVertices() * (embedding.getNumVertices() - 1)) / 2;
        return (max_possible_edges - num_edges) <= max_num_edges_missing;
    }

    @Override
    public boolean shouldExpand(VertexInducedEmbedding embedding) {
        return embedding.getNumVertices() < maxsize;
    }

    @Override
    public void process(VertexInducedEmbedding embedding) {
        if (embedding.getNumVertices() == maxsize) {
            output(embedding);
        }
    }
}
