package io.arabesque.embedding;

import io.arabesque.utils.collection.IntArrayList;
import io.arabesque.utils.ArticulationPoints;
import io.arabesque.graph.Edge;
import net.openhft.koloboke.collect.IntCollection;
import net.openhft.koloboke.collect.set.hash.HashIntSet;
import net.openhft.koloboke.collect.set.hash.HashIntSets;
import net.openhft.koloboke.function.IntConsumer;

import java.io.DataInput;
import java.io.ObjectInput;
import java.io.IOException;

public class VertexInducedEmbedding extends BasicEmbedding {
    // Consumers {{
    private UpdateEdgesConsumer updateEdgesConsumer;
    // }}

    // Edge tracking for incremental modifications {{
    private IntArrayList numEdgesAddedWithWord;
    // }}
    //

    // finding the articulation points in the embedding
    private ArticulationPoints articRunner;

    @Override
    protected void init() {
        updateEdgesConsumer = new UpdateEdgesConsumer();
        numEdgesAddedWithWord = new IntArrayList();

        super.init();
    }

    @Override
    public void reset() {
        super.reset();
        numEdgesAddedWithWord.clear();
    }

    @Override
    public IntArrayList getWords() {
        return getVertices();
    }

    @Override
    public int getNumWords() {
        return getNumVertices();
    }

    @Override
    public IntArrayList getNumWordsAddedWithWord() {
       return numEdgesAddedWithWord;
    }

    @Override
    public void setFromEmbedding(Embedding other) {
       super.setFromEmbedding(other);
       numEdgesAddedWithWord = other.getNumWordsAddedWithWord();
    }

    @Override
    public String toOutputString() {
        StringBuilder sb = new StringBuilder();

        IntArrayList vertices = getVertices();

        for (int i = 0; i < vertices.size(); ++i) {
            sb.append(vertices.getUnchecked(i));
            sb.append(" ");
        }

        return sb.toString();
    }


    @Override
    public int getNumVerticesAddedWithExpansion() {
        if (vertices.isEmpty()) {
            return 0;
        }

        return 1;
    }

    @Override
    public int getNumEdgesAddedWithExpansion() {
        return numEdgesAddedWithWord.getLastOrDefault(0);
    }

    protected IntCollection getValidElementsForExpansion(int vertexId) {
        return mainGraph.getVertexNeighbours(vertexId);
    }

    protected IntCollection getValidElementsForContraction(int vertexId) {
       if (articRunner == null)
          articRunner = new ArticulationPoints(mainGraph);

       boolean[] ap = articRunner.articulationPoints (this);
       IntArrayList contractions = new IntArrayList();

       for (int i = 0; i < vertices.size(); ++i)
          if (!ap[i]) contractions.add(vertices.getUnchecked(i));

       return contractions;
    }

    @Override
    protected boolean areWordsNeighbours(int wordId1, int wordId2) {
        return mainGraph.isNeighborVertex(wordId1, wordId2);
    }

    @Override
    public void addWord(int word) {
        super.addWord(word);
        vertices.add(word);
        updateEdgesAddition(word, vertices.size() - 1);
    }

    @Override
    public void removeLastWord() {
        if (getNumVertices() == 0) {
            return;
        }

        int numEdgesToRemove = numEdgesAddedWithWord.pop();
        edges.removeLast(numEdgesToRemove);
        vertices.removeLast();

        super.removeLastWord();
    }

    @Override
    public void removeWord(int word) {
        if (getNumVertices() == 0) {
            return;
        }

        IntArrayList words = getWords();
        int numWords = words.size();

        int idx = 0;
        while (idx < numWords) {
            if (word == words.getUnchecked(idx))
                break;
            idx++;
        }
        updateEdgesDeletion(idx);
        vertices.remove (idx);

        super.removeWord(word);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        reset();

        vertices.readFields(in);

        int numVertices = vertices.size();

        for (int i = 0; i < numVertices; ++i) {
            updateEdgesAddition(vertices.getUnchecked(i), i);
        }
    }

    @Override
    public void readExternal(ObjectInput objInput) throws IOException, ClassNotFoundException {
       readFields(objInput);
    }

    /**
     * Updates the list of edges of this embedding based on the addition of a new vertex.
     *
     * @param newVertexId The id of the new vertex that was just added.
     */
    private void updateEdgesAddition(int newVertexId, int positionAdded) {
        IntArrayList vertices = getVertices();

        int addedEdges = 0;

        // For each vertex (except the last one added)
        for (int i = 0; i < positionAdded; ++i) {
            int existingVertexId = vertices.getUnchecked(i);

            updateEdgesConsumer.reset();
            mainGraph.forEachEdgeId(existingVertexId, newVertexId, updateEdgesConsumer);
            addedEdges += updateEdgesConsumer.getNumAdded();
        }

        numEdgesAddedWithWord.add(addedEdges);
    }

    /**
     * Updates the list of edges of this embedding based on the deletion of a vertex.
     *
     * @param positionDeleted the idx of the vertex that was just deleted.
     */
    private void updateEdgesDeletion(int positionDeleted) {
       int bla = 0;
        for (int i = 0; i < getNumEdges(); i++) {
              Edge edge = mainGraph.getEdge(edges.getUnchecked(i));
              if (edge.hasVertex(vertices.getUnchecked(positionDeleted))) {
               bla++;
            }
        }

         if (getNumEdges()-bla < getNumVertices()-2) 
            throw new RuntimeException (this.getPattern() + " impossible be connected: " + (getNumEdges()-bla) + " " + (getNumVertices()-2) + " " + bla);
            

        int j = 0, i = 0;
        while (i < getNumWords()) {
           int target = j + numEdgesAddedWithWord.getUnchecked(i);
           while (j < target) {
              Edge edge = mainGraph.getEdge(edges.getUnchecked(j));
              if (edge.hasVertex(vertices.getUnchecked(positionDeleted))) {
                 edges.remove(j);
                 int newNumEdgesAdded = numEdgesAddedWithWord.getUnchecked(i) - 1;
                 numEdgesAddedWithWord.setUnchecked(i, newNumEdgesAdded);
                 target--;
              } else {
                 j++;
              }
           }
           i++;
        }
        numEdgesAddedWithWord.remove(positionDeleted);
    }

    private class UpdateEdgesConsumer implements IntConsumer {
        private int numAdded;

        public void reset() {
            numAdded = 0;
        }

        public int getNumAdded() {
            return numAdded;
        }

        @Override
        public void accept(int i) {
            edges.add(i);
            ++numAdded;
        }
    }

    @Override
    public int getTotalNumWords(){
        return mainGraph.getNumberVertices();
    }
}
