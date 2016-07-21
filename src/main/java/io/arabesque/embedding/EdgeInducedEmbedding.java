package io.arabesque.embedding;

import io.arabesque.graph.Edge;
import io.arabesque.graph.MainGraph;
import io.arabesque.utils.ArticulationPoints;
import io.arabesque.utils.collection.IntArrayList;
import net.openhft.koloboke.collect.IntCollection;

import java.io.DataInput;
import java.io.ObjectInput;
import java.io.IOException;
import java.util.Arrays;

public class EdgeInducedEmbedding extends BasicEmbedding {
    private IntArrayList numVerticesAddedWithWord;

    // finding the articulation points in the embedding
    private ArticulationPoints articRunner;

    @Override
    public void init() {
        numVerticesAddedWithWord = new IntArrayList();

        super.init();
    }

    @Override
    public void reset() {
        super.reset();
        numVerticesAddedWithWord.clear();
    }

    @Override
    public void setFromEmbedding(Embedding other) {
       super.setFromEmbedding(other);
       numVerticesAddedWithWord = other.getNumWordsAddedWithWord();
    }

    @Override
    public IntArrayList getWords() {
        return getEdges();
    }

    @Override
    public int getNumWords() {
        return getNumEdges();
    }

    @Override
    public String toOutputString() {
        StringBuilder sb = new StringBuilder();

        int numEdges = getNumEdges();
        IntArrayList edges = getEdges();

        for (int i = 0; i < numEdges; ++i) {
            Edge edge = mainGraph.getEdge(edges.getUnchecked(i));
            sb.append(edge.getSourceId());
            sb.append("-");
            sb.append(edge.getDestinationId());
            sb.append(" ");
        }

        return sb.toString();
    }

    @Override
    public IntArrayList getNumWordsAddedWithWord() {
       return numVerticesAddedWithWord;
    }

    @Override
    public int getNumVerticesAddedWithExpansion() {
        return numVerticesAddedWithWord.getLastOrDefault(0);
    }

    @Override
    public int getNumEdgesAddedWithExpansion() {
        if (edges.isEmpty()) {
            return 0;
        }

        return 1;
    }

    @Override
    protected IntCollection getValidElementsForExpansion(int edgeId) {
        final Edge edge = mainGraph.getEdge(edgeId);
        IntArrayList expansions = new IntArrayList();
        expansions.addAll(mainGraph.getVertexNeighbourhood(edge.getSourceId()).getNeighbourEdges());
        expansions.addAll(mainGraph.getVertexNeighbourhood(edge.getDestinationId()).getNeighbourEdges());
        expansions.removeInt(edgeId);
        return expansions;
    }

    @Override
    protected IntCollection getValidElementsForContraction(int edgeId) {
        if (articRunner == null)
            articRunner = new ArticulationPoints(mainGraph);

        boolean[] ap = articRunner.articulationBrigdes (this);
        IntArrayList contractions = new IntArrayList();

        for (int i = 0; i < edges.size(); ++i)
            if (!ap[i]) contractions.add(edges.getUnchecked(i));

        return contractions;
    }

    @Override
    protected boolean areWordsNeighbours(int wordId1, int wordId2) {
        return mainGraph.areEdgesNeighbors(wordId1, wordId2);
    }


    /**
     * Add word and update the number of vertices in this embedding.
     *
     * @param word
     */
    @Override
    public void addWord(int word) {
        super.addWord(word);
        edges.add(word);
        updateVerticesAddition(word);
    }

    private void updateVerticesAddition(int word) {
        final Edge edge = mainGraph.getEdge(word);

        int numVerticesAdded = 0;

        boolean srcIsNew = false;
        boolean dstIsNew = false;

        if (!vertices.contains(edge.getSourceId())) {
            srcIsNew = true;
        }

        if (!vertices.contains(edge.getDestinationId())) {
            dstIsNew = true;
        }

        if (srcIsNew) {
            vertices.add(edge.getSourceId());
            ++numVerticesAdded;
        }

        if (dstIsNew) {
            vertices.add(edge.getDestinationId());
            ++numVerticesAdded;
        }

        numVerticesAddedWithWord.add(numVerticesAdded);
    }

    @Override
    public void removeLastWord() {
        if (getNumEdges() == 0) {
            return;
        }

        int numVerticesToRemove = numVerticesAddedWithWord.pop();
        vertices.removeLast(numVerticesToRemove);
        edges.removeLast();

        super.removeLastWord();
    }

    @Override
    public void removeWord(int word) {
        if (getNumEdges() == 0) {
            return;
        }

        if (getNumEdges() == 1 && edges.getUnchecked(0) == word) {
            reset();
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
        updateVerticesDeletion(idx);
        edges.remove(idx);

        super.removeWord(word);
    }


    /**
     * Updates the list of vertices of this embedding based on the deletion of an edge.
     *
     * @param positionDeleted the idx of the edge that was just deleted.
     */
    private void updateVerticesDeletion(int positionDeleted) {
        final int numEdges = getNumEdges();
        final int numVerticesAdded = numVerticesAddedWithWord.getUnchecked(positionDeleted);

        assert (numVerticesAdded!=2 || positionDeleted==0); // only the first edge can add 2 vertices

        if (numVerticesAdded==0) return;

        if (positionDeleted==0) {
            if (numEdges>1) {
                numVerticesAddedWithWord.remove(0);
                numVerticesAddedWithWord.setUnchecked(0, numVerticesAddedWithWord.getUnchecked(0)+1);
            } else {
                vertices.clear();
                numVerticesAddedWithWord.clear();
            }
            return;
        }

        int j = 0, i = 0;
        while (i < positionDeleted) {
            j += numVerticesAddedWithWord.getUnchecked(i);
            i++;
        }
        final int deletedVertex = vertices.getUnchecked(j);

        i++; // skip the deleted edge
        while (i < numEdges) {
            Edge edge = mainGraph.getEdge(edges.getUnchecked(i));
            if (edge.hasVertex(deletedVertex)) {
                numVerticesAddedWithWord.setUnchecked(i, numVerticesAddedWithWord.getUnchecked(i)+1);
            }
            i++;
        }
        numVerticesAddedWithWord.remove(positionDeleted);
    }


    @Override
    public void readFields(DataInput in) throws IOException {
        reset();

        edges.readFields(in);

        int numEdges = edges.size();

        for (int i = 0; i < numEdges; ++i) {
            updateVerticesAddition(edges.getUnchecked(i));
        }
    }

    @Override
    public void readExternal(ObjectInput objInput) throws IOException, ClassNotFoundException {
       readFields(objInput);
    }

    @Override
    public int getTotalNumWords(){
        return mainGraph.getNumberEdges();
    }

}
