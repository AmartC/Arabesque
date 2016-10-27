package io.arabesque.embedding;

import io.arabesque.graph.Edge;
import io.arabesque.utils.ArticulationPoints;
import io.arabesque.utils.collection.IntArrayList;
import net.openhft.koloboke.collect.IntCollection;
import net.openhft.koloboke.collect.set.hash.HashIntSet;
import net.openhft.koloboke.collect.set.hash.HashIntSets;

import java.io.DataInput;
import java.io.IOException;
import java.io.ObjectInput;

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
    public void copy(Embedding other) {
       super.copy(other);
        numVerticesAddedWithWord = new IntArrayList(other.getNumWordsAddedWithWord());
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

        edges.removeLast();

        int numVerticesToRemove = numVerticesAddedWithWord.pop();
        vertices.removeLast(numVerticesToRemove);

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

        if (edges.getLast() == word) {
            removeLastWord();
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

        if (idx == numWords) {
            System.out.println("Problem Emb:" + edges + " word to remove:" + word);
        }
        assert (idx != numWords);

        if (edges.size() != numVerticesAddedWithWord.size()) {
            System.out.println("Problem Emb:" + edges + " numVerticesAddedWithWord:" + numVerticesAddedWithWord);
        }
        assert(edges.size()==numVerticesAddedWithWord.size());


        edges.remove(idx);
        updateVerticesDeletion(idx);

        super.removeWord(word);
    }


    /**
     * Updates the list of vertices of this embedding based on the deletion of an edge.
     *
     * @param positionDeleted the idx of the edge that was just deleted.
     */
    private void updateVerticesDeletion(int positionDeleted) {
        final int numEdges = getNumEdges();

        if (numVerticesAddedWithWord.getUnchecked(positionDeleted)==0) {
            numVerticesAddedWithWord.remove(positionDeleted);
            return;
        }

        IntArrayList order = articRunner.dfs(this);
        HashIntSet addedVertices = HashIntSets.newMutableSet();
        IntArrayList newEdges = new IntArrayList();

        vertices.clear();
        numVerticesAddedWithWord.clear();
        for (int i = 0; i < numEdges; i++) {
            newEdges.add(order.getUnchecked(i));
            Edge edge  = mainGraph.getEdge(order.getUnchecked(i));
            int numAdded = 0;
            if (addedVertices.add(edge.getSourceId())) {
                numAdded++;
                vertices.add(edge.getSourceId());
            }
            if (addedVertices.add(edge.getDestinationId())) {
                numAdded++;
                vertices.add(edge.getDestinationId());
            }
            numVerticesAddedWithWord.add(numAdded);
        }

        edges = newEdges;
    }
    /*
    private void updateVerticesDeletion(int positionDeleted) {
        final int numEdges = getNumEdges();
        final int numVertices = getNumVertices();
        final int numVerticesAdded = numVerticesAddedWithWord.getUnchecked(positionDeleted);
        final Edge edge  = mainGraph.getEdge(edges.getUnchecked(positionDeleted));

        assert (numVerticesAdded==2 && positionDeleted!=0); // only the first edge can add 2 vertices

        if (numVerticesAdded==0) {
            numVerticesAddedWithWord.remove(positionDeleted);
            return;
        }


        final int vertexSrc = edge.getSourceId();
        final int vertexDest = edge.getDestinationId();

        int vertexSrcPos = -1;
        int vertexDestPos = -1;
        int firstEdgeSrcPos = -1;
        int firstEdgeDestPos = -1;
        int newVertexSrcPos = -1;
        int newVertexDestPos = -1;

        boolean isVertexSrcDeleted = true;
        boolean isVertexDestDeleted = true;

        System.out.println("Src Id: " + vertexSrc);
        System.out.println("Dest Id: " + vertexDest);

        System.out.println("Vertices: " + vertices);
        System.out.println("Edges: " + edges);
        System.out.println("getNumWordsAddedWithWord: " + numVerticesAddedWithWord);

        int j = 0, i = 0;
        while (i < numEdges) {
            Edge secondEdge  = mainGraph.getEdge(edges.getUnchecked(i));

            if (secondEdge.hasVertex(vertexSrc)) {
                if (i!=positionDeleted && isVertexSrcDeleted) {
                    isVertexSrcDeleted = false;
                    firstEdgeSrcPos = i;
                    newVertexSrcPos = j;
                }
                if (vertexSrcPos == -1) {
                    for (int k = 0; k < numVerticesAddedWithWord.getUnchecked(i); k++)
                        if (vertices.getUnchecked(j+k)==vertexSrc) vertexSrcPos=j+k;
                    assert(vertexSrcPos!=-1);
                    System.out.println("src j: " + j + " vertexSrcPos: " + vertexSrcPos);
                }
            }

            if (secondEdge.hasVertex(vertexDest)) {
                if (i!=positionDeleted && isVertexDestDeleted) {
                    isVertexDestDeleted = false;
                    firstEdgeDestPos = i;
                    newVertexDestPos = j;
                }
                if (vertexDestPos == -1) {
                    for (int k = 0; k < numVerticesAddedWithWord.getUnchecked(i); k++)
                        if (vertices.getUnchecked(j+k)==vertexDest) vertexDestPos=j+k;
                    assert(vertexDestPos!=-1);
                    System.out.println("dest j: " + j + " vertexDestPos: " + vertexDestPos);
                }
            }

            j += numVerticesAddedWithWord.getUnchecked(i);
            i++;
        }

        assert(isVertexSrcDeleted==false || isVertexDestDeleted == false);
        assert(vertexSrcPos!=-1 && vertexDestPos!=-1);

        System.out.println("src " + vertexSrc + " vertexSrcPos: " + vertexSrcPos +
                " newVertexSrcPos: " + newVertexSrcPos + " firstEdgeSrcPos: " + firstEdgeSrcPos);
        System.out.println("dest " + vertexDest + " vertexDestPos: " + vertexDestPos +
                " newVertexDestPos: " + newVertexDestPos + " firstEdgeDestPos: " + firstEdgeDestPos);
        //src vertex
        if (!isVertexSrcDeleted) {
            int aux = numVerticesAddedWithWord.get(firstEdgeSrcPos);
            numVerticesAddedWithWord.setUnchecked(firstEdgeSrcPos, aux + 1);
            int min = Math.min(newVertexSrcPos, vertexSrcPos);
            int max = Math.max(newVertexSrcPos, vertexSrcPos);
            swiftVertices(min, max-1);
        }

        //dest vertex
        if (!isVertexDestDeleted) {
            int aux = numVerticesAddedWithWord.get(firstEdgeDestPos);
            numVerticesAddedWithWord.setUnchecked(firstEdgeDestPos, aux + 1);
            int min = Math.min(newVertexDestPos, vertexDestPos);
            int max = Math.max(newVertexDestPos, vertexDestPos);
            swiftVertices(min, max);
        }

        if (isVertexSrcDeleted)
            vertices.removeInt(vertexSrc);
        if (isVertexDestDeleted)
            vertices.removeInt(vertexDest);

        numVerticesAddedWithWord.remove(positionDeleted);
    }

    private void swiftVertices(int srcIdx, int destIdx) {

        System.out.println("srcIdx:" + srcIdx + " destIdx:" + destIdx);

        if (srcIdx < destIdx) {
            int i = srcIdx;
            while (i < destIdx - 1) {
                int aux = vertices.getUnchecked(i);
                vertices.setUnchecked(i, vertices.getUnchecked(i + 1));
                vertices.setUnchecked(i + 1, aux);
                i++;
            }
        }
        else {
            int i = srcIdx;
            while (i > destIdx ) {
                int aux = vertices.getUnchecked(i);
                vertices.setUnchecked(i, vertices.getUnchecked(i - 1));
                vertices.setUnchecked(i - 1, aux);
                i--;
            }
        }

    }*/

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

    public IntArrayList getSharedWordIds(EdgeInducedEmbedding embedding) {
        return getSharedEdgeIds(embedding);
    }
}
