package io.arabesque.utils;

import io.arabesque.graph.MainGraph;
import io.arabesque.embedding.Embedding;

import io.arabesque.utils.collection.IntArrayList;

/**
 * This class is used to find articulation points in a subgraph, i.e., those
 * vertices in which their removal turn the subgraph disconnected.
 *
 * This implementation also garantees that no modifications are made to the
 * vertices and edges arrays.
 *
 * This implementation was based on the following article:
 * http://www.geeksforgeeks.org/articulation-points-or-cut-vertices-in-a-graph/
 *
 * TODO: Adapt this class for edge bridging (i.e., make work with edge induced
 * embeddings)
 */
public class ArticulationPoints {
   static final int NIL = -1;

   // must be set by setEmbedding or individually
   private IntArrayList vertices;
   private IntArrayList edges;

   // aux structures, if possible we reuse them for futures executions
   private boolean visited[];
   private IntArrayList disc;
   private IntArrayList low;
   private IntArrayList parent;
   private boolean ap[];

   private MainGraph mainGraph;

   int time;

   public ArticulationPoints(MainGraph mainGraph) {
      this.mainGraph = mainGraph;
   }

   /*
    * Prepare the algorithms to run over the embedding
    */
   private void setFromEmbedding(Embedding embedding) {
      int numVertices = embedding.getNumVertices();
      visited = createOrEnsureCapacity (visited, numVertices);
      disc = createOrEnsureCapacity (disc, numVertices);
      low = createOrEnsureCapacity (low, numVertices);
      parent = createOrEnsureCapacity (parent, numVertices);
      ap = createOrEnsureCapacity (ap, numVertices);
      this.vertices = embedding.getVertices();
      this.edges = embedding.getEdges();
      reset();
   }

   private boolean[] createOrEnsureCapacity(boolean[] flags, int capacity) {
      if (flags != null && flags.length == capacity) {
         return flags;
      } else {
         return new boolean[capacity];
      }
   }

   private IntArrayList createOrEnsureCapacity(IntArrayList ints, int capacity) {
      if (ints == null) {
         return new IntArrayList(capacity);
      } else {
         ints.ensureCapacity(capacity);
         return ints;
      }
   }

   public void reset() {
      time = 0;
      for (int u = 0; u < vertices.size(); ++u) {
         parent.setUnchecked (u, NIL);
         disc.setUnchecked (u, NIL);
         low.setUnchecked (u, NIL);
         visited[u] = false;
         ap[u] = false;
      }
   }

   private void articulationPointsRec(int u) {
      // number of children in the DFS
      int children = 0;

      // visiting new node
      visited[u] = true;

      // discovery time and low value
      disc.setUnchecked (u, ++time);
      low.setUnchecked (u, time);

      for (int v = 0; v < vertices.size(); ++v) {
         // here we verify whether each vertice is adjacent to each vertice
         // In practive (embeddings << graph) this is better than accessing
         // directly the whole graph adjacency (the only one available)
         if (!mainGraph.isNeighborVertex(vertices.getUnchecked(u),
                  vertices.getUnchecked(v)))
            continue;

         if (!visited[v]) {
            children++;
            parent.setUnchecked (v, u);
            articulationPointsRec(v);

            // Check if the subtree rooted with v has a connection to
            // one of the ancestors of u
            low.setUnchecked(u, Math.min(low.getUnchecked(u), low.getUnchecked(v)));

            // u is an articulation point in following cases

            // (1) u is root of DFS tree and has two or more chilren.
            if (parent.getUnchecked(u) == NIL && children > 1)
               ap[u] = true;

            // (2) If u is not root and low value of one of its child
            // is more than discovery value of u.
            if (parent.getUnchecked(u) != NIL && low.getUnchecked(v) >= disc.getUnchecked(u))
               ap[u] = true;
         }

         // Update low value of u for parent function calls.
         else if (v != parent.getUnchecked(u))
            low.setUnchecked(u, Math.min(low.getUnchecked(u), disc.getUnchecked(v)));
      }
   }

   /**
    * Clients call for articulation points of an embedding
    */
   public boolean[] articulationPoints(Embedding embedding) {
      setFromEmbedding(embedding);
      for (int u = 0; u < vertices.size(); ++u)
         if (visited[u] == false)
            articulationPointsRec(u);

      return ap;
   }

}
