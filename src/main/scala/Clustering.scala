package DenseDistributedClustering

import Auxiliary._

// Auxiliary class for implementing the Kruskal MST algorithm
object Clustering extends Serializable {
    // Union by rank
    def union(trees: collection.mutable.Map[String, Tree], a: String, b: String): Unit = {
        val aroot: String = find(trees, a)
        val broot: String = find(trees, b)

        if(trees(aroot).rank < trees(broot).rank) {
            trees(aroot).parent = broot
        } else if(trees(aroot).rank > trees(broot).rank) {
            trees(broot).parent = aroot
        } else {
            trees(broot).parent = aroot
            trees(aroot).rank += 1
        }
    }

    // find root with path compression
    def find(trees: collection.mutable.Map[String, Tree], v: String): String = {
        if(trees(v).parent != v) {
            // path compression: now every vertex from tree's root till v will point to the same parent
            trees(v).parent = find(trees, trees(v).parent)
        }

        trees(v).parent
    }

    /** Using Kruskal Algorithm for Single-linkage local clustering.
      * 
      *
      * @param E the edges of the subset
      * @return the mst produced by the given edges
      */
    def MST(E: Array[MstEdge]): Array[MstEdge] = {
        val edges = E.sortBy(edge => edge._3) // sort edges by length
        
        // initialize the trees of the forest
        var trees = collection.mutable.Map[String, Tree]()
        for(e <- edges) {
            val v1 = vdts(e._1)
            val v2 = vdts(e._2)
            trees(v1) = new Tree(v1, 0)
            trees(v2) = new Tree(v2, 0)
        }
        val vSize = trees.keys.size
        var mst: Array[MstEdge] = new Array(vSize - 1)
        
        var i = 0
        var edgesIncluded = 0
        while(edgesIncluded < vSize - 1 && i < edges.length) {
            val e = edges(i) // (X,Y, w)
            i += 1
            
            val treeofX: String = find(trees, vdts(e._1))
            val treeofY: String = find(trees, vdts(e._2))
            
            // if X and Y do not belong to the same tree it is safe to add the edge in the MST
            // otherwise it creates a circuit
            if(treeofX != treeofY) {
                mst(edgesIncluded) = e
                edgesIncluded += 1
                union(trees, treeofX, treeofY)
            }
        }
        
        mst.filter(_ != null)
    }
    

    /** Using Kruskal Algorithm for Single-linkage the final clustering.
      * 
      *
      * @param E the edges of the subset
      * @param k the number of the final clusters
      * @return the minimum spanning forest based on the given edges and the cluster number(k).
      */
    def clustering(E: Array[MstEdge], n: Long, k: Int): Array[MstEdge] = {   
        val edges = E.sortBy(edge => edge._3) // sort edges by length
        
        // initialize the trees of the forest
        var trees = collection.mutable.Map[String, Tree]()
        for(e <- edges) {
            val v1 = vdts(e._1)
            val v2 = vdts(e._2)
            trees(v1) = new Tree(v1, 0)
            trees(v2) = new Tree(v2, 0)
        }
        val vSize = trees.keys.size
        var mst: Array[MstEdge] = new Array(vSize - 1)
        
        var i = 0
        var edgesIncluded = 0
        while( (n - edgesIncluded > k) && edgesIncluded < vSize - 1 && i < edges.length) {
            val e = edges(i) // (X,Y, w)
            i += 1

            val treeofX: String = find(trees, vdts(e._1))
            val treeofY: String = find(trees, vdts(e._2))

            // if X and Y do not belong to the same tree it is safe to add the edge in the MST
            // otherwise it creates a circuit
            if(treeofX != treeofY) {
                mst(edgesIncluded) = e
                edgesIncluded += 1
                union(trees, treeofX, treeofY)
            }
        }

        mst.filter(_ != null)
    }
}