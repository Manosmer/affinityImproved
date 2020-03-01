package DenseDistributedClustering

object Auxiliary extends Serializable {
    type MstEdge = (Array[Double], Array[Double], Double)

    /** Tree class, representing the trees of a forest for kruskal
     */
    class Tree {
        var parent: String = ""
        var rank: Int = -1
    
        def this(p: String, r: Int) = {
            this()
            this.parent = p
            this.rank = r
        }
    }

    /** Creates a string that represents a vertex.
     * 
     * @return a vertex as a string
     */
    def vdts(v: Array[Double]): String = {
        v.mkString("|")
    }

    /** Deserializes a string that represents a vertex into its endpoints, splitting it 
     * by the character '|'.
     * 
     * @return a vertex as an Array[Double]
     */
    def vstd(v: String): Array[Double] = {
        v.split('|').map(_.toDouble)
    }
}