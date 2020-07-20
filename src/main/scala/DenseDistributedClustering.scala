package DenseDistributedClustering

import org.apache.spark.sql.SparkSession
import math._

import Clustering._
import Auxiliary._

object DenseDistributedClustering extends Serializable {
    val r = new scala.util.Random

    /**
     * Maps a vertex to two subsets.
     * 
     * @param v a vertex
     * @param k the amount of different subsets
     * 
     * @return the vertex v with the two corresponding subset keys.
     */
    def vertexMap(v: String, k: Double): (String, (Int, Int)) = {
        (v, (r.nextInt(k.toInt), r.nextInt(k.toInt)))
    }

    /**
      * Maps an edge to a specific subset based on endpoint keys.
      *
      * @param e an edge
      * @param mapper a hashmap of vertices to subset keys.
      * 
      * @return the edge e mapped to the subset k1 x k2.
      */
    def partition(e: MstEdge, mapper: Map[String, (Int, Int)]): ((Int, Int), MstEdge) = {
        ((mapper(vdts(e._1))._1, mapper(vdts(e._2))._2), e)
    }

    /**
     * @param str String the raw format of the edge
     * @return MstEdge
     */
    def edge(str: String): MstEdge = {
        val splitted = str.split(',')
        
        (vstd(splitted(0)), vstd(splitted(1)), splitted(2).toDouble)
    }

    def main(args: Array[String]): Unit = {
        // command line's arguments handling
        val inputPath = args(0)
        val eps = args(1).toFloat
        val cpus = args(2).toInt

        val spark = SparkSession.builder
            .master("local[" + cpus + "]")
            .appName("Aff Improved cores:" + cpus + "|D:" + inputPath + "|e:" + eps)
            .getOrCreate

        import spark.implicits._

        val sc = spark.sparkContext

        // Space per machine is at most 0(n^(1+eps)) where n is the number of data points
        val clustersNum = 1

        var edges = sc.textFile(inputPath).map(x => edge(x))
        val vertices = edges.flatMap(e => Array(e._1, e._2).map(vdts(_))).distinct

        val n = vertices.count
        var m = edges.count

        var c = math.log(m)/math.log(n) - 1   // == log_n(m/n)

        var iterations = 0
        // while edges can't fit into one machine
        while(c > eps) {
            iterations += 1
            // size of V, U
            var k = math.floor(math.pow(n, (c - eps)/2))
            // broadcast the hashmap for parallel mapping
            val vertexMapper = vertices.map(vertexMap(_, k)).collect.toMap
            val broadcastMapper = sc.broadcast(vertexMapper)

            // partition randomly based on mapper
            val fullPartitionVU = edges.map(partition(_, broadcastMapper.value))
            // reduce edges: run local MST for each shuffled partition on each node. Edges not included in the MST of the local subset are impossible to constitute the final original MST.
            edges = fullPartitionVU.groupByKey().flatMap(x => MST(x._2.toArray))
            
            m = edges.count
            c = math.log(m)/math.log(n) - 1 // == log_n(m/n)

            broadcastMapper.unpersist(true)
        }
        
        val clusters = clustering(edges.collect, n, clustersNum)
        // prints final mst weight
        println(clusters.map(_._3).reduce((x,y) => x+y))
        println(iterations)
        sc.stop()
    }
}