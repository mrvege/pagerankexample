/**
 * Created by dm on 11/3/15.
 */
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object pageRankExample {
  def main (args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val sc = new SparkContext("local[2]", "research")
    // Load my user data and parse into tuples of user id and attribute list
    val users = (sc.textFile("hdfs://localhost:9000/input/users.txt")
      .map(line => line.split(",")).map( parts => (parts.head.toLong, parts.tail) ))
    // Parse the edge data which is already in userId -> userId format
    //local address: /home/open/spark-1.5.1-source/graphx/data/followers.txt
    val followerGraph = GraphLoader.edgeListFile(sc, "hdfs://localhost:9000/input/followers.txt")
    // Attach the user attributes
    val graph = followerGraph.outerJoinVertices(users) {
      case (uid, deg, Some(attrList)) => attrList
      // Some users may not have attributes so we set them as empty
      case (uid, deg, None) => Array.empty[String]
    }
    // Restrict the graph to users with usernames and names
    val subgraph = graph.subgraph(vpred = (vid, attr) => attr.size == 2)
    // Compute the PageRank
    val pagerankGraph = subgraph.pageRank(0.001)
    // Get the attributes of the top pagerank users
    val userInfoWithPageRank = subgraph.outerJoinVertices(pagerankGraph.vertices) {
      case (uid, attrList, Some(pr)) => (pr, attrList.toList)
      case (uid, attrList, None) => (0.0, attrList.toList)
    }

    println(userInfoWithPageRank.vertices.top(5)(Ordering.by(_._2._1)).mkString("\n"))
    sc.stop()
  }

}
