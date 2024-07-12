package com.example

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.HashPartitioner
import org.apache.log4j.{Level, LogManager, Logger}


object pr {

  // Initialize logger
  @transient lazy val log = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      log.error("Usage: pr <k> <iterations> <output-path>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("pr")

    val sc = new SparkContext(conf)
    val logger: Logger = LogManager.getRootLogger
    logger.setLevel(Level.INFO)
    
    // Parameters
    val k = args(0).toInt  // Number of linear chains
    val iterations = args(1).toInt  // Number of PR iterations
    val outputPath = args(2)  // Output path for results

    //logger.info(s"Starting PageRank computation with k=$k and $iterations iterations")

   
  
 
  

    // Generate synthetic graph
    val Graph = generateGraph(sc, k)
    var Ranks: RDD[(Int, Double)] = initializeRanks(sc, k).partitionBy(new HashPartitioner(sc.defaultParallelism))
   
    for (i <- 1 to iterations) {
  //logger.info(s"Starting PR iteration $i")
         // println(s"Iteration $i")
  // Join Graph with Ranks and compute contributions
  val contributions = Graph.partitionBy(Ranks.partitioner.get)
    .join(Ranks)
    .flatMap { case (page, (outlink, rank)) =>
      Seq((outlink, rank)) // Emit contribution to connected pages
    }
    .reduceByKey(_ + _)  // Aggregate contributions
    .mapValues(rank => 0.15/(k*k) + 0.85 * rank)  // Apply damping factor and teleportation

  // Identify nodes in Ranks RDD with no incoming edges
  val nodesWithNoIncomingEdges = Ranks.subtractByKey(contributions)

  // Assign damping factor as contribution to nodes with no incoming edges
  val contributionsWithDamping = nodesWithNoIncomingEdges.mapValues(_ => 0.15/(k*k))

  // Merge contributions with existing contributions
  val allContributions = contributions.union(contributionsWithDamping)

  // Handle dangling nodes (dummy page 0)
  val danglingMassValue = contributions.lookup(0).headOption.getOrElse(0.0)
  val redistributedMass = danglingMassValue / (k * k).toDouble

  // Update Ranks without including node 0 explicitly
  Ranks = allContributions
    .filter(_._1 != 0)  // Exclude dummy page 0
    .mapValues(rank => rank + redistributedMass)
    // Cache the updated Ranks RDD

  // logger.info(s"Completed PR iteration $i")
  
}
   logger.info(s"Lineage of Ranks after iteration $iterations: ${Ranks.toDebugString}") 

    // Output final PR results to file
    Ranks
      .sortByKey()
      .map{ case (page, rank) => s"$page\t$rank" }
      .saveAsTextFile(outputPath)

    sc.stop()
    // logger.info("PageRank computation completed successfully. Spark context stopped.")
  }

  // Function to generate synthetic graph (k linear chains)
  def generateGraph(sc: SparkContext, k: Int): RDD[(Int, Int)] = {
    val pages = sc.parallelize(1 to (k * k))
    val chains = pages.map(page => {
      val chainId = (page - 1) / k
      val pageInChain = (page - 1) % k + 1
      val nextPage = if (pageInChain < k) page + 1 else 0
      (page, nextPage)
    }).filter(_._2 != 0)

    // Add dummy page 0 to Graph
    val dummyEdges = sc.parallelize((1 to k).map(d => (d * k, 0)))
    val completeGraph = chains.union(dummyEdges).partitionBy(new HashPartitioner(sc.defaultParallelism))
    
    completeGraph
  }

  // Function to initialize ranks
  def initializeRanks(sc: SparkContext, k: Int): RDD[(Int, Double)] = {
    val initialPR = 1.0 / (k * k)
    val pages = sc.parallelize(1 to (k * k))  // Exclude page 0 (dummy page)
    val ranks = pages.map(page => (page, initialPR)).partitionBy(new HashPartitioner(sc.defaultParallelism))
    
    ranks
  }
}
