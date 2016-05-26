package com.cloudera.datascience.recommender

import scala.collection.Map
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.recommendation._
import org.apache.spark.rdd.RDD
import org.elasticsearch.spark._
import org.elasticsearch.spark.sql._

object Recommender {

  def main(args: Array[String]): Unit = {
    val base = if (args.length > 0) args(0).toString else "127.0.0.1:9200"
    val baseFolder = if (args.length > 0) args(1).toString else "/home/qinglin/Documents/spark/recommender/profiledata_06-May-2005/"
    var conf = new SparkConf().setAppName("Recommender")
    conf.set("es.nodes", base)
    conf.set("es.index.auto.create", "true")
    val sc = new SparkContext(conf)

    val rddimg = sc.esRDD("demo/images")
    var rddevent = sc.esRDD("demo/pullevent")
    //println(rddimg.first())
    //println(rddevent.first())

    model(sc, rddevent, rddimg)
    evaluate(sc, rddevent, rddimg)
    recommend(sc, rddevent, rddimg, base)
  }

  def buildImageByID(rddimg: RDD[(String, scala.collection.Map[String,AnyRef])]) =
    rddimg.map { record =>
      val doc = record._2
      val (id, name) = (doc.get("id").get.asInstanceOf[Int], doc.get("name").get.asInstanceOf[String])
      if (name.isEmpty) {
        None
      } else {
        try {
          Some((id.toInt, name.trim))
        } catch {
          case e: NumberFormatException => None
        }
      }
    }

def buildEventsID(rddevent: RDD[(String, scala.collection.Map[String,AnyRef])]) =
    rddimg.map { record =>
      val doc = record._2
      val (userid, imageid, count) = (doc.get("userID").get.asInstanceOf[Int], doc.get("imageID").get.asInstanceOf[Int], doc.get("count").get.asInstanceOf[Int])
      if (name.isEmpty) {
        None
      } else {
        try {
          Some((userid.toInt, imageid.toInt, count.toInt))
        } catch {
          case e: NumberFormatException => None
        }
      }
    }

  def buildRatings(
      rawUserArtistData: RDD[String],
      bArtistAlias: Broadcast[Map[Int,Int]]) = {
    rawUserArtistData.map { line =>
      val Array(userID, artistID, count) = line.split(' ').map(_.toInt)
      val finalArtistID = bArtistAlias.value.getOrElse(artistID, artistID)
      Rating(userID, finalArtistID, count)
    }
  }

  def buildEventRating (
      rddevent: RDD[(String, scala.collection.Map[String,AnyRef])]) = {
       rddevent.map { record =>
          val doc = record._2
          val userID = doc.get("userID").get.asInstanceOf[Int]
          val imageID = doc.get("imageID").get.asInstanceOf[Int]
          val count = doc.get("count").get.asInstanceOf[Int]
        Rating(userID, imageID, count)
       }
  }

  def model(
      sc: SparkContext,
      rddevent: RDD[(String, scala.collection.Map[String,AnyRef])],
      rddimg: RDD[(String, scala.collection.Map[String,AnyRef])]): Unit = {

    //val bArtistAlias = sc.broadcast(buildArtistAlias(rawArtistAlias))

    val trainData = buildEventRating(rddevent).cache()

    val model = ALS.trainImplicit(trainData, 10, 5, 0.01, 1.0)

    trainData.unpersist()

    println(model.userFeatures.mapValues(_.mkString(", ")).first())

    val userID = 100
    val recommendations = model.recommendProducts(userID, 5)
    recommendations.foreach(println)
    val recommendedProductIDs = recommendations.map(_.product).toSet

//select events which contains user 100
    
    val imageIDS = buildEventsID(rddevent).
      filter { case Array(user,_,_) => user.toInt == userID }

    val existingProducts = imageIDS.map { case Array(_,image,_) => image.toInt }.
      collect().toSet

    val allImageByID = buildImageByID(rddimg)

    allImageByID.filter { case (id, name) => existingProducts.contains(id) }.
      values.collect().foreach(println)
    allImageByID.filter { case (id, name) => recommendedProductIDs.contains(id) }.
      values.collect().foreach(println)

    unpersist(model)
  }

  def areaUnderCurve(
      positiveData: RDD[Rating],
      bAllItemIDs: Broadcast[Array[Int]],
      predictFunction: (RDD[(Int,Int)] => RDD[Rating])) = {
    // What this actually computes is AUC, per user. The result is actually something
    // that might be called "mean AUC".

    // Take held-out data as the "positive", and map to tuples
    val positiveUserProducts = positiveData.map(r => (r.user, r.product))
    // Make predictions for each of them, including a numeric score, and gather by user
    val positivePredictions = predictFunction(positiveUserProducts).groupBy(_.user)

    // BinaryClassificationMetrics.areaUnderROC is not used here since there are really lots of
    // small AUC problems, and it would be inefficient, when a direct computation is available.

    // Create a set of "negative" products for each user. These are randomly chosen
    // from among all of the other items, excluding those that are "positive" for the user.
    val negativeUserProducts = positiveUserProducts.groupByKey().mapPartitions {
      // mapPartitions operates on many (user,positive-items) pairs at once
      userIDAndPosItemIDs => {
        // Init an RNG and the item IDs set once for partition
        val random = new Random()
        val allItemIDs = bAllItemIDs.value
        userIDAndPosItemIDs.map { case (userID, posItemIDs) =>
          val posItemIDSet = posItemIDs.toSet
          val negative = new ArrayBuffer[Int]()
          var i = 0
          // Keep about as many negative examples per user as positive.
          // Duplicates are OK
          while (i < allItemIDs.size && negative.size < posItemIDSet.size) {
            val itemID = allItemIDs(random.nextInt(allItemIDs.size))
            if (!posItemIDSet.contains(itemID)) {
              negative += itemID
            }
            i += 1
          }
          // Result is a collection of (user,negative-item) tuples
          negative.map(itemID => (userID, itemID))
        }
      }
    }.flatMap(t => t)
    // flatMap breaks the collections above down into one big set of tuples

    // Make predictions on the rest:
    val negativePredictions = predictFunction(negativeUserProducts).groupBy(_.user)

    // Join positive and negative by user
    positivePredictions.join(negativePredictions).values.map {
      case (positiveRatings, negativeRatings) =>
        // AUC may be viewed as the probability that a random positive item scores
        // higher than a random negative one. Here the proportion of all positive-negative
        // pairs that are correctly ranked is computed. The result is equal to the AUC metric.
        var correct = 0L
        var total = 0L
        // For each pairing,
        for (positive <- positiveRatings;
             negative <- negativeRatings) {
          // Count the correctly-ranked pairs
          if (positive.rating > negative.rating) {
            correct += 1
          }
          total += 1
        }
        // Return AUC: fraction of pairs ranked correctly
        correct.toDouble / total
    }.mean() // Return mean AUC over users
  }

  def predictMostListened(sc: SparkContext, train: RDD[Rating])(allData: RDD[(Int,Int)]) = {
    val bListenCount =
      sc.broadcast(train.map(r => (r.product, r.rating)).reduceByKey(_ + _).collectAsMap())
    allData.map { case (user, product) =>
      Rating(user, product, bListenCount.value.getOrElse(product, 0.0))
    }
  }

  def evaluate(
      sc: SparkContext,
      rddevent: RDD[(String, scala.collection.Map[String,AnyRef])],
      rddimg: RDD[(String, scala.collection.Map[String,AnyRef])]): Unit = {
   

    val allData = buildEventRating(rddevent)
    val Array(trainData, cvData) = allData.randomSplit(Array(0.9, 0.1))
    trainData.cache()
    cvData.cache()

    val allItemIDs = allData.map(_.product).distinct().collect()
    val bAllItemIDs = sc.broadcast(allItemIDs)

    val mostListenedAUC = areaUnderCurve(cvData, bAllItemIDs, predictMostListened(sc, trainData))
    println(mostListenedAUC)

    val evaluations =
      for (rank   <- Array(10,  50);
           lambda <- Array(1.0, 0.0001);
           alpha  <- Array(1.0, 40.0))
      yield {
        val model = ALS.trainImplicit(trainData, rank, 10, lambda, alpha)
        val auc = areaUnderCurve(cvData, bAllItemIDs, model.predict)
        unpersist(model)
        ((rank, lambda, alpha), auc)
      }

    evaluations.sortBy(_._2).reverse.foreach(println)

    trainData.unpersist()
    cvData.unpersist()
  }

  def recommend(
      sc: SparkContext,
      rddevent: RDD[(String, scala.collection.Map[String,AnyRef])],
      rddimg: RDD[(String, scala.collection.Map[String,AnyRef])]): Unit = {

   
    val allData = buildEventRating(rddevent)
    val model = ALS.trainImplicit(allData, 50, 10, 1.0, 40.0)
    allData.unpersist()

    val userID = 100
    val recommendations = model.recommendProducts(userID, 5)
    val recommendedProductIDs = recommendations.map(_.product).toSet

    val imageByID = buildImageByID(rddimg)

    imageByID.filter { case (id, name) => recommendedProductIDs.contains(id) }.
       values.collect().foreach(println)

    val someUsers = allData.map(_.user).distinct().take(100)
    val someRecommendations = someUsers.map(userID => model.recommendProducts(userID, 5))
    someRecommendations.map(
      recs => recs.head.user + " -> " + recs.map(_.product).mkString(", ")
    ).foreach(println)
    someRecommendations.saveToEs("demo/recommends")
    unpersist(model)
  }

  def unpersist(model: MatrixFactorizationModel): Unit = {
    // At the moment, it's necessary to manually unpersist the RDDs inside the model
    // when done with it in order to make sure they are promptly uncached
    model.userFeatures.unpersist()
    model.productFeatures.unpersist()
  }

}