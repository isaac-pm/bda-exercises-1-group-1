import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer

object SparkProblem2 {

  sealed trait TokenType
  case object WordToken extends TokenType
  case object NumberToken extends TokenType

  case class TypedToken(value: String, tokenType: TokenType)

  private val WORD_TAG = "W"
  private val WORD_PAIR_TAG = "PWW"
  private val NUMBER_WORD_PAIR_TAG = "PNW"
  private val WINDOW = 2

  def isWord(token: String): Boolean = {
    token.length >= 6 &&
    token.length <= 24 &&
    token.forall(c => (c >= 'a' && c <= 'z') || c == '-')
  }

  def isNumber(token: String): Boolean = {
    if (token.length < 4 || token.length > 16) return false

    val body =
      if (token.startsWith("-")) {
        if (token.length == 1) return false
        token.substring(1)
      } else {
        token
      }

    body.nonEmpty && body.forall(c => (c >= '0' && c <= '9') || c == '.')
  }

  def classify(token: String): Option[TypedToken] = {
    if (isWord(token)) Some(TypedToken(token, WordToken))
    else if (isNumber(token)) Some(TypedToken(token, NumberToken))
    else None
  }

  def tokenizeAndFilter(line: String): Array[TypedToken] = {
    line
      .toLowerCase
      .split("[^a-z0-9.-]+")
      .filter(_.nonEmpty)
      .flatMap(classify)
  }

  def emitTaggedCounts(tokens: Array[TypedToken]): Array[(String, Int)] = {
    val out = ArrayBuffer.empty[(String, Int)]

    var i = 0
    while (i < tokens.length) {
      val first = tokens(i)

      if (first.tokenType == WordToken) {
        out += ((s"$WORD_TAG\t${first.value}", 1))
      }

      var distance = 1
      while (distance <= WINDOW && i + distance < tokens.length) {
        val second = tokens(i + distance)

        if (first.tokenType == WordToken && second.tokenType == WordToken) {
          out += ((s"$WORD_PAIR_TAG\t${first.value}:${second.value}", 1))
        }

        if (first.tokenType == NumberToken && second.tokenType == WordToken) {
          out += ((s"$NUMBER_WORD_PAIR_TAG\t${first.value}:${second.value}", 1))
        }

        distance += 1
      }

      i += 1
    }

    out.toArray
  }

  def top100Lines(
      counts: org.apache.spark.rdd.RDD[(String, Int)],
      tag: String,
      taskLabel: String
  ): Array[String] = {
    val prefix = tag + "\t"

    val ordering = Ordering.by[(String, Int), (Int, String)] {
      case (token, count) => (-count, token)
    }

    counts
      .filter { case (key, _) => key.startsWith(prefix) }
      .map { case (key, count) => (key.substring(prefix.length), count) }
      .takeOrdered(100)(ordering)
      .map { case (token, count) => s"$taskLabel\t$token\t$count" }
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Correct arguments: <input-directory> <output-directory>")
      System.exit(1)
    }

    val sc = new SparkContext(new SparkConf().setAppName("SparkProblem2"))

    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    val textFile = sc.textFile(args(0))

    val taggedCounts = textFile
      .flatMap(line => emitTaggedCounts(tokenizeAndFilter(line)))
      .reduceByKey(_ + _)
      .persist(StorageLevel.MEMORY_AND_DISK)

    val task1Lines = taggedCounts
      .filter { case (key, count) => key.startsWith(WORD_TAG + "\t") && count == 1000 }
      .map { case (key, count) => s"TASK1\t${key.substring(2)}\t$count" }
      .collect()
      .sorted

    val task2Lines = taggedCounts
      .filter { case (key, count) => key.startsWith(WORD_PAIR_TAG + "\t") && count == 1000 }
      .map { case (key, count) => s"TASK2\t${key.substring(4)}\t$count" }
      .collect()
      .sorted

    val task3Lines = top100Lines(taggedCounts, WORD_TAG, "TASK3")
    val task4Lines = top100Lines(taggedCounts, NUMBER_WORD_PAIR_TAG, "TASK4")

    println(s"TASK1_COUNT=${task1Lines.length}")
    println(s"TASK2_COUNT=${task2Lines.length}")
    println(s"TASK3_COUNT=${task3Lines.length}")
    println(s"TASK4_COUNT=${task4Lines.length}")

    println("TASK3_PREVIEW")
    task3Lines.take(10).foreach(println)

    println("TASK4_PREVIEW")
    task4Lines.take(10).foreach(println)

    val finalLines =
      task1Lines ++
      task2Lines ++
      task3Lines ++
      task4Lines

    sc.parallelize(finalLines.toSeq, 1).saveAsTextFile(args(1))

    taggedCounts.unpersist()

    sc.stop()
  }
}
