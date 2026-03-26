import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

object Stage1InputLoad {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Correct arguments: <input-directory> <output-directory>")
      System.exit(1)
    }

    val sc = new SparkContext(new SparkConf().setAppName("Stage1InputLoad"))

    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    val textFile = sc.textFile(args(0))

    val totalLines = textFile.count()
    val preview = textFile.take(10)

    println(s"TOTAL_LINES=$totalLines")
    preview.zipWithIndex.foreach { case (line, idx) =>
      println(s"LINE_${idx + 1}=$line")
    }

    sc.parallelize(Seq(s"TOTAL_LINES=$totalLines"), 1).saveAsTextFile(args(1))

    sc.stop()
  }
}
