package stackoverflow

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite}

@RunWith(classOf[JUnitRunner])
class StackOverflowSuite extends FunSuite with BeforeAndAfterAll {

  val sc = StackOverflow.sc
  private val question = 1
  private val answer__ = 2

  /* <postTypeId>, <id>, <acceptedAnswer>, <parentId>, <score>, <tag> */
  val posts = List(
    Posting(question, 1, Some(2), None, 1, Option("Java")),
    Posting(answer__, 2, None, Some(1), 1, Option("Java")),
    Posting(question, 3, Some(4), None, 2, Option("Scala")),
    Posting(answer__, 4, None, Some(3), 3, Option("Scala")),
    Posting(question, 5, None, None, 2, Option("Scala"))
  )

  val postsRDD = sc.parallelize(posts)

  lazy val testObject = new StackOverflow {
    override val langs =
      List(
        "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
        "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

    override def langSpread = 50000

    override def kmeansKernels = 45

    override def kmeansEta: Double = 20.0D

    override def kmeansMaxIterations = 120
  }

  test("testObject can be instantiated") {
    val instantiatable = try {
      testObject
      true
    } catch {
      case _: Throwable => false
    }
    assert(instantiatable, "Can't instantiate a StackOverflow object")
  }

  test("groupedPostings") {
    val grouped = StackOverflow.groupedPostings(postsRDD).collect()
    assert(grouped.length == 2)
  }
}
