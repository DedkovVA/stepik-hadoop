package dedkov

import scala.collection.mutable

/**
  * Created by dedkov-va on 20.02.17.
  */
object MRAlgos {
  object InMapperCombiningV1 {
    object Main {
      def main(args: Array[String]) {
        // put your code here
        for (ln <- io.Source.stdin.getLines) {
          var map = scala.collection.mutable.Map.empty[String, Int]
          for (term <- ln.split(" ")) {
            map += ((term, if (map.contains(term)) map(term) + 1 else 1))
          }
          for (iter <- map) {
            println(s"${iter._1}\t${iter._2}")
          }
        }
      }
    }
  }

  object InMapperCombiningV2 {
    object Main {
      def main(args: Array[String]) {
        // put your code here
        val map = scala.collection.mutable.Map.empty[String, Int]
        for (ln <- io.Source.stdin.getLines) {
          for (term <- ln.split(" ")) {
            map += ((term, if (map.contains(term)) map(term) + 1 else 1))
          }
        }
        for (iter <- map) {
          println(s"${iter._1}\t${iter._2}")
        }
      }
    }
  }

  object AverageTimeSpentOnSite {
    object Main {
      def main(args: Array[String]) {
        // put your code here
        var counter = 0
        var sum = 0
        var lastKey = ""
        for (ln <- io.Source.stdin.getLines) {
          val ln2 = ln.split("\t")
          val key = ln2(0)
          if (counter == 0) {
            lastKey = key
          }
          if (key != lastKey) {
            println(s"$lastKey\t${sum / counter}")
            counter = 1
            sum = ln2(1).toInt
            lastKey = key
          } else {
            counter += 1
            sum += ln2(1).toInt
          }
        }
        println(s"$lastKey\t${sum / counter}")
      }
    }
  }

  object AverageTimeSpentOnSiteCombiner {
    object Main {
      def main(args: Array[String]) {
        // put your code here
        var counter = 0
        var sum = 0
        var lastKey = ""
        for (ln <- io.Source.stdin.getLines) {
          val ln2 = ln.split("\t")
          val key = ln2(0)
          if (counter == 0) {
            lastKey = key
          }
          if (key != lastKey) {
            println(s"$lastKey\t$sum;$counter")
            counter = 1
            sum = ln2(1).split(";")(0).toInt
            lastKey = key
          } else {
            counter += 1
            sum += ln2(1).split(";")(0).toInt
          }
        }
        println(s"$lastKey\t$sum;$counter")
      }
    }
  }

  object DistinctValuesV1Phase1Mapper {
    object Main {
      def main(args: Array[String]) {
        // put your code here
        for {
          ln <- io.Source.stdin.getLines
          kv = ln.split("\t")
          values = kv(1).split(",")
          v <- values
        } println(s"${kv(0)},$v\t1")
      }
    }
  }

  object DistinctValuesV1Phase1Reducer {
    object Main {
      def main(args: Array[String]) {
        // put your code here
        var prevV = ""
        for {
          ln <- io.Source.stdin.getLines
          vv = ln.split("\t")
          v = vv(0)
        } {
          if (prevV != v) {
            prevV = v
            println(v)
          }
        }
      }
    }
  }

  object DistinctValuesV1Phase2Mapper {
    object Main {
      def main(args: Array[String]) {
        // put your code here
        for {
          ln <- io.Source.stdin.getLines
          vv = ln.split(",")
        } println(s"${vv(1)}\t1")
      }
    }
  }

  object DistinctValuesV2Reducer {
    object Main {
      def main(args: Array[String]) {
        // put your code here
        val map = collection.mutable.Map.empty[String, Set[String]]
        for {
          ln <- io.Source.stdin.getLines
          vv = ln.split("\t")
        } if (map.contains(vv(1)))
            map += ((vv(1), map(vv(1)) + vv(0)))
          else
            map += ((vv(1), Set(vv(0))))

        for {
          e <- map
        } println(s"${e._1}\t${e._2.size}")
      }
    }
  }

  object CrossCorrelationMapper {
      object Main {
        def main(args: Array[String]) {
          // put your code here
          for {
            ln <- io.Source.stdin.getLines
            vv = ln.split(" ")
            v1 <- vv
            v2 <- vv
          } if (v1 != v2) println(s"$v1,$v2\t1")
        }
      }
  }

  object CrossCorrelationWithStripesMapper {
    object Main {
      def main(args: Array[String]) {
        // put your code here
        for {
          ln <- io.Source.stdin.getLines
          vv = ln.split(" ")
        } {
          for { v1 <- vv } {
            var map = collection.immutable.Map.empty[String, Int]
            for { v2 <- vv } {
              if (v1 != v2) {
                if (map.contains(v2))
                  map += ((v2, map(v2) + 1))
                else
                  map += ((v2, 1))
              }
            }
            val s1 = map.map(e => s"${e._1}:${e._2}").mkString(",")
            val s2 = s"$v1\t$s1"
            println(s2)
          }
        }
      }
    }
  }

  //4.2 relational functions

  object Selection {
    object Main {
      def main(args: Array[String]) {
        // put your code here
        for {
          ln <- io.Source.stdin.getLines
          siteStats = ln.split("\t")
        } if (siteStats(1) == "user10") println (ln)
      }
    }
  }

  object Projection {
    object Main {
      def main(args: Array[String]) {
        // put your code here
        for {
          ln <- io.Source.stdin.getLines
          siteStats = ln.split("\t")
        } println (siteStats(2))
      }
    }
  }

  object Union {
    object Main {
      def main(args: Array[String]) {
        // put your code here
        var lastKey = ""
        for {
          ln <- io.Source.stdin.getLines
          plurality = ln.split("\t")
        } if (plurality(1) == "A" || plurality(1) == "B") {
          val key = plurality(0)
          if (lastKey != key) {
            lastKey = key
            println(lastKey)
          }
        }
      }
    }
  }

  object IntersectionReducer {
    object Main {
      def main(args: Array[String]) {
        // put your code here
        var lastKey = ""
        for {
          ln <- io.Source.stdin.getLines
          plurality = ln.split("\t")
        } if (plurality(1) == "A" || plurality(1) == "B") {
          val key = plurality(0)
          if (lastKey != key) {
            lastKey = key
          } else {
            println(key)
          }
        }
      }
    }
  }

  object SubtractionAMinusBReducer {
    object Main {
      def main(args: Array[String]) {
        // put your code here
        def f1(lastKey: String, keyForA: String, keyForB: String) =
          keyForA != "" && keyForA != keyForB && keyForA == lastKey

        SubtractionABCmnReducer.f0(f1)
      }
    }
  }

  object SubtractionABSymmetricReducer {
    object Main {
      def main(args: Array[String]) {
        // put your code here

        def f1(lastKey: String, keyForA: String, keyForB: String) =
          keyForA != keyForB && (keyForA == lastKey || keyForB == lastKey)

        SubtractionABCmnReducer.f0(f1)
      }
    }
  }

  object SubtractionABCmnReducer {
    def f0(f1: (String, String, String) => Boolean) = {
      var lastKey = ""
      var keyForA = ""
      var keyForB = ""

      def f2() = if (f1(lastKey, keyForA, keyForB)) println(lastKey)

        for {
          ln <- io.Source.stdin.getLines
          plurality = ln.split("\t")
        } {
        val key = plurality(0)

        if (lastKey != key) {
          f2()
          lastKey = key
        }

        val p1 = plurality(1)
        if (p1 == "A") {
          keyForA = key
        } else if (p1 == "B") {
          keyForB = key
        }
      }

      f2()
    }
  }

  object JoinReducer {
    object Main {
      def main(args: Array[String]) {
        // put your code here
        var lastKey = ""
        var listUrl = List.empty[String]
        var listQuery = List.empty[String]
        var key = ""

        def printResult() = {
          for {
            q <- listQuery.reverse
            u <- listUrl.reverse
          } println(s"$lastKey\t$q\t$u")

          lastKey = key

          listUrl = List.empty[String]
          listQuery = List.empty[String]
        }

        for {
          ln <- io.Source.stdin.getLines
          vv = ln.split("\t")
        } {
          key = vv(0)

          if (key != lastKey) {
            printResult()
          }

          val vv1 = vv(1).split(":")
          if (vv1(0) == "query") {
            listQuery = vv1(1) +: listQuery
          } else if (vv1(0) == "url") {
            listUrl = vv1(1) +: listUrl
          }
        }

        printResult()
      }
    }
  }
}
