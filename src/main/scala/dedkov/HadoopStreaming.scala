package dedkov

/**
  * Created by dedkov-va on 19.02.17.
  */

object HadoopStreaming {
  //Напишите программу, которая реализует mapper для задачи WordCount в Hadoop Streaming.
  object Mapper {
    object Main {
      def main(args: Array[String]) {
        for (ln <- io.Source.stdin.getLines) {
          for (word <- ln.split(" ")) {
            println(s"$word\t1")
          }
        }
      }
    }
  }

  //Напишите программу, которая реализует reducer для задачи WordCount в Hadoop Streaming.
  object Reducer {
    object Main {
      def main(args: Array[String]) {
        // put your code here
        var sum = 0
        var lastKey = ""
        for (ln <- io.Source.stdin.getLines) {
          val ln2 = ln.split("\t")
          val key = ln2(0)
          if (sum == 0) {
            lastKey = key
          }
          if (key != lastKey) {
            println(s"$lastKey\t$sum")
            sum = 1
            lastKey = key
          } else {
            sum += 1
          }
        }
        println(s"$lastKey\t$sum")
      }
    }
  }
}
