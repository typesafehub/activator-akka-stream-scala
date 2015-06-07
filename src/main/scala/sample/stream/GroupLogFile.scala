package sample.stream

import java.io.File

import akka.actor.ActorSystem
import akka.stream.ActorFlowMaterializer
import akka.stream.scaladsl._
import akka.stream.stage.{ Context, StatefulStage, SyncDirective }
import akka.util.ByteString

import scala.annotation.tailrec

object GroupLogFile {

  def main(args: Array[String]): Unit = {
    // actor system and implicit materializer
    implicit val system = ActorSystem("Sys")
    implicit val materializer = ActorFlowMaterializer()

    // execution context

    val LoglevelPattern = """.*\[(DEBUG|INFO|WARN|ERROR)\].*""".r

    // read lines from a log file
    val logFile = new File("src/main/resources/logfile.txt")

    import akka.stream.io.Implicits._ // add file sources to Source object or use explicitly: SynchronousFileSource(f)
    Source.synchronousFile(logFile).
      // parse chunks of bytes into lines
      transform(() => parseLines(System.lineSeparator, 512)).
      // group them by log level
      groupBy {
        case LoglevelPattern(level) => level
        case other                  => "OTHER"
      }.
      // write lines of each group to a separate file
      mapAsync(parallelism = 5) {
        case (level, groupFlow) =>
          groupFlow.map(line => ByteString(line + "\n")).runWith(Sink.synchronousFile(new File(s"target/log-$level.txt")))
      }.
      runWith(Sink.onComplete { _ =>
        system.shutdown()
      })
  }

  def parseLines(separator: String, maximumLineBytes: Int) =
    new StatefulStage[ByteString, String] {
      private val separatorBytes = ByteString(separator)
      private val firstSeparatorByte = separatorBytes.head
      private var buffer = ByteString.empty
      private var nextPossibleMatch = 0

      def initial = new State {
        override def onPush(chunk: ByteString, ctx: Context[String]): SyncDirective = {
          buffer ++= chunk
          if (buffer.size > maximumLineBytes)
            ctx.fail(new IllegalStateException(s"Read ${buffer.size} bytes " +
              s"which is more than $maximumLineBytes without seeing a line terminator"))
          else emit(doParse(Vector.empty).iterator, ctx)
        }

        @tailrec
        private def doParse(parsedLinesSoFar: Vector[String]): Vector[String] = {
          val possibleMatchPos = buffer.indexOf(firstSeparatorByte, from = nextPossibleMatch)
          if (possibleMatchPos == -1) {
            // No matching character, we need to accumulate more bytes into the buffer
            nextPossibleMatch = buffer.size
            parsedLinesSoFar
          } else {
            if (possibleMatchPos + separatorBytes.size > buffer.size) {
              // We have found a possible match (we found the first character of the terminator
              // sequence) but we don't have yet enough bytes. We remember the position to
              // retry from next time.
              nextPossibleMatch = possibleMatchPos
              parsedLinesSoFar
            } else {
              if (buffer.slice(possibleMatchPos, possibleMatchPos + separatorBytes.size)
                == separatorBytes) {
                // Found a match
                val parsedLine = buffer.slice(0, possibleMatchPos).utf8String
                buffer = buffer.drop(possibleMatchPos + separatorBytes.size)
                nextPossibleMatch -= possibleMatchPos + separatorBytes.size
                doParse(parsedLinesSoFar :+ parsedLine)
              } else {
                nextPossibleMatch += 1
                doParse(parsedLinesSoFar)
              }
            }
          }

        }
      }
    }
}
