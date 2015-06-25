package sample.stream

import java.io.File

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.io.Framing
import akka.stream.scaladsl._
import akka.stream.stage.{ Context, StatefulStage, SyncDirective }
import akka.util.ByteString

import scala.annotation.tailrec

object GroupLogFile {

  def main(args: Array[String]): Unit = {
    // actor system and implicit materializer
    implicit val system = ActorSystem("Sys")
    implicit val materializer = ActorMaterializer()

    // execution context

    val LoglevelPattern = """.*\[(DEBUG|INFO|WARN|ERROR)\].*""".r

    // read lines from a log file
    val logFile = new File("src/main/resources/logfile.txt")

    import akka.stream.io.Implicits._ // add file sources to Source object or use explicitly: SynchronousFileSource(f)
    Source.synchronousFile(logFile).
      // parse chunks of bytes into lines
      via(Framing.delimiter(ByteString(System.lineSeparator), maximumFrameLength = 512, allowTruncation = true)).
      map(_.utf8String).
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
}
