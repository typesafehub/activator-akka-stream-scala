package sample.stream

import java.nio.file.Paths

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl._
import akka.util.ByteString

import scala.util.Failure
import scala.util.Success

object GroupLogFile {

  def main(args: Array[String]): Unit = {
    // actor system and implicit materializer
    implicit val system = ActorSystem("Sys")
    implicit val materializer = ActorMaterializer()

    // execution context

    val LoglevelPattern = """.*\[(DEBUG|INFO|WARN|ERROR)\].*""".r

    // read lines from a log file
    val logFile = Paths.get("src/main/resources/logfile.txt")

    FileIO.fromPath(logFile).
      // parse chunks of bytes into lines
      via(Framing.delimiter(ByteString(System.lineSeparator), maximumFrameLength = 512, allowTruncation = true)).
      map(_.utf8String).
      map {
        case line @ LoglevelPattern(level) => (level, line)
        case line @ other                  => ("OTHER", line)
      }.
      // group them by log level
      groupBy(5, _._1).
      fold(("", List.empty[String])) {
        case ((_, list), (level, line)) => (level, line :: list)
      }.
      // write lines of each group to a separate file
      mapAsync(parallelism = 5) {
        case (level, groupList) =>
          Source(groupList.reverse).map(line => ByteString(line + "\n")).runWith(FileIO.toPath(Paths.get(s"target/log-$level.txt")))
      }.
      mergeSubstreams.
      runWith(Sink.onComplete {
        case Success(_) =>
          system.terminate()
        case Failure(e) =>
          println(s"Failure: ${e.getMessage}")
          system.terminate()
      })
  }
}
