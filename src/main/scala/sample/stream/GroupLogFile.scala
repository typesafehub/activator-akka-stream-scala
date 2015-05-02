package sample.stream

import java.io.File

import akka.actor.ActorSystem
import akka.stream.ActorFlowMaterializer
import akka.stream.io.Implicits._
import akka.stream.scaladsl._
import akka.util.ByteString

import scala.util.Try

object GroupLogFile {

  def main(args: Array[String]): Unit = {
    // actor system and implicit materializer
    implicit val system = ActorSystem("Sys")
    implicit val materializer = ActorFlowMaterializer()

    // execution context
    import system.dispatcher

    val LoglevelPattern = """.*\[(DEBUG|INFO|WARN|ERROR)\].*""".r

    // read lines from a log file
    val logFile = io.Source.fromFile("src/main/resources/logfile.txt", "utf-8")

    Source(() => logFile.getLines()).
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
        Try(logFile.close())
        system.shutdown()
      })
  }
}
