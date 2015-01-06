package sample.stream

import akka.actor.ActorSystem
import akka.stream.scaladsl.{ Sink, Source }
import akka.stream.{ FlowMaterializer, OverflowStrategy }
import java.io.{ FileOutputStream, PrintWriter }
import scala.util.Try

object GroupLogFile {

  def main(args: Array[String]): Unit = {
    // actor system and implicit materializer
    implicit val system = ActorSystem("Sys")
    // execution context
    import system.dispatcher

    implicit val materializer = FlowMaterializer()

    val LoglevelPattern = """.*\[(DEBUG|INFO|WARN|ERROR)\].*""".r
    val maximumLogLevelCount = 5

    // read lines from a log file
    val logFile = io.Source.fromFile("src/main/resources/logfile.txt", "utf-8")

    Source(() => logFile.getLines()).
      // group them by log level
      groupBy {
        case LoglevelPattern(level) => level
        case other                  => "OTHER"
      }.
      // write lines of each group to a separate file
      map {
        case (level, groupFlow) =>
          val output = new PrintWriter(new FileOutputStream(s"target/log-$level.txt"), true)
          // close resource when the group stream is completed
          // foreach returns a future that we can key the close() off of
          groupFlow.
            foreach(line => output.println(line)).
            andThen { case t => output.close(); t.get }
      }.
      buffer(maximumLogLevelCount, OverflowStrategy.error).
      mapAsync(identity).
      runWith(Sink.onComplete { _ =>
        Try(logFile.close())
        system.shutdown()
      })
  }
}
