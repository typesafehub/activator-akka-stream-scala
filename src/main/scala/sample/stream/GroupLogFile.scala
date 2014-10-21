package sample.stream

import akka.actor.ActorSystem
import akka.stream.MaterializerSettings
import akka.stream.scaladsl2._
import java.io.{ FileOutputStream, PrintWriter }
import scala.util.Try

object GroupLogFile {

  def main(args: Array[String]): Unit = {
    // execution context
    import system.dispatcher

    // actor system and implicit materializer
    val system = ActorSystem("Sys")
    implicit val materializer = FlowMaterializer(MaterializerSettings(system))(system)

    val LoglevelPattern = """.*\[(DEBUG|INFO|WARN|ERROR)\].*""".r

    // read lines from a log file
    val source = io.Source.fromFile("src/main/resources/logfile.txt", "utf-8")

    Source(source.getLines()).
      // group them by log level
      groupBy {
        case LoglevelPattern(level) => level
        case other                  => "OTHER"
      }.
      // write lines of each group to a separate file
      foreach {
        case (level, groupFlow) =>
          val output = new PrintWriter(new FileOutputStream(s"target/log-$level.txt"), true)
          // close resource when the group stream is completed
          // foreach returns a future that we can key the close() off of
          groupFlow.foreach(line => output.println(line)).onComplete(_ => Try(output.close()))
      }.
      onComplete { _ =>
        Try(source.close())
        system.shutdown()
      }
  }
}
