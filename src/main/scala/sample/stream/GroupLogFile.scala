package sample.stream

import java.io.FileOutputStream
import java.io.PrintWriter
import scala.io.Source
import scala.util.Try
import akka.actor.ActorSystem
import akka.stream.FlowMaterializer
import akka.stream.MaterializerSettings
import akka.stream.scaladsl.Flow

object GroupLogFile {
  def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem("Sys")
    val materializer = FlowMaterializer(MaterializerSettings())

    val LoglevelPattern = """.*\[(DEBUG|INFO|WARN|ERROR)\].*""".r

    // read lines from a log file
    val source = Source.fromFile("src/main/resources/logfile.txt", "utf-8")
    val completedFuture = Flow(source.getLines()).
      // group them by log level
      groupBy {
        case LoglevelPattern(level) => level
        case other                  => "OTHER"
      }.
      // write lines of each group to a separate file
      foreach {
        case (level, producer) =>
          val output = new PrintWriter(new FileOutputStream(s"target/log-$level.txt"), true)
          Flow(producer).
            foreach(line => output.println(line)).
            // close resource when the group stream is completed
            onComplete(materializer)(_ => Try(output.close()))
      }.
      onComplete(materializer) { _ =>
        Try(source.close())
        system.shutdown()
      }
  }
}