package sample.stream

import java.io.{ FileOutputStream, PrintWriter }
import scala.io.Source
import scala.util.Try
import akka.actor.ActorSystem
import akka.stream.scaladsl2.FlowFrom
import akka.stream.scaladsl2.FlowMaterializer
import akka.stream.scaladsl2.FlowWithSource
import akka.stream.scaladsl2.ForeachSink

object GroupLogFile {

  def main(args: Array[String]): Unit = {
    val system = ActorSystem("Sys")
    implicit val materializer = FlowMaterializer()(system)

    val LoglevelPattern = """.*\[(DEBUG|INFO|WARN|ERROR)\].*""".r

    // read lines from a log file
    val source = Source.fromFile("src/main/resources/logfile.txt", "utf-8")

    val flow = FlowFrom(source.getLines()).
      // group them by log level
      groupBy {
        case LoglevelPattern(level) => level
        case other                  => "OTHER"
      }

    // write lines of each group to a separate file
    val foreachSink = ForeachSink[(String, FlowWithSource[String, String])] {
      case (level, groupFlow) =>
        val output = new PrintWriter(new FileOutputStream(s"target/log-$level.txt"), true)
        val foreach = ForeachSink[String](line => output.println(line))
        // close resource when the group stream is completed
        import system.dispatcher
        foreach.future(groupFlow.withSink(foreach).run()).onComplete(_ => Try(output.close()))

    }
    val materializedFlow = flow.withSink(foreachSink).run()

    // shutdown when done
    import system.dispatcher
    foreachSink.future(materializedFlow).onComplete { _ =>
      Try(source.close())
      system.shutdown()
    }
  }
}
