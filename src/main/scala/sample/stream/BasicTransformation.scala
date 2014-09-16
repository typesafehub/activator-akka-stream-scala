package sample.stream

import scala.util.{ Failure, Success }
import akka.actor.ActorSystem
import akka.stream.scaladsl2.FlowFrom
import akka.stream.scaladsl2.FlowMaterializer
import akka.stream.scaladsl2.ForeachSink

object BasicTransformation {

  def main(args: Array[String]): Unit = {
    val system = ActorSystem("Sys")
    implicit val materializer = FlowMaterializer()(system)

    val text =
      """|Lorem Ipsum is simply dummy text of the printing and typesetting industry.
         |Lorem Ipsum has been the industry's standard dummy text ever since the 1500s, 
         |when an unknown printer took a galley of type and scrambled it to make a type 
         |specimen book.""".stripMargin

    val flow = FlowFrom(text.split("\\s").toVector).
      // transform
      map(line => line.toUpperCase)

    val foreachSink = ForeachSink[String](transformedLine => println(transformedLine))
    val materializedFlow = flow.withSink(foreachSink).run()

    // shutdown when done
    import system.dispatcher
    foreachSink.future(materializedFlow).onComplete {
      case Success(_) => system.shutdown()
      case Failure(e) =>
        println("Failure: " + e.getMessage)
        system.shutdown()
    }
  }
}
