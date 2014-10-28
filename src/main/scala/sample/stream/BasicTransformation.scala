package sample.stream

import akka.actor.ActorSystem
import akka.stream.MaterializerSettings
import akka.stream.scaladsl2._

object BasicTransformation {

  def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem("Sys")
    import system.dispatcher

    implicit val materializer = FlowMaterializer()

    val text =
      """|Lorem Ipsum is simply dummy text of the printing and typesetting industry.
         |Lorem Ipsum has been the industry's standard dummy text ever since the 1500s, 
         |when an unknown printer took a galley of type and scrambled it to make a type 
         |specimen book.""".stripMargin

    Source(text.split("\\s").iterator).
      map(_.toUpperCase).
      foreach(println).
      onComplete(_ => system.shutdown())

    // could also use .runWith(ForeachDrain(println)) instead of .foreach(println) above
    // as it is shorthand for the same thing. Drains may be constructed elsewhere and plugged
    // in like this. Note also that foreach returns a future (in either form) which may be
    // used to attach lifecycle events to, like here with the onComplete.
  }
}
