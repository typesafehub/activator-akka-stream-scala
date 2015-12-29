package sample.stream

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source

object BasicTransformation {

  def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem("Sys")
    import system.dispatcher

    implicit val materializer = ActorMaterializer()

    val text =
      """|Lorem Ipsum is simply dummy text of the printing and typesetting industry.
         |Lorem Ipsum has been the industry's standard dummy text ever since the 1500s,
         |when an unknown printer took a galley of type and scrambled it to make a type
         |specimen book.""".stripMargin

    Source.fromIterator(() => text.split("\\s").iterator).
      map(_.toUpperCase).
      runForeach(println).
      onComplete(_ => system.shutdown())

    // could also use .runWith(Sink.foreach(println)) instead of .runForeach(println) above
    // as it is a shorthand for the same thing. Sinks may be constructed elsewhere and plugged
    // in like this. Note also that foreach returns a future (in either form) which may be
    // used to attach lifecycle events to, like here with the onComplete.
  }
}
