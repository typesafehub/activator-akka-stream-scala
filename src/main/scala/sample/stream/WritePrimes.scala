package sample.stream

import java.io.{ FileOutputStream, PrintWriter }
import scala.concurrent.forkjoin.ThreadLocalRandom
import scala.util.{ Failure, Success }
import akka.actor.ActorSystem
import akka.stream.scaladsl2.FlowFrom
import akka.stream.scaladsl2.FlowGraph
import akka.stream.scaladsl2.FlowGraphImplicits._
import akka.stream.scaladsl2.FlowMaterializer
import akka.stream.scaladsl2.ForeachSink
import akka.stream.scaladsl2.Broadcast

object WritePrimes {

  def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem("Sys")
    implicit val materializer = FlowMaterializer()(system)

    // generate random numbers
    val maxRandomNumberSize = 1000000
    val primeFlow = FlowFrom(() => Some(ThreadLocalRandom.current().nextInt(maxRandomNumberSize))).
      // filter prime numbers
      filter(rnd => isPrime(rnd)).
      // and neighbor +2 is also prime
      filter(prime => isPrime(prime + 2))

    // write to file  
    val output = new PrintWriter(new FileOutputStream("target/primes.txt"), true)
    val slowSink = ForeachSink[Int] { prime =>
      output.println(prime)
      // simulate slow consumer
      Thread.sleep(1000)
    }

    // write to console  
    val consoleSink = ForeachSink[Int](println)

    // connect flows
    val materialized = FlowGraph { implicit builder =>
      val broadcast = Broadcast[Int]
      primeFlow ~> broadcast ~> slowSink
      broadcast ~> consoleSink
    }.run()

    // shutdown when done
    import system.dispatcher
    slowSink.future(materialized).onComplete {
      case Success(_) => system.shutdown()
      case Failure(e) =>
        println("Failure: " + e.getMessage)
        system.shutdown()
    }

  }

  def isPrime(n: Int): Boolean = {
    if (n <= 1) false
    else if (n == 2) true
    else !(2 to (n - 1)).exists(x => n % x == 0)
  }
}
