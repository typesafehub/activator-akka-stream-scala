package sample.stream

import java.io.{ FileOutputStream, PrintWriter }

import akka.actor.ActorSystem
import akka.stream.FlowMaterializer
import akka.stream.scaladsl.{ Broadcast, FlowGraph, ForeachSink, Source }

import scala.concurrent.forkjoin.ThreadLocalRandom
import scala.util.{ Failure, Success, Try }
import akka.stream.scaladsl.FlowGraphImplicits

object WritePrimes {

  def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem("Sys")
    import system.dispatcher
    implicit val materializer = FlowMaterializer()

    // generate random numbers
    val maxRandomNumberSize = 1000000
    val primeSource: Source[Int] =
      Source(() => Some(ThreadLocalRandom.current().nextInt(maxRandomNumberSize))).
        // filter prime numbers
        filter(rnd => isPrime(rnd)).
        // and neighbor +2 is also prime
        filter(prime => isPrime(prime + 2))

    // write to file sink
    val output = new PrintWriter(new FileOutputStream("target/primes.txt"), true)
    val slowSink = ForeachSink[Int] { prime =>
      output.println(prime)
      // simulate slow consumer
      Thread.sleep(1000)
    }

    // console output sink
    val consoleSink = ForeachSink[Int](println)

    // send primes to both slow file sink and console sink using graph API
    val materialized = FlowGraph { implicit builder =>
      import FlowGraphImplicits._
      val broadcast = Broadcast[Int] // the splitter - like a Unix tee
      primeSource ~> broadcast ~> slowSink // connect primes to splitter, and one side to file
      broadcast ~> consoleSink // connect other side of splitter to console
    }.run()

    // ensure the output file is closed and the system shutdown upon completion
    materialized.get(slowSink).onComplete {
      case Success(_) =>
        Try(output.close())
        system.shutdown()
      case Failure(e) =>
        println(s"Failure: ${e.getMessage}")
        Try(output.close())
        system.shutdown()
    }

  }

  def isPrime(n: Int): Boolean = {
    if (n <= 1) false
    else if (n == 2) true
    else !(2 to (n - 1)).exists(x => n % x == 0)
  }
}
