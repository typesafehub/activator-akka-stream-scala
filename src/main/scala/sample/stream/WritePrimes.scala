package sample.stream

import akka.actor.ActorSystem
import akka.stream.{ FlowMaterializer, MaterializerSettings }
import akka.stream.scaladsl.Flow
import java.io.{ FileOutputStream, PrintWriter }
import org.reactivestreams.api.Producer
import scala.concurrent.forkjoin.ThreadLocalRandom
import scala.util.Try

object WritePrimes {

  def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem("Sys")
    val materializer = FlowMaterializer(MaterializerSettings())

    // generate random numbers
    val maxRandomNumberSize = 1000000
    val producer: Producer[Int] =
      Flow(() => ThreadLocalRandom.current().nextInt(maxRandomNumberSize)).
        // filter prime numbers
        filter(rnd => isPrime(rnd)).
        // and neighbor +2 is also prime
        filter(prime => isPrime(prime + 2)).
        toProducer(materializer)

    // connect two consumer flows to the producer  

    // write to file  
    val output = new PrintWriter(new FileOutputStream("target/primes.txt"), true)
    Flow(producer).
      foreach { prime =>
        output.println(prime)
        // simulate slow consumer
        Thread.sleep(1000)
      }.
      onComplete(materializer) { _ =>
        Try(output.close())
        system.shutdown()
      }

    // write to console  
    Flow(producer).
      foreach(println).
      consume(materializer)

  }

  def isPrime(n: Int): Boolean = {
    if (n <= 1) false
    else if (n == 2) true
    else !(2 to (n - 1)).exists(x => n % x == 0)
  }
}
