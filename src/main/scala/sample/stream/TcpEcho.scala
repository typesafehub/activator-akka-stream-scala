package sample.stream

import java.net.InetSocketAddress
import akka.actor.ActorSystem
import akka.pattern.ask
import akka.stream.ActorFlowMaterializer
import akka.stream.scaladsl.{ Flow, Sink, Source, StreamTcp }
import akka.util.ByteString
import scala.concurrent.duration._
import scala.util.{ Failure, Success }

object TcpEcho {

  /**
   * Use without parameters to start both client and
   * server.
   *
   * Use parameters `server 0.0.0.0 6001` to start server listening on port 6001.
   *
   * Use parameters `client 127.0.0.1 6001` to start client connecting to
   * server on 127.0.0.1:6001.
   *
   */
  def main(args: Array[String]): Unit = {
    if (args.isEmpty) {
      val system = ActorSystem("ClientAndServer")
      val serverAddress = new InetSocketAddress("127.0.0.1", 6000)
      server(system, serverAddress)
      client(system, serverAddress)
    } else {
      val serverAddress =
        if (args.length == 3) new InetSocketAddress(args(1), args(2).toInt)
        else new InetSocketAddress("127.0.0.1", 6000)
      if (args(0) == "server") {
        val system = ActorSystem("Server")
        server(system, serverAddress)
      } else if (args(0) == "client") {
        val system = ActorSystem("Client")
        client(system, serverAddress)
      }
    }
  }

  def server(system: ActorSystem, serverAddress: InetSocketAddress): Unit = {
    implicit val sys = system
    import system.dispatcher
    implicit val materializer = ActorFlowMaterializer()

    val handler = Sink.foreach[StreamTcp.IncomingConnection] { conn =>
      println("Client connected from: " + conn.remoteAddress)
      conn handleWith Flow[ByteString]
    }

    val connections = StreamTcp().bind(serverAddress)
    val binding = connections.to(handler).run()

    binding.onComplete {
      case Success(b) =>
        println("Server started, listening on: " + b.localAddress)
      case Failure(e) =>
        println(s"Server could not bind to $serverAddress: ${e.getMessage}")
        system.shutdown()
    }

  }

  def client(system: ActorSystem, serverAddress: InetSocketAddress): Unit = {
    implicit val sys = system
    import system.dispatcher
    implicit val materializer = ActorFlowMaterializer()

    val testInput = ('a' to 'z').map(ByteString(_))

    val result = Source(testInput).via(StreamTcp().outgoingConnection(serverAddress)).
      runFold(ByteString.empty) { (acc, in) ⇒ acc ++ in }

    result.onComplete {
      case Success(result) =>
        println(s"Result: " + result.utf8String)
        println("Shutting down client")
        system.shutdown()
      case Failure(e) =>
        println("Failure: " + e.getMessage)
        system.shutdown()
    }
  }
}
