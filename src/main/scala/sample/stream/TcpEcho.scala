package sample.stream

import akka.actor.ActorSystem
import akka.io.IO
import akka.pattern.ask
import akka.stream.{ FlowMaterializer, MaterializerSettings }
import akka.stream.io.StreamTcp
import akka.stream.scaladsl.Flow
import akka.util.{ ByteString, Timeout }
import java.net.InetSocketAddress
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
    implicit val ec = system.dispatcher
    val settings = MaterializerSettings()
    val materializer = FlowMaterializer(settings)
    implicit val timeout = Timeout(5.seconds)

    val serverFuture = IO(StreamTcp) ? StreamTcp.Bind(settings, serverAddress)

    serverFuture.onSuccess {
      case serverBinding: StreamTcp.TcpServerBinding =>
        println("Server started, listening on: " + serverBinding.localAddress)

        Flow(serverBinding.connectionStream).foreach { conn ⇒
          println("Client connected from: " + conn.remoteAddress)
          conn.inputStream.produceTo(conn.outputStream)
        }.consume(materializer)
    }

    serverFuture.onFailure {
      case e: Throwable =>
        println(s"Server could not bind to $serverAddress: ${e.getMessage}")
        system.shutdown()
    }

  }

  def client(system: ActorSystem, serverAddress: InetSocketAddress): Unit = {
    implicit val sys = system
    implicit val ec = system.dispatcher
    val settings = MaterializerSettings()
    val materializer = FlowMaterializer(settings)
    implicit val timeout = Timeout(5.seconds)

    val clientFuture = IO(StreamTcp) ? StreamTcp.Connect(settings, serverAddress)
    clientFuture.onSuccess {
      case clientBinding: StreamTcp.OutgoingTcpConnection =>
        val testInput = ('a' to 'z').map(ByteString(_))
        Flow(testInput).toProducer(materializer).produceTo(clientBinding.outputStream)

        Flow(clientBinding.inputStream).fold(Vector.empty[Char]) { (acc, in) ⇒ acc ++ in.map(_.asInstanceOf[Char]) }.
          foreach(result => println(s"Result: " + result.mkString("[", ", ", "]"))).
          onComplete(materializer) {
            case Success(_) =>
              println("Shutting down client")
              system.shutdown()
            case Failure(e) =>
              println("Failure: " + e.getMessage)
              system.shutdown()
          }
    }

    clientFuture.onFailure {
      case e: Throwable =>
        println(s"Client could not connect to $serverAddress: ${e.getMessage}")
        system.shutdown()
    }
  }
}
