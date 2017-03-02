package sample.stream

import java.io.InputStream
import java.net.InetSocketAddress
import java.security.{ KeyStore, SecureRandom }
import javax.net.ssl.{ SSLContext, SSLParameters, TrustManagerFactory }

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.{ ActorMaterializer, TLSClientAuth, TLSProtocol, TLSRole }
import akka.stream.scaladsl.Tcp.ServerBinding
import akka.stream.scaladsl._
import akka.util.ByteString
import com.typesafe.sslconfig.akka.AkkaSSLConfig
import com.typesafe.sslconfig.ssl.{ ClientAuth, KeyManagerFactoryWrapper, SSLConfigSettings }

import scala.concurrent.Future
import scala.util.{ Failure, Success }

object TcpTLSEcho {

  /**
   * Use without parameters to start both client and server.
   * <p>
   * Use parameters `server 0.0.0.0 6001` to start server listening on port
   * 6001.
   * <p>
   * Use parameters `client 127.0.0.1 6001` to start client connecting to server
   * on 127.0.0.1:6001.
   */
  def main(args: Array[String]) {
    if (args.length == 0) {
      val serverSystem: ActorSystem = ActorSystem.create("Server")
      val clientSystem: ActorSystem = ActorSystem.create("Client")
      val serverAddress: InetSocketAddress = new InetSocketAddress("127.0.0.1", 6000)
      server(serverSystem, serverAddress)
      // http://typesafehub.github.io/ssl-config/CertificateGeneration.html#client-configuration
      client(clientSystem, serverAddress)
    } else {
      var serverAddress: InetSocketAddress = null
      if (args.length == 3) {
        serverAddress = new InetSocketAddress(args(1), Integer.valueOf(args(2)))
      } else {
        serverAddress = new InetSocketAddress("127.0.0.1", 6000)
      }
      if (args(0) == "server") {
        val system: ActorSystem = ActorSystem.create("Server")
        server(system, serverAddress)
      } else if (args(0) == "client") {
        val system: ActorSystem = ActorSystem.create("Client")
        client(system, serverAddress)
      }
    }
  }

  // ------------------------ server ------------------------------ 
  def server(system: ActorSystem, serverAddress: InetSocketAddress): Unit = {
    implicit val materializer = ActorMaterializer()(system)
    import system.dispatcher

    val handler = Sink.foreach[Tcp.IncomingConnection] { conn =>
      system.log.info("Client connected from: " + conn.remoteAddress)

      val h: Flow[ByteString, ByteString, NotUsed] =
        tlsStage(system, TLSRole.server).reversed
          .join(
            Flow[ByteString]
              .log("in the server handler", bs => bs.utf8String)
              .takeWhile(p => p.utf8String.endsWith("z"), inclusive = true) // close connection once "z" received (last letter we expect)
          )

      conn.handleWith(h)
    }
    val bindingFuture: Future[ServerBinding] =
      Tcp(system)
        .bind(serverAddress.getHostString, serverAddress.getPort)
        .to(handler)
        .run()

    bindingFuture.foreach { binding =>
      system.log.info("Server started, listening on: " + binding.localAddress)
      NotUsed.getInstance()
    }
  }

  // ------------------------ end of server ------------------------------ 

  // ------------------------ client ------------------------------ 
  def client(system: ActorSystem, serverAddress: InetSocketAddress): Unit = {
    implicit val materializer = ActorMaterializer()(system)
    import system.dispatcher

    val testInput = ('a' to 'z').map(c => String.valueOf(c)).map(ByteString(_)).toList

    val sink = Sink.fold[ByteString, ByteString](ByteString.empty) { case (acc, in) => acc ++ in }
    val source = Source(testInput).log("Element out", bs => bs.utf8String)
    val connection = Tcp(system).outgoingConnection(serverAddress.getHostString, serverAddress.getPort)

    val result =
      Flow.fromSinkAndSourceMat(sink, source)(Keep.left)
        .joinMat(tlsStage(system, TLSRole.client))(Keep.left)
        .joinMat(Flow[ByteString].via(connection))(Keep.left)
        .run()

    result onComplete {
      case Success(success) =>
        system.log.info("Result: " + success.utf8String)
      case Failure(failure) =>
        system.log.info("Failure: " + failure.getMessage)
    }
    system.log.info("Shutting down client")
    system.terminate()
    NotUsed.getInstance()
  }

  // ------------------------ end of client ------------------------------ 

  // ------------------------ setting up TLS ------------------------
  private def tlsStage(system: ActorSystem, role: TLSRole): BidiFlow[ByteString, ByteString, ByteString, ByteString, NotUsed] = {
    val sslConfig: AkkaSSLConfig = AkkaSSLConfig.get(system)
    val config: SSLConfigSettings = sslConfig.config
    // -----------
    val password: Array[Char] = "abcdef".toCharArray
    // do not store passwords in code, read them from somewhere safe!
    val ks: KeyStore = KeyStore.getInstance("PKCS12")
    val keystore: InputStream = TcpTLSEcho.getClass.getClassLoader.getResourceAsStream("keys/server.p12")
    ks.load(keystore, password)
    // initial ssl context
    val tmf: TrustManagerFactory = TrustManagerFactory.getInstance("SunX509")
    tmf.init(ks)
    val keyManagerFactory: KeyManagerFactoryWrapper = sslConfig.buildKeyManagerFactory(config)
    keyManagerFactory.init(ks, password)
    val sslContext: SSLContext = SSLContext.getInstance("TLS")
    sslContext.init(keyManagerFactory.getKeyManagers, tmf.getTrustManagers, new SecureRandom)
    // protocols
    val defaultParams: SSLParameters = sslContext.getDefaultSSLParameters
    val defaultProtocols: Array[String] = defaultParams.getProtocols
    val protocols: Array[String] = sslConfig.configureProtocols(defaultProtocols, config)
    defaultParams.setProtocols(protocols)
    // ciphers
    val defaultCiphers: Array[String] = defaultParams.getCipherSuites
    val cipherSuites: Array[String] = sslConfig.configureCipherSuites(defaultCiphers, config)
    defaultParams.setCipherSuites(cipherSuites)

    var firstSession: TLSProtocol.NegotiateNewSession =
      TLSProtocol.NegotiateNewSession
        .withCipherSuites(cipherSuites: _*)
        .withProtocols(protocols: _*)
        .withParameters(defaultParams)

    // auth
    val clientAuth = getClientAuth(config.sslParametersConfig.clientAuth)
    clientAuth foreach { a => firstSession = firstSession.withClientAuth(a) }

    val tls = TLS(sslContext, firstSession, role)

    val pf: PartialFunction[TLSProtocol.SslTlsInbound, ByteString] = {
      case sb: TLSProtocol.SessionBytes =>
        sb.bytes
      case ib: TLSProtocol.SslTlsInbound =>
        system.log.info("Received other that SessionBytes" + ib)
        null

    }

    val tlsSupport: BidiFlow[ByteString, TLSProtocol.SslTlsOutbound, TLSProtocol.SslTlsInbound, ByteString, NotUsed] =
      BidiFlow.fromFlows(
        Flow[ByteString].map(TLSProtocol.SendBytes(_)),
        Flow[TLSProtocol.SslTlsInbound].collect(pf)
      )

    tlsSupport.atop(tls)
  }

  private def getClientAuth(auth: ClientAuth): Option[TLSClientAuth] =
    auth match {
      case ClientAuth.Want => Some(TLSClientAuth.Want)
      case ClientAuth.Need => Some(TLSClientAuth.Need)
      case ClientAuth.None => Some(TLSClientAuth.None)
      case _               => None
    }
}
