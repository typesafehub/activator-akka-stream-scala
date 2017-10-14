package sample.stream

import java.nio.file.Paths

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorAttributes.SupervisionStrategy
import akka.stream.impl.Stages.DefaultAttributes
import akka.stream.impl.fusing.GraphInterpreter
import akka.stream._
import akka.stream.scaladsl._
import akka.stream.stage._
import akka.util.ByteString

import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration
import scala.util.Failure
import scala.util.Success
import scala.util.control.NonFatal

object GroupLogFile {

  def main(args: Array[String]): Unit = {
    // actor system and implicit materializer
    implicit val system = ActorSystem("Sys")
    implicit val materializer = ActorMaterializer()

    // execution context

    val LoglevelPattern = """.*\[(DEBUG|INFO|WARN|ERROR)\].*""".r

    // read lines from a log file
    val logFile = Paths.get("src/main/resources/logfile.txt")

    FileIO.fromPath(logFile).
      // parse chunks of bytes into lines
      via(Framing.delimiter(ByteString(System.lineSeparator), maximumFrameLength = 512, allowTruncation = true)).
      map(_.utf8String).
      map {
        case line @ LoglevelPattern(level) => (level, line)
        case other                         => ("OTHER", other)
      }.
      // group them by log level
      via(new GroupByUnsafe[(String, String), String](
        5,
        _._1,
        level ⇒ Flow[(String, String)].map(l ⇒ ByteString(l._2)).to(FileIO.toPath(Paths.get(s"target/log-$level.txt")))
      ))
      .run()
//      groupBy(5, _._1).
//      fold(("", List.empty[String])) {
//        case ((_, list), (level, line)) => (level, line :: list)
//      }.
//      // write lines of each group to a separate file
//      mapAsync(parallelism = 5) {
//        case (level, groupList) =>
//          Source(groupList.reverse).map(line => ByteString(line + "\n")).runWith(FileIO.toPath(Paths.get(s"target/log-$level.txt")))
//      }.
//      mergeSubstreams.
//      runWith(Sink.onComplete {
//        case Success(_) =>
//          system.terminate()
//        case Failure(e) =>
//          println(s"Failure: ${e.getMessage}")
//          system.terminate()
//      })
  }
}


/////////////////////////

/**
 * INTERNAL API
 */
final class GroupByUnsafe[T, K](
    val maxSubstreams: Int,
    val keyFor: T ⇒ K,
    val initSink: K => Sink[T, Any]
  ) extends GraphStage[FlowShape[T, Source[Any, NotUsed]]] {

  val in: Inlet[T] = Inlet("GroupByUnsafe.in")
  val out: Outlet[Source[T, NotUsed]] = Outlet("GroupByUnsafe.out")

  override val shape: FlowShape[T, Source[T, NotUsed]] = FlowShape(in, out)
  override def initialAttributes = DefaultAttributes.groupBy

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new TimerGraphStageLogic(shape) with OutHandler with InHandler {
    parent ⇒
    lazy val decider = inheritedAttributes.get[SupervisionStrategy].map(_.decider).getOrElse(Supervision.stoppingDecider)
    private val activeSubstreamsMap = new java.util.HashMap[Any, SubstreamSource]()
    private val closedSubstreams = new java.util.HashSet[Any]()
    private var timeout: FiniteDuration = _
    private var substreamWaitingToBePushed: Option[SubstreamSource] = None
    private var nextElementKey: K = null.asInstanceOf[K]
    private var nextElementValue: T = null.asInstanceOf[T]
    private var _nextId = 0
    private val substreamsJustStared = new java.util.HashSet[Any]()
    private var firstPushCounter: Int = 0

    private def nextId(): Long = { _nextId += 1; _nextId }

    private def hasNextElement = nextElementKey != null

    private def clearNextElement(): Unit = {
      nextElementKey = null.asInstanceOf[K]
      nextElementValue = null.asInstanceOf[T]
    }

    private def tryCompleteAll(): Boolean =
      if (activeSubstreamsMap.isEmpty || (!hasNextElement && firstPushCounter == 0)) {
        for (value ← activeSubstreamsMap.values().asScala) value.complete()
        completeStage()
        true
      } else false

    private def fail(ex: Throwable): Unit = {
      for (value ← activeSubstreamsMap.values().asScala) value.fail(ex)
      failStage(ex)
    }

    private def needToPull: Boolean = !(hasBeenPulled(in) || isClosed(in) || hasNextElement)

    override def preStart(): Unit =
      timeout = ActorMaterializerHelper.downcast(interpreter.materializer).settings.subscriptionTimeoutSettings.timeout

    override def onPull(): Unit = {
      substreamWaitingToBePushed match {
        case Some(substreamSource) ⇒
          push(out, Source.fromGraph(substreamSource.source))
          scheduleOnce(substreamSource.key, timeout)
          substreamWaitingToBePushed = None
        case None ⇒
          if (hasNextElement) {
            val subSubstreamSource = activeSubstreamsMap.get(nextElementKey)
            if (subSubstreamSource.isAvailable) {
              subSubstreamSource.push(nextElementValue)
              clearNextElement()
            }
          } else if (!hasBeenPulled(in)) tryPull(in)
      }
    }

    override def onUpstreamFailure(ex: Throwable): Unit = fail(ex)

    override def onDownstreamFinish(): Unit =
      if (activeSubstreamsMap.isEmpty) completeStage() else setKeepGoing(true)

    override def onPush(): Unit = try {
      val elem = grab(in)
      val key = keyFor(elem)
      if(key == null) throw new IllegalArgumentException("Key cannot be null")
      val substreamSource = activeSubstreamsMap.get(key)
      if (substreamSource != null) {
        if (substreamSource.isAvailable) {
          substreamSource.push(elem)
        } else {
          nextElementKey = key
          nextElementValue = elem
        }
      } else {
        if (activeSubstreamsMap.size == maxSubstreams) {
          fail(new IllegalStateException(s"Cannot open substream for key '$key': too many substreams open"))
        } else if (closedSubstreams.contains(key) && !hasBeenPulled(in)) {
          pull(in)
        } else {
          runSubstream(key, elem)
        }
      }
    } catch {
      case NonFatal(ex) ⇒
        decider(ex) match {
          case Supervision.Stop                         ⇒ fail(ex)
          case Supervision.Resume | Supervision.Restart ⇒ if (!hasBeenPulled(in)) pull(in)
        }
    }

    override def onUpstreamFinish(): Unit = {
      if (!tryCompleteAll()) setKeepGoing(true)
    }

    private def runSubstream(key: K, value: T): Unit = {
      val substreamSource = new SubstreamSource("GroupByUnsafeSource " + nextId, key, value)
      activeSubstreamsMap.put(key, substreamSource)
      firstPushCounter += 1
      if (isAvailable(out)) {
        val sinkForKey = initSink(key)
        Source.fromGraph(substreamSource.source).runWith(sinkForKey)(GraphInterpreter.currentInterpreter.materializer)
        // push(out, Source.fromGraph(substreamSource.source))
        scheduleOnce(key, timeout)
        substreamWaitingToBePushed = None
      } else {
        setKeepGoing(true)
        substreamsJustStared.add(substreamSource)
        substreamWaitingToBePushed = Some(substreamSource)
      }
    }

    override protected def onTimer(timerKey: Any): Unit = {
      val substreamSource = activeSubstreamsMap.get(timerKey)
      if (substreamSource != null) {
        substreamSource.timeout(timeout)
        closedSubstreams.add(timerKey)
        activeSubstreamsMap.remove(timerKey)
        if (isClosed(in)) tryCompleteAll()
      }
    }

    setHandlers(in, out, this)

    private class SubstreamSource(name: String, val key: K, var firstElement: T) extends SubSourceOutlet[T](name) with OutHandler {
      def firstPush(): Boolean = firstElement != null
      def hasNextForSubSource = hasNextElement && nextElementKey == key
      private def completeSubStream(): Unit = {
        complete()
        activeSubstreamsMap.remove(key)
        closedSubstreams.add(key)
      }

      private def tryCompleteHandler(): Unit = {
        if (parent.isClosed(in) && !hasNextForSubSource) {
          completeSubStream()
          tryCompleteAll()
        }
      }

      override def onPull(): Unit = {
        cancelTimer(key)
        if (firstPush) {
          firstPushCounter -= 1
          push(firstElement)
          firstElement = null.asInstanceOf[T]
          substreamsJustStared.remove(this)
          if (substreamsJustStared.isEmpty) setKeepGoing(false)
        } else if (hasNextForSubSource) {
          push(nextElementValue)
          clearNextElement()
        } else if (needToPull) pull(in)

        tryCompleteHandler()
      }

      override def onDownstreamFinish(): Unit = {
        if (hasNextElement && nextElementKey == key) clearNextElement()
        if (firstPush()) firstPushCounter -= 1
        completeSubStream()
        if (parent.isClosed(in)) tryCompleteAll() else if (needToPull) pull(in)
      }

      setHandler(this)
    }
  }

  override def toString: String = "GroupBy"

}
