package kvstore

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging, ActorRef, Props}

import scala.concurrent.duration._

object Replicator {
  case class Replicate(key: String, valueOption: Option[String], id: Long)
  case class Replicated(key: String, id: Long)
  
  case class Snapshot(key: String, valueOption: Option[String], seq: Long)
  case class SnapshotAck(key: String, seq: Long)

  def props(replica: ActorRef): Props = Props(new Replicator(replica))
}

class Replicator(val replica: ActorRef) extends Actor with ActorLogging {
  import Replicator._
  import Replica._
  import context.dispatcher
  var timer = FiniteDuration(100, TimeUnit.MILLISECONDS)
  case class Check(val msg: String)

  override def preStart(): Unit = {
    super.preStart()
    //implicit val ec = context.dispatcher
    context.system.scheduler.schedule(
      timer,
      timer,
      self,
      new Check("everybody"))
  }
  
  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  // map from sequence number to pair of sender and request
  var acks = Map.empty[Long, (ActorRef, Replicate)]
  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
  var pending = Vector.empty[Snapshot]
  
  var _seqCounter = 0L
  def nextSeq() = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }

  def sendSnapshot(act: ActorRef, r: Replicate, seq: Long): Unit = {
    val snapshot = Snapshot(r.key, r.valueOption, seq)
    act ! snapshot
    acks += ((seq) -> ((act, r)))
  }

  
  /* TODO Behavior for the Replicator. */
  def receive: Receive = {
    case r: Replicate => {
      val snapshot = Snapshot(r.key, r.valueOption, nextSeq())
      replica ! snapshot
      acks += ((snapshot.seq , (sender,r)))}
    case s: SnapshotAck => {
      val tuple = acks(s.seq)
      acks -= s.seq
      tuple._1 ! Replicated(tuple._2.key, tuple._2.id)
    }
    case c: Check => {
      log.warning("0,1 check")
      acks.keys.foreach(key => { sendSnapshot(replica, acks(key)._2, key)})
    }
  }

}
