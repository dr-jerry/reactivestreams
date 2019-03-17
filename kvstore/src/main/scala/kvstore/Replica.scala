package kvstore

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging, ActorRef, Cancellable, NotInfluenceReceiveTimeout, OneForOneStrategy, PoisonPill, Props, ReceiveTimeout, SupervisorStrategy, Terminated}
import kvstore.Arbiter._

import scala.collection.immutable.Queue
import akka.actor.SupervisorStrategy.Restart

import scala.annotation.tailrec
import akka.pattern.{ask, pipe}

import scala.concurrent.duration._
import akka.util.Timeout
trait AckAble;

object Replica {

  sealed trait Operation {
    def key: String
    def id: Long
  }
  case class Insert(key: String, value: String, id: Long) extends Operation with AckAble with NotInfluenceReceiveTimeout
  case class Remove(key: String, id: Long) extends Operation with AckAble
  case class Get(key: String, id: Long) extends Operation
  case class CheckPersist(id: Long, v: Option[String]) extends NotInfluenceReceiveTimeout
  case class CheckPersist2(duration: Duration)

  sealed trait OperationReply
  case class OperationAck(id: Long) extends OperationReply
  case class OperationFailed(id: Long) extends OperationReply
  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor with ActorLogging {
  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher
  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */
  
  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]
  var persistRef : ActorRef = context.actorOf(persistenceProps)
  val ms100 = FiniteDuration(100L, TimeUnit.MILLISECONDS)
  def scheduler(cp: CheckPersist) = {
    context.system.scheduler.schedule(ms100,ms100,self, cp)
  }
  // Map of unacked operations which need to be resend, failed on notification,
  // ActorRef1 Actor sentTo, and Actor Expecting Ack from.
  // Long1 id
  // ActorRef2 Actor received Operation from.
  // Long2 the timestamp.
  var unacked = Map.empty[(ActorRef, Long), (ActorRef, AckAble, Long)]

  arbiter ! Join

  def receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica(0L))
  }

  /* TODO Behavior for  the leader role. */
  val leader: Receive = {
    case Get(key, id) => context.sender() ! GetResult(key, kv.get(key), id)
    case i: Insert => {
      kv += (i.key -> i.value)
      var operation = Persist(i.key, Some(i.value), i.id)
      log.warning(s"send $operation to $persistRef")
      unacked += ((persistRef, i.id) -> ((context.sender(), operation, System.currentTimeMillis())))
      //var schedule = scheduler(CheckPersist(i.id, Some(i.value))) //5.2
      persistRef ! operation
//      context.setReceiveTimeout(1000.milliseconds)
//      context.become(insertAwait(context.sender(), i, schedule, replicators), false)
      for (ref <- replicators) {
        var repl = Replicate(i.key, Some(i.value), i.id)
        log.warning(s"sending Snapshot $repl  to $ref")
        ref ! repl
      }
    }
    case Remove(key, id) => {
      kv -= (key)
      context.sender() ! OperationAck(id)
    }
    case Replicas(replicas) => {
      val added = replicas -- secondaries.keys.toSet
      var removed = secondaries.keys.toSet -- replicas
      var newMap = added.map(replica => (replica, context.actorOf(Replicator.props(replica))))
      secondaries ++= newMap
//      for (replicator <- newMap.toMap.values) {
//        for (entry <- kv) {
//          replicator ! Snapshot(entry._1, Some(entry._2), nex)
//        }
//      }
      secondaries -= self
      replicators = secondaries.values.toSet
      for (elem <- secondaries) { log.warning(s"secondaries $elem")}
    }

    case CheckPersist2(duration) => {
      unacked.foreach(entry => {
        var ((recvr, id), (origSendr, operation, starttime)) = entry
        if (System.currentTimeMillis() - starttime > duration.toMillis) {
          origSendr ! OperationFailed(id)
        } else {
          recvr ! operation
        }
      })
    }
    case Persisted(pkey, id) => {
      log.warning(s"received Persisted $pkey, $id, ${sender()}")
      unacked.foreach(entry => {
        var ((rcvr, id), (origSendr, operation, dur)) = entry
        log.warning(s"unacked ($rcvr, $id) = > ($origSendr $operation $dur")
      })
      unacked.get((sender, id)).map(entry => {
        var (origSendr, operation, dur) = entry
        origSendr ! OperationAck(id)
      })
    }
  }

  def insertAwait(origSender: ActorRef, i: Insert, schedule: Cancellable, replicas: Set[ActorRef] ): Receive = {
    case Persisted(pkey, id) if(id==i.id && i.key == pkey) => {
      origSender ! OperationAck(i.id)
      context.setReceiveTimeout(Duration.Inf)
      schedule.cancel()
      context.unbecome()
    }
    case CheckPersist(pid, valueOption) if(pid==i.id) => {
      log.warning("firing persist again")
      persistRef ! Persist(i.key, valueOption, i.id)
    }
    case r: ReceiveTimeout => {
      log.warning(s"timed out $r")
      origSender ! OperationFailed(i.id)
    }
  }


  /* TODO Behavior for the replica role. */
  def replica(seq: Long): Receive = {
    case s:Snapshot if (s.seq <= seq) => { if (s.seq == seq) {
      context.become(replica(seq + 1))
      s.valueOption match {
        case Some(value) => kv += (s.key -> value)
        case None => kv -= s.key
      }}
      persistRef ! Persist(s.key, s.valueOption, s.seq)
      var schedule = scheduler(CheckPersist(seq, s.valueOption))
      context.become(persistAwait(s.key, s.seq, sender, schedule), false)
    }
    case Get(key, id) => sender ! GetResult(key, kv.get(key),id)
  }

  def persistAwait(key: String, seq: Long, origSender: ActorRef, schedule: Cancellable): Receive = {
    case Persisted(pkey, id) if(id==seq && key == pkey) => {
      origSender ! SnapshotAck(key, seq)
      schedule.cancel()
      context.unbecome()
    }
    case CheckPersist(pid, valueOption) if(pid==seq) => {
      persistRef ! Persist(key, valueOption, seq)
    }
    case Get(key, id) => sender ! GetResult(key, kv.get(key),id)
  }

  override def preStart(): Unit = {
    super.preStart()
    context.system.scheduler.schedule(ms100, ms100, self, CheckPersist2(1000.milliseconds))
  }
}

