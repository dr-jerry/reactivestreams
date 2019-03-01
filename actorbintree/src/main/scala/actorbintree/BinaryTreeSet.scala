/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package actorbintree

import akka.actor._

import scala.collection.immutable.Queue

object BinaryTreeSet {

  trait Operation {
    def requester: ActorRef
    def id: Int
    def elem: Int
  }

  trait OperationReply {
    def id: Int
  }

  trait TestReply {
    def id: Int
  }

  /** Request with identifier `id` to insert an element `elem` into the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to check whether an element `elem` is present
    * in the tree. The actor at reference `requester` should be notified when
    * this operation is completed.
    */
  case class GetAll(requestor: ActorRef, includeRemoved: Boolean)
  case class Collect(elem: Int, removed: Boolean)
  case object PrintElements

  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to remove the element `elem` from the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request to perform garbage collection*/
  case object GC
  case class GCNode(requestor: ActorRef)

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply
  
  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply

  case class OperationMissed(id: Int, size: Int, nr: Int) extends OperationReply

}


class BinaryTreeSet extends Actor with ActorLogging {
  import BinaryTreeSet._
  import BinaryTreeNode._

  def createRoot: ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  var root = createRoot
  var allElements = List[Collect]()

  // optional
  var pendingQueue = Queue.empty[Operation]

  // optional
  def receive = normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = {
    case i: Insert => {
      root ! i
    }
    case c: Contains => {
      root ! c
    }
    case r: Remove => {
      root ! r
    }
    case gc: GC.type => {
      val newRoot = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))
      root ! CopyTo(newRoot, self)
      log.info("received GC")
      context.become(garbageCollecting(newRoot), false)
    }
    case ga: GetAll => {
      root ! ga
    }
    case pe: PrintElements.type => {
      log.warning(s"all elements ${allElements.mkString(", ")}")
      allElements.foreach(cl => log.info(s"all elements: ${cl.elem}, ${cl.removed}"))
    }
    case cl: Collect => {
      allElements = cl :: allElements
    }
    case _ => {
      log.warning("default")
    }
  }

  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = {
      case op: Operation => pendingQueue= pendingQueue.enqueue(op)
      case df @ CopyFinished => {
        root ! PoisonPill
        root = newRoot
        newRoot ! GetAll
        log.warning(s"all elements ${allElements.mkString(",")}")
        pendingQueue.foreach(op => root ! op)
        context.become(normal)
        pendingQueue = Queue.empty[Operation]
      }
  }
}

object BinaryTreeNode {
  trait Position

  case object Left extends Position
  case object Right extends Position

  case class CopyTo(treeNode: ActorRef, accumulator: ActorRef)
  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode],  elem, initiallyRemoved)
}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor with ActorLogging {
  import BinaryTreeNode._
  import BinaryTreeSet._

  var subtrees = Map[Position, ActorRef]()
//  var left = Option[ActorRef]
//  var right = Option[ActorRef]
  var removed = initiallyRemoved

  // optional
  def receive = normal

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = {
    case op: Operation if (op.elem < elem && subtrees.contains(Left)) => subtrees(Left) ! op
    case op: Operation if (op.elem > elem && subtrees.contains(Right)) => subtrees(Right) ! op

    case i: Insert => {
      if (i.elem == elem) removed = false;
      else {
        val operation = if (i.elem < elem) Left else Right;
        subtrees += (operation -> context.actorOf(BinaryTreeNode.props(i.elem, initiallyRemoved = false), s"${i.elem}"))
      }
      i.requester ! BinaryTreeSet.OperationFinished(i.id)
    }
    case c: Contains => {
      c.requester ! BinaryTreeSet.ContainsResult(c.id, c.elem == elem && !removed)
    }
    case r: Remove => {
      if (r.elem == elem) removed = true
      r.requester ! BinaryTreeSet.OperationFinished(r.id)
    } case ct: CopyTo => {
      if (!removed) ct.treeNode ! Insert(self, elem, elem)
      //log.info(s"received CopyTO on $elem")
      subtrees.values.foreach(actor => actor ! CopyTo(ct.treeNode, ct.accumulator))
      context.become(copying(subtrees.values.toSet, ct.accumulator, removed))
    }
    case ga: GetAll => {
      ga.requestor ! Collect(elem, removed)
      subtrees.values.foreach(ac => ac ! ga)
    }
    case _ => {
      log.warning("default")
    }
  }


        // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], acc: ActorRef, insertConfirmed: Boolean): Receive = {
    if (expected.isEmpty && insertConfirmed) {
      context.parent ! CopyFinished
      acc ! Collect(elem, removed)
      context.become(normal)
      normal
    } else {
      case c: CopyFinished.type => {
        //log.warning(s"copying finished for $elem")
        context.become(copying(expected-sender, acc, insertConfirmed))
      }
      case of: OperationFinished => {
        if (of.id == elem) context.become(copying(expected, acc,true))
        else log.error("received unmatched copy OperationFinished")
      }
    }
  }
}
