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

  /** Request with identifier `id` to insert an element `elem` into the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to check whether an element `elem` is present
    * in the tree. The actor at reference `requester` should be notified when
    * this operation is completed.
    */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to remove the element `elem` from the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request to perform garbage collection*/
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply
  
  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply

}


class BinaryTreeSet extends Actor {
  import BinaryTreeSet._
  import BinaryTreeNode._

  def createRoot: ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  var root = createRoot

  // optional
  var pendingQueue = Queue.empty[Operation]

  // optional
  def receive = normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = {
    case Insert(req, id, elem) => root ! Insert(req, id, elem)
    case Contains(req, id, elem) => root ! Contains(req, id, elem)
    case Remove(req, id, elem) => root ! Remove(req, id, elem)
    case GC =>
      //println("GC op during normal")
      val newRoot = createRoot
      root ! CopyTo(newRoot)
      context become garbageCollecting(newRoot)
    case _ => ???
  }

  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  //TODO use pendingQueue for getting the message
  //TODO also handle CopyFinished here!
  def garbageCollecting(newRoot: ActorRef): Receive = {
    case GC => //do nothing
    case op: Operation =>
      pendingQueue = pendingQueue :+ op
    case CopyFinished =>
      //println(pendingQueue.size + " jobs are waiting")
      root ! PoisonPill
      pendingQueue foreach (op => newRoot ! op)
      pendingQueue = Queue.empty[Operation]
      root = newRoot
      context become normal
  }

}

object BinaryTreeNode {
  trait Position

  case object Left extends Position
  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)
  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode],  elem, initiallyRemoved)
}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor {
  import BinaryTreeNode._
  import BinaryTreeSet._

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  // optional
  def receive = normal

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = {
    case Contains(req, id, e) =>
      if (!removed && elem == e) { req ! ContainsResult(id, result = true) }
      else {
        if (elem > e && subtrees.contains(Right)) { subtrees(Right) ! Contains(req, id, e) }
        else if (elem < e && subtrees.contains(Left)) { subtrees(Left) ! Contains(req, id, e) }
        else { req ! ContainsResult(id, result = false) }
      }
    case Insert(req, id, e) =>
      if (elem == e) {
        removed = false // flagging the node to be alive if inserted again
        req ! OperationFinished(id)
      }
      else {
        if (elem > e) {
          insertToChild(Right, req, id, e)
        }
        else {
          insertToChild(Left, req, id, e)
        }
      }
    case Remove(req, id, e) =>
      if (elem == e) {
        removed = true
        req ! OperationFinished(id)
      }
      else if (elem > e && subtrees.contains(Right)) subtrees(Right) ! Remove(req, id, e)
      else if (elem < e && subtrees.contains(Left)) subtrees(Left) ! Remove(req, id, e)
      else req ! OperationFinished(id)
    case CopyTo(root) =>
      if (removed && subtrees.isEmpty) {
        context.parent ! CopyFinished
        context become normal
      }
      else {
        if (!removed) root ! Insert(self, 1, elem)
        val waitSet = subtrees.toVector.map({case (pos, ref) =>
          ref ! CopyTo(root)
          ref
        }).toSet
        context become copying(waitSet, insertConfirmed = removed)
      }
  }

  // optional
  def insertToChild(pos: Position, req: ActorRef, id: Int, e: Int): Unit = {
    if (subtrees.contains(pos)) subtrees(pos) ! Insert(req, id, e)
    else {
      subtrees = subtrees + (pos -> context.actorOf(BinaryTreeNode.props(e, initiallyRemoved = false)))
      req ! OperationFinished(id)
    }
  }

  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  //TODO define insertoperation and copyFinished here
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = {
    case OperationFinished(id) =>
      if (expected.isEmpty) {
        context.parent ! CopyFinished
        context become normal
      }
      else context become copying(expected, insertConfirmed = true)
    case CopyFinished =>
      val newSet = expected - sender()
      if (newSet.isEmpty && insertConfirmed) {
        context.parent ! CopyFinished
        context become normal
      }
      else context become copying(newSet, insertConfirmed)
    case _ => println("some undefined operation here")
  }


}
