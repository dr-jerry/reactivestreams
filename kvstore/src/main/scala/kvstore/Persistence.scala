package kvstore

import akka.actor.{Actor, Props}

import scala.util.Random
import java.util.concurrent.atomic.AtomicInteger

import kvstore.AckAble

object Persistence {
  case class Persisted(key: String, id: Long)
  case class Persist(key: String, valueOption: Option[String], id: Long) extends AckAble

  class PersistenceException extends Exception("Persistence failure")

  def props(flaky: Boolean): Props = Props(classOf[Persistence], flaky)
}

class Persistence(flaky: Boolean) extends Actor {
  import Persistence._

  def receive = {
    case Persist(key, _, id) =>
      if (!flaky || Random.nextBoolean()) sender ! Persisted(key, id)
      else throw new PersistenceException
  }

}
