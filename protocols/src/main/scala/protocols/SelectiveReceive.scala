package protocols


import akka.actor.typed
import akka.actor.typed.{ActorContext, Behavior, ExtensibleBehavior, Signal, Terminated}
import akka.actor.typed.scaladsl._

object SelectiveReceive {
    /**
      * @return A behavior that stashes incoming messages unless they are handled
      *         by the underlying `initialBehavior`
      * @param bufferSize Maximum number of messages to stash before throwing a `StashOverflowException`
      *                   Note that 0 is a valid size and means no buffering at all (ie all messages should
      *                   always be handled by the underlying behavior)
      * @param initialBehavior Behavior to decorate
      * @tparam T Type of messages
      *
      * Hint: Implement an [[ExtensibleBehavior]], use a [[StashBuffer]] and [[Behavior]] helpers such as `start`,
      * `validateAsInitial`, `interpretMessage`,`canonicalize` and `isUnhandled`.
      */
    def apply[T](bufferSize: Int, initialBehavior: Behavior[T]): Behavior[T]  = {
        println(s"applying on Stasher")
        new Stasher(initialBehavior, bufferSize, StashBuffer[T](bufferSize))
    }

//        import akka.actor.typed.Behavior.{validateAsInitial, start, interpretMessage, canonicalize, isUnhandled}
//
//        override def receive(ctx: ActorContext[T], msg: T): Behavior[T] = {
//
//        }
//
//        override def receiveSignal(ctx: ActorContext[T], msg: Signal): Behavior[T] = ???
//    }
}

class Stasher[T](current: Behavior[T], stashSize: Int, stash: StashBuffer[T]) extends ExtensibleBehavior[T] {
    import akka.actor.typed.Behavior.{validateAsInitial, start, interpretMessage, canonicalize, isUnhandled}
     def receive(ctx: ActorContext[T], msg: T): Behavior[T] = try {
        //println(s"stasher receive $msg")
        val started = validateAsInitial(start(current,ctx))
        val next = interpretMessage(started, ctx, msg)
        val canonicalNext = canonicalize(next, started, ctx)
        //println(s"canonicalNext = $canonicalNext, next: $next msg: $msg")
        if (Behavior.isUnhandled(next)) {
          //println(s"$next with $msg is unhandled stashsize is ${stash.size}")
          stash.stash(msg)
          //println(s"stashed $msg length = ${stash.size}")
          new Stasher[T](canonicalNext, stashSize, stash)
        } else {
            //println(s"handled and unstashing message is $msg ${stash.size}")
            stash.unstashAll(ctx.asScala, new Stasher[T](canonicalNext, stashSize, StashBuffer[T](stashSize)))
        }

     } catch {
         case x: Exception => {println(s"exception $x");throw x}
     }

    def receiveSignal(ctx: ActorContext[T], msg: Signal): Behavior[T] = {
        msg match {
            case t: Terminated => {
                print("terminated"); Behaviors.same
            }
            case _ => {
                print("not terminated but still bad"); Behavior.same
            }
        }
    }
}
