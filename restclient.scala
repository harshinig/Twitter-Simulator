
import java.util.concurrent.TimeUnit

import scala.collection.mutable.HashSet
import scala.concurrent.duration.Duration

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSelection.toScala
import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.actorRef2Scala
import spray.client.pipelining.Get
import spray.client.pipelining.Post
import spray.client.pipelining.sendReceive
import spray.client.pipelining.sendReceive$default$3

case class init(userId: Int, type1: Boolean, type2: Boolean, type3: Boolean, type4: Boolean)
case class doSomething(action: String)
case class setScheduler()
case class requestHomeLine(userId: Int)
case class reqUserTimeline(userId: Int)
case class outTraffic()
case class resetcount()
case class terminate()

object restclient {

  var startId:Int = 1
  var endId:Int = 100000
  var noOfusers = endId - startId + 1

  var countOfRequests = 0

  var monitor: ActorRef = null

  var serverIp = "127.0.0.1"

  def main(args: Array[String]): Unit = {
    //   noOfusers = args(0).toInt
    println("Users " + noOfusers)

    //  serverIp = args(1).toString()

    val system = ActorSystem()

    // TRAFFIC STATISTICS
    // top 15 % of users generate most of the traffic
    val tweetsMostUser = (.15 * noOfusers).toInt

    val moderateTweetUSers = (.20 * noOfusers).toInt

    val lazyUsers = (.20 * noOfusers).toInt

    println("tweetsMostUser " + tweetsMostUser + " moderateTweetUSers " + moderateTweetUSers + " lazyUsers " + lazyUsers)

    var userSet = new HashSet[Int]()

    // to set the traffic created by users
    for (i <- startId to endId) {
      val user = system.actorOf(Props(new User), i.toString)
      userSet.add(i)
    }
    // to update traffic statistics
    var count1 = 0
    for ((user) <- userSet) {
      count1 = count1 + 1
      if (count1 <= tweetsMostUser) {
        system.actorSelection("/user/" + user) ! init(user, true, false, false, false)
      } else if (count1 <= moderateTweetUSers + tweetsMostUser) {
        system.actorSelection("/user/" + user) ! init(user, false, true, false, false)
      } else if (count1 <= lazyUsers + moderateTweetUSers + tweetsMostUser) {
        system.actorSelection("/user/" + user) ! init(user, false, false, true, false)
      } else {
        system.actorSelection("/user/" + user) ! init(user, false, false, false, true)
      }

    }

    monitor = system.actorOf(Props(new Monitor), "monitor")
    for (uid <- startId to endId) {
      system.actorSelection("/user/" + uid) ! setScheduler()
    }
  }

  class User extends Actor {

    var alarm: akka.actor.Cancellable = null
    var alarm1: akka.actor.Cancellable = null
    var alarm2: akka.actor.Cancellable = null
    var alarm3: akka.actor.Cancellable = null

    implicit val system = context.system
    import system.dispatcher
    val pipeline = sendReceive

    var userId = 0
    var counter = 0

    // generates most of the traffic
    var type1User = false
    // moderately tweets
    var type2User = false
    // no tweets, only sees hometimelne
    var type3User = false
    // no activity 
    var type4User = false

    // generate twitter content  
    def generateContent(): String = {

      var size = largeBodyOfText.length()

      var startIndex = util.Random.nextInt(size) + 1
      var endIndex = util.Random.nextInt(130) + startIndex +10
      if (endIndex > size) {
        endIndex = size
      }
      //  println("start " + startIndex + " end " + endIndex)
      var text = largeBodyOfText.substring(startIndex, endIndex)
      return text
    }

    def receive = {

      case init(id: Int, type1: Boolean, type2: Boolean, type3: Boolean, type4: Boolean) =>
        userId = id
        type1User = type1
        type2User = type2
        type3User = type3
        type4User = type4

      case setScheduler() =>
        // set scheduler for this user   
        if (type1User) {
          //    println("type1")
          var action: String = ""

          alarm = context.system.scheduler.schedule(Duration.create(10000, TimeUnit.MILLISECONDS),
            Duration.create(30000, TimeUnit.MILLISECONDS), self, doSomething("tweetMessage"))
          alarm1 = context.system.scheduler.schedule(Duration.create(10000, TimeUnit.MILLISECONDS),
            Duration.create(100000, TimeUnit.MILLISECONDS), self, doSomething("getHomeTimeline"))
          alarm2 = context.system.scheduler.schedule(Duration.create(10000, TimeUnit.MILLISECONDS),
            Duration.create(100000, TimeUnit.MILLISECONDS), self, doSomething("getUserTimeline"))
             alarm3 = context.system.scheduler.schedule(Duration.create(10000, TimeUnit.MILLISECONDS),
            Duration.create(100000, TimeUnit.MILLISECONDS), self, doSomething("getMentionFeed"))

        } else if (type2User) {
          //  println("type2")
          alarm = context.system.scheduler.schedule(Duration.create(30000, TimeUnit.MILLISECONDS),
            Duration.create(60000, TimeUnit.MILLISECONDS), self, doSomething("tweetMessage"))
             alarm3 = context.system.scheduler.schedule(Duration.create(10000, TimeUnit.MILLISECONDS),
            Duration.create(100000, TimeUnit.MILLISECONDS), self, doSomething("getMentionFeed"))
        } else if (type3User) {
          //  println("type3")
          alarm = context.system.scheduler.schedule(Duration.create(40000, TimeUnit.MILLISECONDS),
            Duration.create(60000, TimeUnit.MILLISECONDS), self, doSomething("getHomeTimeline"))
        }

      case doSomething(action: String) =>
        action match {
          case "tweetMessage" =>

            var text = generateContent()

            // mention tweet 
            if (1 == util.Random.nextInt(2)) {
              val mentionId = util.Random.nextInt(noOfusers)
              text = "@" + mentionId + "+" + text
            }

            val tweetcreatedAt = System.currentTimeMillis
            // send tweet to server
            monitor ! outTraffic()
            val result = pipeline(Post("http://localhost:5150/tweet?userID=" + userId +
              "&text=" + text + "&createdAt=" + tweetcreatedAt))

          case "getMentionFeed" =>
            monitor ! outTraffic()
            val result = pipeline(Get("http://localhost:5150/mentiontweets?userID=" + userId))

          case "getHomeTimeline" =>
            monitor ! outTraffic()
            val result = pipeline(Get("http://localhost:5150/hometweets?userID=" + userId))

          case "getUserTimeline" =>
            monitor ! outTraffic()
            val result = pipeline(Get("http://localhost:5150/usertweets?userID=" + userId))
        }
    }
  }

  var largeBodyOfText = "microbloggingserviceaspartofarecruitingtalkatUCBerkeleyOneinterestingstatinvolvesthenumberofAPIcallsthatthesit" +
    "eisseeing:KrikoriansaysthatTwitterisseeing6billionAPIcallsperdayor70,000persecondThatsmorethandoubletheamount(billionAPIcalls" +
    "aahjklksaysthatTwitterisseeing6billionAPIcallsperdayor70,000persecondThatsmorethandoublklksaysthatTwitterisseeing6b" + 
    "microbloggingserviceaspartofarecruitingtalkatUCBerkeleyOneinterestingstatinvolvesthenumberofAPIcallsthatthesit"+ 
     "eisseeing:KrikoriansaysthatTwitterisseeing6billionAPIcallsperdayor70,000persecondThatsmorethandoubletheamount(billionAPIcalls" 


  class Monitor extends Actor {

    var outgoingTraffic = 0
    import context.dispatcher
    var alarm: akka.actor.Cancellable = context.system.scheduler.schedule(Duration.create(10000, TimeUnit.MILLISECONDS),
      Duration.create(10000, TimeUnit.MILLISECONDS), self, resetcount())

    var alarm2: akka.actor.Cancellable = context.system.scheduler.scheduleOnce(Duration.create(180000, TimeUnit.MILLISECONDS), self, terminate())

    def receive = {
      // Incoming traffic
      case outTraffic() =>
        outgoingTraffic = outgoingTraffic + 1

      case resetcount() =>
        println("outgoingTraffic " + outgoingTraffic)
        outgoingTraffic = 0
      case terminate() =>
        alarm.cancel
        System.exit(0)

    }
  }

}

class Tweets extends Serializable {

  // 
  var Id: Int = 0
  // time
  var createdAt: Long = 0
  var sourceUser = 0
  var text: String = ""

}

