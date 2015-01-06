

import java.util.concurrent.TimeUnit
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSelection.toScala
import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.actorRef2Scala
import akka.pattern.ask
import akka.util.Timeout
import spray.httpx.marshalling.ToResponseMarshallable.isMarshallable
import spray.routing.Directive.pimpApply
import spray.routing.SimpleRoutingApp
import spray.routing.directives.ParamDefMagnet.apply
import akka.routing.RoundRobinRouter

case class resetCounts()
case class outputStats()
case class requestHomeLine()
case class reqUserTimeline()
case class print()
case class terminate()
case class getfollowedbyList()
case class mentiontimeline()
case class followedByUsers()

object restserver extends App with SimpleRoutingApp {

  var totalNoOfusers: Int = 100000
 // totalNoOfusers = args(0).toInt

  var monitor: ActorRef = null
  var client: ActorRef = null
  var initialWorker: ActorRef = null

  var tweetMap = new HashMap[Int, Tweets]()

  implicit val system = ActorSystem()
  val server = system.actorOf(Props(new server), "server")

  monitor = system.actorOf(Props(new Monitor), "monitor")
  server ! "init"

  implicit val timeout = Timeout(100000)

  startServer(interface = "localhost", port = 5150) {

    post {
      path("tweet") {
        parameter("userID", "text", "createdAt") { (userID, text, createdAt) =>
          server ! (userID.toInt, text, createdAt.toLong)
          complete {
            "Tweet Added  "
          }
        }
      }
    } ~
      get {
        path("followedByUsers") {
          parameter("userID") { userID =>
            val future = server.ask(userID.toInt, "followedByUsers")
            complete {

              val future1 = Await.result(future, timeout.duration).asInstanceOf[Future[ArrayBuffer[Int]]]
              val result = Await.result(future1, timeout.duration).asInstanceOf[ArrayBuffer[Int]]
              "To return mention tweets " + result
            }
          }
        }
      } ~
      get {
        path("mentiontweets") {
          parameter("userID") { userID =>
            val future = server.ask(userID.toInt, "mentiontweets")
            complete {

              val future1 = Await.result(future, timeout.duration).asInstanceOf[Future[ArrayBuffer[Tweets]]]
              val result = Await.result(future1, timeout.duration).asInstanceOf[ArrayBuffer[Tweets]]
              "To return mention tweets " + result
            }
          }
        }
      } ~
      get {
        path("hometweets") {
          parameter("userID") { userID =>
            val future = server.ask(userID.toInt, "hometweets")
            complete {
              val future1 = Await.result(future, timeout.duration).asInstanceOf[Future[ArrayBuffer[Tweets]]]
              val result = Await.result(future1, timeout.duration).asInstanceOf[ArrayBuffer[Tweets]]
              "To return home tweets " + result
            }
          }
        }
      } ~
      get {
        path("usertweets") {
          parameter("userID") { userID =>

            val future = server.ask(userID.toInt, "usertweets")
            complete {
              val future1 = Await.result(future, timeout.duration).asInstanceOf[Future[ArrayBuffer[Tweets]]]
              val result = Await.result(future1, timeout.duration).asInstanceOf[ArrayBuffer[Tweets]]
              "To return user tweets " + result
            }
          }
        }
      }

  }

  class Worker extends Actor {
    
    def receive = {

      case (count: Int, exceptionalUserFollowingCount:Int,  avg2UserFollowingCount:Int, avgUserFollowingCount:Int, mostUsersFollowingCount:Int, user:Int) =>
        
        var followedByIDList = new ArrayBuffer[Int]()
                  // add largeFC users to followed by list
          if (count <= exceptionalUserFollowingCount) {
            var exceptionalFC = (0.03 * totalNoOfusers).toInt
            for (i <- 1 to exceptionalFC) {
              var fUSer = util.Random.nextInt(totalNoOfusers - 1) + 1
              while (followedByIDList.contains(fUSer) || fUSer == user) {
                fUSer = util.Random.nextInt(totalNoOfusers - 1) + 1
              }
              followedByIDList.append(fUSer)
            }
          } else if (count < exceptionalUserFollowingCount + avg2UserFollowingCount) {
            var avg2FC = util.Random.nextInt(1000) + 500

            for (i <- 1 to avg2FC) {
              var fUSer = util.Random.nextInt(totalNoOfusers - 1) + 1
              while (followedByIDList.contains(fUSer) || fUSer == user) {
                fUSer = util.Random.nextInt(totalNoOfusers - 1) + 1
              }
              followedByIDList.append(fUSer)
            }
          } else if (count <= avgUserFollowingCount + exceptionalUserFollowingCount + avg2UserFollowingCount) {
            var avgFC = util.Random.nextInt(450) + 50
            for (i <- 1 to avgFC) {
              var fUSer = util.Random.nextInt(totalNoOfusers - 1) + 1
              while (followedByIDList.contains(fUSer) || fUSer == user) {
                fUSer = util.Random.nextInt(totalNoOfusers - 1) + 1
              }
              followedByIDList.append(fUSer)
            }
          } else if (count < avgUserFollowingCount + exceptionalUserFollowingCount + avg2UserFollowingCount + mostUsersFollowingCount) {
            var mostFC = util.Random.nextInt(48) + 2
            for (i <- 1 to mostFC) {
              var fUSer = util.Random.nextInt(totalNoOfusers - 1) + 1
              while (followedByIDList.contains(fUSer) || fUSer == user) {
                fUSer = util.Random.nextInt(totalNoOfusers - 1) + 1
              }
              followedByIDList.append(fUSer)
            }
          }

          var userinfo = new userinfo()
          userinfo.id = user
          userinfo.followedbyList = followedByIDList
         userinfo.homeTweets = new ArrayBuffer[Tweets]()
          userinfo.userTweets = new ArrayBuffer[Tweets]()
          userinfo.mentionTweets = new ArrayBuffer[Tweets]()
          //println("Initialize " + user)
          context.actorSelection("/user/server/" + user) ! (userinfo)
        
    }

  }

  class server extends Actor {

    var tweetId = 0

    def generateTweetId(): Int = {
      tweetId = tweetId + 1
      return (tweetId)
    }

    // send incoming traffic to monitor and to route it to handler
    def receive = {

      case (init: String) =>
        println("Initializing Server")
        val workerRouter = system.actorOf(Props[Worker].withRouter(RoundRobinRouter(100)), name = "workerRouter")

        // FOLLOWING STATISTICS
        // 50 % follow 2 or more 
        var mostUsersFollowingCount = (totalNoOfusers * 0.5).toInt
        // 10 % follow 50 or more
        var avgUserFollowingCount = (totalNoOfusers * 0.1).toInt
        // 1% follow more than 500 people 
        var avg2UserFollowingCount = (totalNoOfusers * 0.01).toInt
        // 2 accounts follow more than 524,000
        var exceptionalUserFollowingCount = 2

        var userSet = new HashSet[Int]()
        for (i <- 1 to totalNoOfusers) {
          userSet.add(i)
        }

        for (i <- 1 to totalNoOfusers) {
          val handler = context.actorOf(Props(new Handle), i.toString)
        }
        // to update followed by list of users
        var count = 0
        for ((user) <- userSet) {
          count = count + 1
          workerRouter ! (count, exceptionalUserFollowingCount,  avg2UserFollowingCount, avgUserFollowingCount, mostUsersFollowingCount, user)
        }
        println("Initialization Done")

      // GET REQUESTS
      case (userId: Int, action: String) =>

        if (action == "usertweets") {
          val future = context.actorSelection("/user/server/" + userId.toString) ? reqUserTimeline()
          sender ! future
          monitor ! reqUserTimeline()
        } else if (action == "hometweets") {
          val future1 = context.actorSelection("/user/server/" + userId.toString) ? requestHomeLine()
          sender ! future1
          monitor ! requestHomeLine()
        } else if (action == "followedByUsers") {
          val future2 = context.actorSelection("/user/server/" + userId.toString) ? followedByUsers()
          sender ! future2
        } else if (action == "mentiontweets") {
          val future2 = context.actorSelection("/user/server/" + userId.toString) ? mentiontimeline()
          sender ! future2
          monitor ! mentiontimeline()
        }

      // POST REQUESTS
      case (userId: Int, tweetText: String, createdAt: Long) =>
        // received tweet
        var tweet = new Tweets()
        tweet.Id = generateTweetId()
        tweet.text = tweetText
        tweet.sourceUser = userId
        tweet.createdAt = createdAt
        tweetMap.put(tweet.Id, tweet)
        // println(tweetText)
        if (tweetText.startsWith("@")) {
          // println("Recieved mention tweet")
          var idstring = ""
          var i = 1
          while (!Character.isSpace(tweetText.charAt(i))) {
            idstring = idstring + tweetText.charAt(i)
            i = i + 1
          }
          // id string'
          context.actorSelection("/user/server/" + idstring) ! (tweet, idstring)
        } else {
          // println("Recieve simple tweet")
          // it is a simple tweet without any mention
          // println(" TweetId " + tweet.Id +  " Tweet User" + tweet.sourceUser  + " content " +  tweet.text + " time " + tweet.createdAt )
          context.actorSelection("/user/server/" + tweet.sourceUser.toString) ! (tweet)
        }
        monitor ! "tweet"

    }
  }

  class Handle extends Actor {

    var id = 0
    var uInfo = new userinfo()

    def receive = {

      case (userInfo: userinfo) =>
        this.uInfo = userInfo
        this.id = userInfo.id

      //  println("Init user " + uInfo.id + " followCount " + uInfo.followedbyList.length  + 
      //		" homeTw " + uInfo.homeTweets + " userTw " + uInfo.userTweets )

      case requestHomeLine() =>
        // sending home tweets 

        var recentTweets = ArrayBuffer[Tweets]()
        var size = uInfo.homeTweets.length
        var count = 0
        if (size > 20) {
          while (count < 20) {
            recentTweets.append(uInfo.homeTweets.apply(size - 1))
            size = size - 1
            count = count + 1
          }
        } else {
          recentTweets = uInfo.homeTweets
        }
        sender ! (recentTweets)
        monitor ! outputStats()

      case mentiontimeline() =>
        // sending mention tweets 
        var recentTweets = ArrayBuffer[Tweets]()
        var size = uInfo.mentionTweets.length
        var count = 0
        if (size > 20) {
          while (count < 20) {
            recentTweets.append(uInfo.mentionTweets.apply(size - 1))
            size = size - 1
            count = count + 1
          }
        } else {
          recentTweets = uInfo.mentionTweets
        }
        sender ! (recentTweets)
        monitor ! outputStats()

      case followedByUsers() =>

        sender ! uInfo.followedbyList

      case reqUserTimeline() =>
        // sending user tweets 
        var recentTweets = ArrayBuffer[Tweets]()
        var size = uInfo.userTweets.length
        var count = 0

        if (size > 20) {
          while (count < 20) {
            recentTweets.append(uInfo.userTweets.apply(size - 1))
            size = size - 1
            count = count + 1
          }
        } else {
          recentTweets = uInfo.userTweets
        }
        sender ! (recentTweets)
        monitor ! outputStats()

      case (tweet: Tweets) =>
        //  println("Adding tweet" + tweet.Id + " to timeline User " +  tweet.sourceUser + " followedbyList " + uInfo.followedbyList.length  )
        uInfo.homeTweets.append(tweet)
        uInfo.userTweets.append(tweet)

        // add this tweet to all the users following this user

        for (followedByUserId <- uInfo.followedbyList) {
          context.actorSelection("/user/server/" + followedByUserId.toString) ! (followedByUserId, tweet)
        }
        monitor ! outputStats()

      case (user: Int, tweet: Tweets) =>
        //this is tweeted by other,  add tweet to my home timeline

        //println("I am " + id + " tweeted by User " +  tweet.sourceUser + " TID " + tweet.Id )

        uInfo.homeTweets.append(tweet)

      case ("addtousertweets", tweet: Tweets) =>
        uInfo.userTweets.append(tweet)

      case (tweet: Tweets, mentionUserName: String) =>

        /*  source user -  add to its user timeline
           			     -	add to its home timeline if they both follow each other.

           mention user - add to its mention feeds
          			 	- add to its home timeline if they both follow each other.
           followers-  add to hometimeline - if they follow both source user & mention user.
          */

        // add to mentionUserName mention feeds
        uInfo.mentionTweets append (tweet)
        // add to user feeds of source user
        context.actorSelection("/user/server/" + tweet.sourceUser) ! ("addtousertweets", tweet)

        // ask source user for its follower list  - to check common
        implicit val timeout = Timeout(100000)

        val future = context.actorSelection("/user/server/" + tweet.sourceUser).ask(getfollowedbyList())
        val srcUserFollowedbylist = Await.result(future, timeout.duration).asInstanceOf[ArrayBuffer[Int]]

        // println("followed by list " + srcUserFollowedbylist)

        // check if  for common followers & and if they follow each other
        if (uInfo.followedbyList != null && srcUserFollowedbylist != null &&
          uInfo.followedbyList.contains(tweet.sourceUser.toInt) && srcUserFollowedbylist.contains(mentionUserName.toInt)) {
          // add to thier home timeline
          uInfo.homeTweets.append(tweet)
          context.actorSelection("/user/server/" + tweet.sourceUser) ! (tweet.sourceUser, tweet)
        }
        // find out common followers
        for (follower <- uInfo.followedbyList) {
          if (srcUserFollowedbylist.contains(follower)) {
            context.actorSelection("/user/server/" + follower) ! (follower, tweet)
          }
        }
        monitor ! outputStats()

      case getfollowedbyList() =>
        // send followed by list back to sender - for mention tweets
        sender ! uInfo.followedbyList
    }
  }

  class Monitor extends Actor {
    var requestsHome = 0
    var requestsUser = 0
    var mentionfeed = 0
    var reqTweet = 0
    var outputstats = 0
    import context.dispatcher
    var alarm: akka.actor.Cancellable = context.system.scheduler.schedule(Duration.create(10000, TimeUnit.MILLISECONDS),
      Duration.create(1000, TimeUnit.MILLISECONDS), self, resetCounts())

    // var alarm4: akka.actor.Cancellable = context.system.scheduler.scheduleOnce(Duration.create(180000, TimeUnit.MILLISECONDS), self, terminate())
    def receive = {
      // Incoming traffic
      case requestHomeLine() =>
        requestsHome = requestsHome + 1

      case reqUserTimeline() =>
        requestsUser = requestsUser + 1
      case mentiontimeline() =>
        mentionfeed = mentionfeed + 1

      case (tweet: String) =>
        reqTweet = reqTweet + 1

      case resetCounts() =>
        println("outputstats " + outputstats + "\t" + " mentionTimelineReq " + mentionfeed + "\t" + "requestsHome " + requestsHome + "\t" + " requestsUser "
          + requestsUser + "\t" + " reqTweet " + reqTweet + "\t" + " input " + (requestsHome + requestsUser + reqTweet + mentionfeed))
        requestsHome = 0
        requestsUser = 0
        reqTweet = 0
        outputstats = 0
        mentionfeed = 0

      case outputStats() =>
        outputstats = outputstats + 1

      case terminate() =>
        alarm.cancel
      // println("Terminate")
      //  for (i <- 1 to totalNoOfusers) {
      //    context.actorSelection("/user/server/" + i.toString) ! "print"
      //    }

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

class userinfo {

  var id = 0

  var followingList: ArrayBuffer[Int] = null

  var followedbyList: ArrayBuffer[Int] = null

  // for home timeline - all tweets
  var homeTweets: ArrayBuffer[Tweets] = null

  // for mention timeline
  var mentionTweets: ArrayBuffer[Tweets] = null

  // for user timeline - only my tweets
  var userTweets: ArrayBuffer[Tweets] = null

  var reTweets: ArrayBuffer[Tweets] = null

  var dmSent: ArrayBuffer[Tweets] = null

  var dmRecieved: ArrayBuffer[Tweets] = null

}