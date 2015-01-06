Twitter-Simulator
=================
In this project we simulated the Twitter search engine with akka actors and spray can using a Client/Server model. The clients send HTTP requests which are received and processed by the Server and response to the client is in JSON format.We implemented 4 functionalities namely
Tweet-to post tweets
GetUserTimeLinetweets-To read tweets posted by us
GetHomeTimeLineTweets-To read tweets posted by people we are following
GetMentionFeed-To read tweets in which we are being mentioned.

Statistical Distribution provided on the web has been used for modelling the behavior of the above mentioned functionalities to make simulation close to real world system.  
Record for maximum tweets/sec=143199
Average number of tweets/sec=5700


1.The performance varies as number of users increase. Beyond 700000, the processing capability reduces.
2.The number of requests processed are close to actual twitter capability which is around 5700/sec on average.
