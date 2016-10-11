from pyalgotrade import strategy
from pyalgotrade.bitstamp import barfeed
from pyalgotrade.bitstamp import broker
from pyalgotrade.twitter import feed as twitterfeed


class Strategy(strategy.BaseStrategy):
    def __init__(self, feed, brk, twitterFeed):
        strategy.BaseStrategy.__init__(self, feed, brk)
        self.__instrument = "BTC"

        # Subscribe to Twitter events.
        twitterFeed.subscribe(self.__onTweet)

    def __onTweet(self, data):
        # Refer to https://dev.twitter.com/docs/streaming-apis/messages#Public_stream_messages for
        # the information available in data.
        try:
            self.info("Twitter: %s" % (data["text"]))
        except KeyError:
            pass

    def onBars(self, bars):
        bar = bars[self.__instrument]
        self.info("Price: %s. Volume: %s." % (bar.getClose(), bar.getVolume()))


def main():
    # Go to http://dev.twitter.com and create an app.
    # The consumer key and secret will be generated for you after that.    
    consumer_key = "5kCakPuUeVnVMmng64s4UNk3c"
    consumer_secret = "QdYNLpoG2LGSNuX4ySsaIUPSDpivwjDsRTthBwUvkbo3uifq57"

    # After the step above, you will be redirected to your app's page.
    # Create an access token under the the "Your access token" section
    access_token = "2645896470-NVlAC5QkhZWizYX4brcnWYpM4QRWdVUBANWrjRk" 
    access_token_secret = "PKD0Se31hQVuw5BNCGGCUAaJ3LHfIJSViPUPBjL2UCfwz" 

    # Create a twitter feed to track BitCoin related events.
    track = ["bitcoin", "btc", "mtgox", "bitstamp", "xapo"]
    follow = []
    languages = ["en"]
    twitterFeed = twitterfeed.TwitterFeed(consumer_key, consumer_secret, access_token, access_token_secret, track, follow, languages)

    barFeed = barfeed.LiveTradeFeed()
    brk = broker.PaperTradingBroker(1000, barFeed)
    strat = Strategy(barFeed, brk, twitterFeed)

    # It is VERY important to add twitterFeed to the event dispatch loop before running the strategy.
    strat.getDispatcher().addSubject(twitterFeed)

    strat.run()

if __name__ == "__main__":
    main()