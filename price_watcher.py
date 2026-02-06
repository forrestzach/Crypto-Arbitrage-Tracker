from ast import Or
import sys
import asyncio
from websockets import connect
import websockets
import requests
import json
from decimal import Decimal
import nest_asyncio
import time

from order_book import OrderBook


class EchoWebsocket:
    def __await__(self):
        # see: http://stackoverflow.com/a/33420721/1113207
        return self._async_init().__await__()

    async def _async_init(self):
        self._conn = connect("wss://echo.websocket.org")
        self.websocket = await self._conn.__aenter__()
        return self

    async def close(self):
        await self._conn.__aexit__(*sys.exc_info())

    async def send(self, message):
        await self.websocket.send(message)

    async def receive(self):
        return await self.websocket.recv()

class dataWebsocket:
    id = 34
    def __init__(self, symbol):
        self.symbolPair = symbol
        self.tickerName = symbol[0] + symbol[1]

    async def start(self,uri):
        print("start called for " + str(self.symbolPair))
        self.ob = OrderBook()
        # self.ob.bids = {Decimal(price): size for price, size, _ in data['bids']}
        # self.ob.asks = {Decimal(price): size for price, size, _ in data['asks']}

        # self.symbol = uri[uri.rindex('/')+1:]
        self.websocket = await connect(uri,max_size=None)
        self.counter = 0
        print("socket started")
        async for message in self.websocket:
            # print(message)
            if self.symbolPair == ['ZEC', 'BCH']:
                print(message)


            parsedMessage = json.loads(message)

            for points in parsedMessage.get('events'):
                # print(f"REMAINING: {points.get('remaining')} at price: {points.get('price')}")
                # print(points)
                if points.get('side') == "bid":
                    if Decimal(points.get('remaining')) == 0.0:
                        del self.ob.bids[Decimal(points.get('price'))]
                        # if self.symbol != "ETHUSD": 
                            # print(f"Deleted {self.symbol} BID at {Decimal(points.get('price'))}")
                    else:
                        self.ob.bids[Decimal(points.get('price'))] = points.get('remaining')
                        # print(f"Changed BID {points.get('price')} to {points.get('remaining')}")
                elif points.get('side') == "ask":
                    if Decimal(points.get('remaining')) == 0.0:
                        del self.ob.asks[Decimal(points.get('price'))]
                        # print(f"Deleted ASK at {Decimal(points.get('price'))}")
                    else:
                        self.ob.asks[Decimal(points.get('price'))] = points.get('remaining')
                        # print(f"Changed ASK {points.get('price')} to {points.get('remaining')}")

    async def printTop(self):
        #EXAMPLE of data access:
        # Data is accessible by .index(), which returns a tuple of (price, size) at that level in the book
        # price, size = ob.bids.index(0)
        # print(f"Best bid price: {price} size: {size}")

        # price, size = ob.asks.index(0)
        # print(f"Best ask price: {price} size: {size}")

        # print(f"The spread is {ob.asks.index(0)[0] - ob.bids.index(0)[0]}\n\n")

        # Have function wait 20 seconds to populate orderbook
        await asyncio.sleep(20)
        while (True):
            
            print(f"Best {self.symbolPair} Ask: {self.ob.asks.index(0)[0]} at size: {self.ob.asks.index(0)[1]}")
            print(f"Best {self.symbolPair} Bid: {self.ob.bids.index(0)[0]} at size: {self.ob.bids.index(0)[1]}")
            # print(f"Size of one ask entry in bytes: {sys.getsizeof(self.ob.asks.index(0))} bid order book in bytes: {sys.getsizeof(self.ob.bids)}")
            # print(f"Length of order book {len(self.ob)}")
            
            # Delay 3 seconds so as to not spam
            await asyncio.sleep(3.0)

    async def __aiter__(self):
        return self

    async def getTopAsk(self):
        try:
            if self.ob.asks.index(0) is None:
                # print(f"{self.symbolPair} cannot return top ask")
                return (Decimal('0.0'), '0.0')
            else:
                return self.ob.asks.index(0)
        except IndexError:
            # print("Index Error, for some reason")
            return (Decimal('0.0'), '0.0')
        except Exception as e:
            print(f"Exception in getTopAsk(): {e}")
    
    async def getTopBid(self):
        try:
            if self.ob.bids.index(0) is None:
                # print(f"{self.symbolPair} cannot return top bid")
                return (Decimal('0.0'), '0.0')
            else:
                return self.ob.bids.index(0)
        except IndexError:
            # print("Index Error, for some reason")
            return (Decimal('0.0'), '0.0')
        except Exception as e:
            print(f"Exception in getTopBid(): {e}")
    
    async def getPair(self):
        return self.symbolPair
    
    async def getNameAsync(self):
        return self.tickerName
    
    def getName(self):
        return self.tickerName

#If the classes are declared as parts of the list tickerWS, is that where I can access them?
class Watcher:
    def __init__(self, tickerWS, outputName):
        self.name = "I am the Watcher"
        self.marketBook = {}
        self.outputName = outputName

        # Build out marketBook, the dict of dicts that has the bid/ask price/volume for all the tickers
        for tick in tickerWS:
            tickerName = tick.getName()
            # print(f"tickerName got value: {tickerName}")
            value = {
                "topAskPrice": Decimal("0.0"),
                "topAskSize": Decimal("0.0"),
                "topBidPrice": Decimal("0.0"),
                "topBidSize": Decimal("0.0")
            }
            self.marketBook[tickerName] = value

        print(self.marketBook)

    async def start(self, tickerWS):
        await asyncio.sleep(2.5)
        while True:
            for ticks in tickerWS:
                try:
                    topAsk = await ticks.getTopAsk()
                    topBid = await ticks.getTopBid()
                    name = await ticks.getNameAsync()

                    # Possible performance improvement opportunity, not sure the best way to reassign all of these values
                    self.marketBook[name]["topAskPrice"] = topAsk[0]
                    self.marketBook[name]["topAskSize"] = Decimal(topAsk[1])
                    self.marketBook[name]["topBidPrice"] = topBid[0]
                    self.marketBook[name]["topBidSize"] = Decimal(topBid[1])

                except Exception as e:
                    print(f"Exception in trying to update {ticks}: {e}")
            await asyncio.sleep(1.0)

    async def printMarketBook(self):
        await asyncio.sleep(6.0)
        while True:
            for markets in self.marketBook.keys():
                print(f"{markets}: {self.marketBook[markets]}")

            await asyncio.sleep(3.0)

    def matcher(self, BBS_combos, BSS_combos):
        # await asyncio.sleep(10.0)
        time.sleep(9)
        fileTimeThreshold = time.time()+60
        outputQueue = []
        startingValue = Decimal('100.0')
        takerFee = Decimal('0.996')
        # takerFee = Decimal('1.0')
        print(f"Matcher starting {startingValue}")
        outputQueue.append(f"starting value: {startingValue} takerFee: {takerFee}")
        while True:
            for path in BBS_combos:
                result = Decimal('100.0')
                stepNumber = 0
                brokenPath = False
                enoughVolume = True
                maxVolume = Decimal('100.0')
                for step in path:
                    try:    
                        # print(step)
                        if stepNumber < 2:
                            # if result > self.marketBook[step]["topAskPrice"] * self.marketBook[step]["topAskSize"]:
                            #     enoughVolume = False
                            result = result / self.marketBook[step]["topAskPrice"] * takerFee
                            maxVolume = maxVolume / self.marketBook[step]["topAskPrice"] * takerFee
                            volumeRatio = self.marketBook[step]["topAskSize"] / maxVolume
                            if volumeRatio < Decimal('1.0'):
                                maxVolume = maxVolume * volumeRatio
                        elif stepNumber == 2:
                            # if result > self.marketBook[step]["topBidPrice"] * self.marketBook[step]["topBidSize"]:
                            #     enoughVolume = False
                            result = result * self.marketBook[step]["topBidPrice"] * takerFee
                            maxVolume = maxVolume * self.marketBook[step]["topBidPrice"] * takerFee
                            volumeRatio = self.marketBook[step]["topBidSize"] / maxVolume
                            if volumeRatio < Decimal('1.0'):
                                maxVolume = maxVolume * volumeRatio
                        else:
                            print("something went wrong with  step counter")
                        stepNumber += 1
                    except Exception as e:
                        # print(f"Exception in trying to match {path}: {e}")
                        brokenPath = True
                if not brokenPath:
                    if result > startingValue:
                        if result > maxVolume:
                            enoughVolume = False
                        # print(f"SUCCESS: on BBS path {path} converted {startingValue} into {round(result,3)}")
                        # print(f"{round(time.time(),2)}:SUCCESS:BBS:{startingValue}->{round(result,3)}:{path[0]}-{self.marketBook[path[0]]},{path[1]}-{self.marketBook[path[1]]},{path[2]}-{self.marketBook[path[2]]}")
                        outputQueue.append(f"{round(time.time(),2):.2f}|SUCCESS|BBS|{startingValue}->{round(result,3)}|MAXVOLUME={maxVolume}|VOLUME={enoughVolume}|{path[0]}-{self.marketBook[path[0]]},{path[1]}-{self.marketBook[path[1]]},{path[2]}-{self.marketBook[path[2]]}\n")
                        # print(f"{round(time.time(),2)}:SUCCESS:BBS:{startingValue}->{round(result,3)}:VOLUME={enoughVolume}:{path[0]}-{self.marketBook[path[0]]},{path[1]}-{self.marketBook[path[1]]},{path[2]}-{self.marketBook[path[2]]}\n")
                    else:
                        # print(f"FAIL   : on BBS path {path} converted {startingValue} into {result}")
                        pass

            for path in BSS_combos:
                result = Decimal('100.0')
                stepNumber = 0
                brokenPath = False
                enoughVolume = True
                maxVolume = Decimal('100.0')
                for step in path:
                    try:
                        if stepNumber == 0:
                            # if result > self.marketBook[step]["topAskPrice"] * self.marketBook[step]["topAskSize"]:
                            #     enoughVolume = False
                            result = result / self.marketBook[step]["topAskPrice"] * takerFee
                            maxVolume = maxVolume / self.marketBook[step]["topAskPrice"] * takerFee
                            volumeRatio = self.marketBook[step]["topAskSize"] / maxVolume
                            if volumeRatio < Decimal('1.0'):
                                maxVolume = maxVolume * volumeRatio
                        elif stepNumber > 0:
                            # if result > self.marketBook[step]["topBidPrice"] * self.marketBook[step]["topBidSize"]:
                            #     enoughVolume = False
                            result = result * self.marketBook[step]["topBidPrice"] * takerFee
                            maxVolume = maxVolume * self.marketBook[step]["topBidPrice"] * takerFee
                            volumeRatio = self.marketBook[step]["topBidSize"] / maxVolume
                            if volumeRatio < Decimal('1.0'):
                                maxVolume = maxVolume * volumeRatio
                        else:
                            print("something went wrong with  step counter")
                        stepNumber += 1
                    except Exception as e:
                        # print(f"Exception in trying to match {path}: {e}")
                        brokenPath = True
                if not brokenPath:
                    if result > startingValue:
                        if result > maxVolume:
                            enoughVolume = False
                        # print(f"SUCCESS: on BSS path {path} converted {startingValue} into {round(result,3)}")
                        # print(f"{round(time.time(),2)}:SUCCESS:BSS:{startingValue}->{round(result,3)}:{path[0]}-{self.marketBook[path[0]]},{path[1]}-{self.marketBook[path[1]]},{path[2]}-{self.marketBook[path[2]]}")
                        outputQueue.append(f"{round(time.time(),2):.2f}|SUCCESS|BSS|{startingValue}->{round(result,3)}|MAXVOLUME={maxVolume}|VOLUME={enoughVolume}|{path[0]}-{self.marketBook[path[0]]},{path[1]}-{self.marketBook[path[1]]},{path[2]}-{self.marketBook[path[2]]}\n")
                        # print(f"{round(time.time(),2)}:SUCCESS:BSS:{startingValue}->{round(result,3)}:VOLUME={enoughVolume}:{path[0]}-{self.marketBook[path[0]]},{path[1]}-{self.marketBook[path[1]]},{path[2]}-{self.marketBook[path[2]]}\n")
                    else:
                        # print(f"FAIL   : on BSS path {path} converted {startingValue} into {result}")
                        pass

            # await asyncio.sleep(1.5)
            if fileTimeThreshold < time.time():
                fileTimeThreshold = time.time() + 60
                with open(self.outputName, 'a') as f_object:
                    f_object.write(f"{round(time.time(),2)}:ALIVE\n")
                    for item in outputQueue:
                        f_object.write(item)
                    outputQueue  = []
                    f_object.close()

            time.sleep(0.75)

def getSymbols():
    print("getsymbols run")
    try: 
        response = requests.get("https://api.gemini.com/v1/symbols", timeout=4)

    except:
        print("failed")

    json_response = json.loads(response.text)
    for i in json_response:
        print(i)
    tuplepairs = []
    for i in json_response:
        try: 
            response = requests.get(("https://api.gemini.com/v1/symbols/details/" + str(i)), timeout=4)

        except:
            print("failed")

        json_response2 = json.loads(response.text)
        pair = [json_response2.get('base_currency'), json_response2.get('quote_currency')]
        tuplepairs.append(pair)

    spiffyMarkets = []
    for i in range(0, len(tuplepairs)-1, 1):
        if i != 0 or i != len(tuplepairs):
            if tuplepairs[i-1][0] == tuplepairs[i][0] or tuplepairs[i+1][0] == tuplepairs[i][0]:
                if(tuplepairs[i][1] != 'GBP' and tuplepairs[i][1] != 'EUR' and tuplepairs[i][1] != 'SGD'):
                    spiffyMarkets.append(tuplepairs[i])

    
    for i in spiffyMarkets:
        print(i)

    return spiffyMarkets

def get_crypto_combinations(market_symbols, base):
    BBS_combinations = []
    BSS_combinations = []
    for sym1 in market_symbols:
        sym1_token1 = sym1[0]
        sym1_token2 = sym1[1]
        if (sym1_token2 == base):
            for sym2 in market_symbols:
                sym2_token1 = sym2[0]
                sym2_token2 = sym2[1]
                if (sym1_token1 == sym2_token2):
                    for sym3 in market_symbols:
                        sym3_token1 = sym3[0]
                        sym3_token2 = sym3[1]
                        if((sym2_token1 == sym3_token1) and (sym3_token2 == sym1_token2)):
                            combination = [sym1_token1 + sym1_token2,  sym2_token1 + sym1_token1, sym2_token1 + sym1_token2]
                            BBS_combinations.append(combination.copy())
                            combination.reverse()
                            BSS_combinations.append(combination)
    # print(combinations)
    return BBS_combinations, BSS_combinations

async def main():
    # symbols = getSymbols()
    # symbols = [['BAT', 'BTC'], ['BAT', 'ETH'], ['BAT', 'USD'], ['BCH', 'BTC'], ['BCH', 'ETH'], ['BCH', 'USD'], ['BTC', 'DAI'], ['BTC', 'GUSD'], ['BTC', 'USD'], ['BTC', 'USDT'], ['DOGE', 'BTC'], ['DOGE', 'ETH'], ['DOGE', 'USD'], ['ETH', 'BTC'], ['ETH', 'DAI'], ['ETH', 'GUSD'], ['ETH', 'USD'], ['ETH', 'USDT'], ['GUSD', 'USD'], ['LINK', 'BTC'], ['LINK', 'ETH'], ['LINK', 'USD'], ['LTC', 'BCH'], ['LTC', 'BTC'], ['LTC', 'ETH'], ['LTC', 'USD'], ['OXT', 'BTC'], ['OXT', 'ETH'], ['OXT', 'USD'], ['ZEC', 'BCH'], ['ZEC', 'BTC'], ['ZEC', 'ETH'], ['ZEC', 'LTC'], ['ZEC', 'USD']]
    # For some inexplicable reason, gusdusd is not working??
    
    symbols = [['BAT', 'BTC'], ['BAT', 'ETH'], ['BAT', 'USD'], ['BCH', 'BTC'], ['BCH', 'ETH'], ['BCH', 'USD'], ['BTC', 'DAI'], ['BTC', 'GUSD'], ['BTC', 'USD'], ['BTC', 'USDT'], ['DOGE', 'BTC'], ['DOGE', 'ETH'], ['DOGE', 'USD'], ['ETH', 'BTC'], ['ETH', 'DAI'], ['ETH', 'GUSD'], ['ETH', 'USD'], ['ETH', 'USDT'], ['LINK', 'BTC'], ['LINK', 'ETH'], ['LINK', 'USD'], ['LTC', 'BCH'], ['LTC', 'BTC'], ['LTC', 'ETH'], ['LTC', 'USD'], ['OXT', 'BTC'], ['OXT', 'ETH'], ['OXT', 'USD'], ['ZEC', 'BCH'], ['ZEC', 'BTC'], ['ZEC', 'ETH'], ['ZEC', 'LTC'], ['ZEC', 'USD']]
    BBS_combinations, BSS_combinations = get_crypto_combinations(symbols, "USD")
    print("BBS Combinations:")
    for combo in BBS_combinations:
        print(combo)
    print("BSS Combinations")
    for combo in BSS_combinations:
        print(combo)

    tickerWS = []     
    loop = asyncio.get_running_loop()

    #Now the the full list of symbols has been aggregated, make a list of websocket classes
    #If the classes are declared as parts of the list tickerWS, is that where I can access them?
    for symbol in symbols:
        tickerWS.append(dataWebsocket(symbol))
    
    tm = time.localtime()
    filename = "data/gemAribitBot_" + str(tm[0]) + "-" + str(tm[1]) + "-" + str(tm[2]) + "_" + str(tm[3]) + "-" + str(tm[4]) + ".txt"

    watcher = Watcher(tickerWS,filename)

    print(tickerWS)

    for i in range(len(tickerWS)):
        print("in loop")
        loop.create_task(tickerWS[i].start("wss://api.gemini.com/v1/marketdata/" + str(symbols[i][0]) + str(symbols[i][1])))
        # loop.create_task(tickerWS[i].printTop())
    
    
    loop.create_task(watcher.start(tickerWS))
    await loop.run_in_executor(None, watcher.matcher, BBS_combinations, BSS_combinations)

    try:
        loop.run_forever()
    finally:
        loop.close()

if __name__ == '__main__':
    nest_asyncio.apply()
    asyncio.run(main())