###### Refinitiv Matching Engine Exercise

Your task is to create a new matching engine for FX orders. The engine will take a CSV file of orders for a given
currency pair and match orders together. In this example you'll be looking at USD/GBP.

There are two types of orders, BUY and SELL orders. A BUY order is for the price in USD you'll pay for GBP, SELL
order is the price in USD you'll sell GBP for.

Each order has the following fields:
1. Order ID
   - This is a unique ID in the file which is used to track an order
2. User Name
   - This is the user name of the user making the order
3. Order Time
   - This is the time, in milliseconds since Jan 1st 1970, the order was placed
4. Order Type
   - Either BUY or SELL
5. Quantity
   - The number of currency units you want to BUY or SELL
6. Price
   - The price you wish to sell for, this is in the lowest unit of the currency, i.e. for GBP it's in pence and for USD it's cents

The matching engine must do the following:
- It should match orders when they have the same quantity
- If an order is matched it should be closed
- If an order is not matched it should be kept on an "order book" and wait for an order which does match
- When matching an order to the book the order should look for the best price
- The best price for a BUY is the lowest possible price to BUY for
- The best price for a SELL is the highest possible price to SELL for
- You should always use the price from the "order book" when matching orders
- When an order has matched you should record the IDs of both orders, the price, quantity and time of the match
- If two orders match with the same price the first order is used
- Orders won't have the same timestamp

The file exampleOrders.csv is some example trading data, the matches for that trading data is in outputExampleMatches.csv
