# This Exercise is still a work in progress

=====

# Sample Requests Project
## Leverage

Write a program that allows a user to specify certain parameters and leverage api endpoints at https://developers.coinbase.com/api/v2#data-endpoints to provide and return the data per requirements below. You can choose how you want to collect and manage user inputs.

* Allow the user to specify a currency code and return the full currency name for that code, include both the currency names and codes
* Allow the user to specify a currency then get data for all currencies and their exchange rate relative to that specified currency and output this data to a csv.
    * Should have at least the columns: base_currency_code, base_currency, currency_code, currency, exchange_rate
    * csv file name should follow the format {currency-code}-exchange_output.{timestamp}.csv
* Allow the user to specify buy, sell, or spot as well as a currency and return the bitcoin (BTC) buy/sell/spot price for that currency

#### Bonus
* For a given base currency (specified by user) find and return the currencies and rate with the highest or lowest exchange rate
* Create a fulld output file which contains currency, currency_code, exchange_rate for USD, and each of buy, sell, and spot BTC prices
