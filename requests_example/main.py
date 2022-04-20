import json
import requests
from argparse import ArgumentParser
from datetime import datetime

from pandas import read_json


CUR_URL = "https://api.coinbase.com/v2/currencies"
XCH_URL = "https://api.coinbase.com/v2/exchange-rates"
XCH_URL_CURR = "https://api.coinbase.com/v2/exchange-rates?currency={0}"
PRICE_URLS = {
    'buy':      "https://api.coinbase.com/v2/prices/{0}/buy",
    'sell':     "https://api.coinbase.com/v2/prices/{0}/sell",
    'spot':     "https://api.coinbase.com/v2/prices/{0}/spot",
}

def get_currency_data():
    """ """

    df = read_json(CUR_URL, orient='split')
    df.rename(columns={
        'id': 'currency_code', 'name': 'currency'}, inplace=True)

    return df[['currency', 'currency_code']]


def get_currency_code(currency_name):
    """ """
    currency_df = get_currency_data()
    try:
        currency_code = currency_df[
            currency_df['currency'].str.contains(currency_name, case=False)
            ]['currency_code'].values[0]
    except IndexError as e:
        currency_code = "Could not find matching currency_code"

    return currency_code


def get_currency_name(currency_code):
    """ """
    currency_df = get_currency_data()
    try:
        currency_name = currency_df[
            currency_df['currency_code'].str.contains(currency_code, case=False)
            ]['currency'].values[0]
    except IndexError as e:
        currency_name = "Could not find matching currency_name"
    return currency_name


def get_exchange_rates(currency_code=None):
    """ """
    url = XCH_URL_CURR.format(currency_code) if currency_code is not None else XCH_URL

    df = read_json(url, orient='split')
    df.reset_index(inplace=True)
    df.rename(columns={"currency":"base_currency","index":"currency", "rates":"exchange_rate"}, inplace=True)

    return df


def get_price(price_type, currency):
    """ """
    try:
        url = PRICE_URLS[price_type]
    except KeyError:
        raise ValueError("Price Type: `{price type}` not recognized")

    res = requests.get(url.format(f'BTC-{currency}'))
    data = json.loads(res.content)['data']
    price = data['amount']

    return price


def parse_cmd_args():
    """ Parses command line arguments and returns them as a dictionary """
    ### Set up argument parser
    parser = ArgumentParser(description='Process input arguments')

    ### Add command line arguments
    parser.add_argument('main_cmd', metavar='Input', type=str, nargs=1,
            help='Expects a single argument of "name", "rates", or "price"')
    parser.add_argument('-code', metavar='Currency Code', type=str,
            help='Currency Code - e.g. USD', default=None)
    parser.add_argument('-name', metavar='Currency Name', type=str,
            help='Currency Name', default=None)
    parser.add_argument('-price', metavar='Price Type', type=str,
            help='Price Type - expects "buy", "sell", or "spot", must also have specified a -code or -name arg', default=None)

    ### Send command line arguments to dict
    cmd_args = vars(parser.parse_args())

    ### Since nargs puts this arg in a list, need to pull it out
    cmd_args['main_cmd'] = cmd_args['main_cmd'][0]

    return cmd_args


def run_currency_name(cmd_args):
    """ """
    ### Get code or name, run function based on which is supplied and return
    curr_code = cmd_args.get('code')
    curr_name = cmd_args.get('name')

    if curr_code is not None:
        curr_name = get_currency_name(curr_code)
    elif curr_name is not None:
        curr_code = get_currency_code(curr_name)
    else:
        raise ValueError("Must provide either currency code or currency name.")

    print(f"{curr_code} : {curr_name}")

    return True


def run_currency_rates(cmd_args):
    """ """
    curr_code = cmd_args.get('code')
    curr_name = cmd_args.get('name')

    if curr_code is None and curr_name is not None:
        curr_code = get_currency_code(curr_name)
    else:
        raise ValueError("Must provide either currency code or currency name.")

    exch_df = get_exchange_rates(curr_code)
    exch_df.rename(columns={"currency":"currency_code", "base_currency":"base_currency_code"}, inplace=True)

    all_currencies_df = get_currency_data()

    final_df = exch_df.merge(all_currencies_df, how='left', on='currency_code')
    final_df = final_df[~final_df['currency'].isnull()]

    if curr_name is None:
        curr_name = get_currency_name(curr_code)

    final_df['base_currency'] = curr_name
    final_df = final_df[['currency_code', 'currency', 'base_currency_code', 'base_currency', 'exchange_rate']]

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    output_filename = f"requests_example/output/{curr_code}-exchange_output.{timestamp}.csv"

    final_df.to_csv(output_filename, index=False)

    return True


def run_currency_price(cmd_args):
    """ """
    curr_code = cmd_args.get('code')
    curr_name = cmd_args.get('name')
    price_types = cmd_args.get('price')

    if curr_code is None and curr_name is not None:
        curr_code = get_currency_code(curr_name)
    elif curr_code is None and curr_name is None:
        raise ValueError("Must provide either currency code or currency name.")

    for price_type in price_types.split(','):
        price = get_price(price_type, curr_code)
        print(f"Current {price_type} price for BTC-{curr_code} is: {price}")

    return True


def run(cmd_args):
    """ """
    funcs = {
        "name": run_currency_name,
        "rates": run_currency_rates,
        "price": run_currency_price,
    }
    cmd = cmd_args.get('main_cmd')

    try:
        func = funcs[cmd]
    except KeyError:
        raise ValueError(f'`{cmd}` is not a recognized command')

    func(cmd_args)

    return True


if __name__ == '__main__':
    run(parse_cmd_args())
    # print(get_exchange_rates())
    # print(get_exchange_rates('BAM'))
    # print(get_price('buy', 'ALL'))