# ---------------------- Import Section ---------------------
import datetime
import json
import logging
import time

import requests

# -----------------------------------------------------------

# --------------------- LOGGERS INIT ------------------------
logger_stream = logging.getLogger('STREAM')
logger_stream.setLevel(logging.INFO)
handler_stream = logging.FileHandler(f"stream.log", mode='w')
formatter_stream = logging.Formatter("%(name)s [%(asctime)s] %(levelname)s %(message)s")
handler_stream.setFormatter(formatter_stream)
logger_stream.addHandler(handler_stream)

logger_main = logging.getLogger('MAIN')
logger_main.setLevel(logging.INFO)
handler_main = logging.FileHandler(f"main.log", mode='a')
formatter_main = logging.Formatter("[%(asctime)s] %(levelname)s %(message)s")
handler_main.setFormatter(formatter_main)
logger_main.addHandler(handler_main)
# -----------------------------------------------------------

# ---------------------- Base Constants ---------------------
url = 'https://api.bitget.com'
info = {}
symbols = {}
time_const = {
    'MINUTE': 60,
    'DAY': 60 * 60 * 24,
    'SECOND': 1
}


# -----------------------------------------------------------
# ----------------------- Classes Init ----------------------
class Schema:
    class Pair:
        name: str
        first_name: str
        second_name: str
        sell_price: float
        buy_price: float
        sell_vol: float
        buy_vol: float
        min_trade: float
        taker_fee: float
        maker_fee: float
        price_scale: int
        qty_scale: int
        status: bool
        count: float
        exist = False

        def __init__(self, symbol: dict):
            self.count = 0
            self.name = symbol['main']['symbolName']
            self.first_name = symbol['main']['baseCoin']
            self.second_name = symbol['main']['quoteCoin']
            self.sell_price = float(symbol['trade_data']['sellOne'])
            self.buy_price = float(symbol['trade_data']['buyOne'])
            self.taker_fee = float(symbol['main']['takerFeeRate'])
            self.maker_fee = float(symbol['main']['makerFeeRate'])
            self.exist = True
            try:
                self.sell_vol = float(symbol['trade_data']['sellVol'])
                self.buy_vol = float(symbol['trade_data']['buyVol'])
            except:
                self.sell_vol = 0
                self.buy_vol = 0
            self.min_trade = float(symbol['main']['minTradeAmount'])
            self.price_scale = int(symbol['main']['priceScale'])
            self.qty_scale = int(symbol['main']['quantityScale'])
            if symbol['main']['status'] == 'online':
                self.status = True
            else:
                self.status = False

    first: Pair
    second: Pair
    third: Pair

    base_currency = ''
    second_currency = ''
    third_currency = ''

    first_pair = ''
    second_pair = ''
    third_pair = ''

    base_count = 100
    final_count = 0.00

    final_percent = 0.00

    error = False

    def __init__(self, first_pair, second_pair, base_count=100):
        self.first_pair = first_pair
        self.second_pair = second_pair
        self.base_count = base_count

    def calculate_schema(self, symbols: dict):
        self.first = Schema.Pair(symbols[self.first_pair])
        self.base_currency = self.first.second_name
        self.second_currency = self.first.first_name
        self.second = Schema.Pair(symbols[self.second_pair])
        self.third_currency = self.second.first_name
        self.third_pair = self.third_currency + self.base_currency
        try:
            self.third = Schema.Pair(symbols[self.third_pair])

            fee = self.first.taker_fee
            self.first.count = self.base_count / self.first.sell_price * (1 - fee)  # BUY First coin

            fee = self.second.taker_fee
            self.second.count = self.first.count / self.second.sell_price * (1 - fee)  # BUY Second coin for First coin

            fee = self.third.taker_fee
            self.final_count = self.second.count * self.third.buy_price * (1 - fee)  # SELL Second coin for Base coin
            # -------------- Condition!!! ---------------------------
            if not self.third.exist: self.error = True
            if self.first.status and self.second.status and self.third.status:
                self.error = False
            else:
                self.error = True
            if self.third.second_name != self.first.second_name: self.error = True
            if self.first.first_name != self.second.second_name: self.error = True
            if self.third_pair == self.second_pair: self.error = True
            # print(self.error)
            # --------------------------------------------------------

            self.calculate_percent(self.base_count, self.final_count)
        except:
            self.error = True
            pass

    def calculate_percent(self, start, final):
        perc = final / start - 1
        self.final_percent = perc
        return perc


# -----------------------------------------------------------


def get_all_tickers(symbols_dict: dict):
    path = '/api/spot/v1/market/tickers'
    r = requests.get(url + path)
    data = json.loads(r.content.decode('utf-8'))['data']
    glob_buf = {}
    loc_buf = {}
    for symbol in data:
        loc_buf = {
            symbol['symbol']: {
                'trade_data': symbol,
                'main': symbols_dict[symbol['symbol']]
            }
        }
        glob_buf.update(loc_buf)
    return glob_buf


def get_symbols_data(fname='symbols.json'):
    path = '/api/spot/v1/public/products'
    r = requests.get(url + path)
    data = json.loads(r.content.decode('utf-8'))['data']
    buf = {}
    for symbol in data:
        buf.update({symbol['symbolName']: symbol})
    buf = get_all_tickers(buf)
    js = json.dumps(buf, indent=4).encode('utf-8')
    f = open(fname, 'wb')
    f.write(js)
    f.close()
    return buf


def find_3rd_pair(first_pair, second_pair, symbols_dict):
    third_pair = 'NONE'
    try:
        third_pair = symbols_dict[second_pair]['main']['baseCoin'] + symbols_dict[first_pair]['main']['quoteCoin']
        # print(third_pair)
    except:
        third_pair = 'NONE'
    if third_pair == 'NONE':
        return False
    else:
        return True


def get_all_schemas(symbols_dict: dict):
    schemas = []
    for first_symbol in symbols_dict:
        for second_symbol in symbols_dict:
            # print(first_symbol + " : "+ second_symbol)
            status = find_3rd_pair(first_symbol, second_symbol, symbols)
            if status:
                # print(first_symbol+" "+second_symbol)
                s = Schema(first_symbol, second_symbol)
                s.calculate_schema(symbols)
                if not (s.error):
                    # print(s.first_pair + " -> " + s.second_pair + " -> " + s.third_pair)
                    # if s.base_currency in ['BUSD', 'USDT', 'USDC']:
                    # logger_stream.info(s.first_pair + ' ' + s.second_pair + ' ' + s.third_pair)
                    schemas.append(s)
    #print(len(schemas))
    return schemas


def check_vol(schema: Schema, in_count=100.00):
    volume = False
    f_count = in_count / schema.first.sell_price * (1 - schema.first.taker_fee)
    if schema.first.sell_vol >= f_count:
        s_count = f_count / schema.second.sell_price * (1 - schema.second.taker_fee)
        if schema.second.sell_vol >= s_count:
            t_count = s_count * schema.third.buy_price * (1 - schema.third.taker_fee)
            if schema.second.sell_vol >= t_count:
                volume = True
    return volume


def check_schema(schema: Schema, symbols_dict: dict):
    s_names = [schema.first_pair, schema.second_pair, schema.third_pair]
    tickers = {}
    for s_name in s_names:
        tickers.update(get_actual_trade_data(s_name))
    for ticker in tickers:
        symbols_dict[ticker]['trade_data']['sellOne'] = tickers[ticker]['ask']
        symbols_dict[ticker]['trade_data']['sellVol'] = tickers[ticker]['askVol']
        symbols_dict[ticker]['trade_data']['buyOne'] = tickers[ticker]['bid']
        symbols_dict[ticker]['trade_data']['buyVol'] = tickers[ticker]['bidVol']
        if ticker == schema.first_pair:
            schema.first_rate = tickers[ticker]['ask']
        elif ticker == schema.second_pair:
            schema.second_rate = tickers[ticker]['ask']
        elif ticker == schema.third_pair:
            schema.third_rate = tickers[ticker]['bid']
    schema.calculate_schema(symbols_dict)
    schema.final_percent = schema.calculate_percent(schema.base_count, schema.final_count)
    o = schema.final_count - schema.base_count
    return o


def get_actual_trade_data(symbol: str) -> dict:
    path = '/api/spot/v1/market/depth?'
    buf = f'symbol={symbol}_SPBL&type=step0&limit=2'
    u = url + path + buf
    r = requests.get(u)
    js = json.loads(r.content.decode('utf-8'))
    ticker = {
        symbol: {
            'ask': float(js['data']['asks'][0][0]),
            'askVol': float(js['data']['asks'][0][1]),
            'bid': float(js['data']['bids'][0][0]),
            'bidVol': float(js['data']['bids'][0][1])
        }
    }
    return ticker


def main_process(threshold, schemas):
    try:
        f = open('full-income', 'r')
        full_income = float(f.read())
        f.close()
    except:
        full_income = 0
    start_time = datetime.datetime.now()
    while True:
        curr_time = datetime.datetime.now()
        if (curr_time - start_time).total_seconds() < 1:
            time.sleep(1)
            start_time = datetime.datetime.now()
        symbols = get_symbols_data('symbols.json')
        #print(len(symbols))
        schemas = get_all_schemas(symbols)
        f_st_time = datetime.datetime.now()
        #print(len(schemas))
        for sch in schemas:
            if sch.base_currency in ['USDT']:
                i = check_schema(sch, symbols)
                if i > threshold:
                    c_st_time = datetime.datetime.now()
                    if (c_st_time - f_st_time).total_seconds() < 0.05:
                        time.sleep(0.05)
                        f_st_time = datetime.datetime.now()
                    if check_vol(sch, sch.base_count):
                        full_income += i
                        f = open('full-income', 'w')
                        f.write(str(full_income))
                        f.close()

                        stf = '[=^_^=] {0}: {1} {2} -> {3} ({4}) -> {5} ({6}) -> {7} ({8}) =>> {9}; FULL: {10}'.format(
                            'OK', sch.base_count, sch.base_currency, sch.first_pair,
                            sch.first.sell_price, sch.second_pair, sch.second.sell_price, sch.third_pair,
                            sch.third.buy_price,
                            sch.base_count + i, full_income.__str__())
                        logger_main.info(stf)
                        logger_stream.info(stf)
                        print(stf)
                    else:
                        stf = '[:-(] {0}: {1} {2} -> {3} ({4}) -> {5} ({6}) -> {7} ({8}) =>> {9}; FULL: {10}'.format(
                            'Insufficient VOL',
                            sch.base_count,
                            sch.base_currency,
                            sch.first_pair,
                            sch.first.sell_price,
                            sch.second_pair,
                            sch.second.sell_price,
                            sch.third_pair,
                            sch.third.buy_price,
                            sch.base_count + i,
                            full_income.__str__())
                        logger_main.info(stf)
                        logger_stream.info(stf)
                        print(stf)
                else:
                    st = f'[{i}] {sch.first_pair} r({sch.first.sell_price} [{sch.first.count}] -> {sch.second_pair} r({sch.second.sell_price} [{sch.second.count}] -> {sch.third_pair} r({sch.third.buy_price} [{sch.base_count + i}])'
                    logger_stream.info(st)
                    print(st)


if __name__ == '__main__':
    symbols = get_symbols_data('symbols.json')
    print(f'Symbols found: {len(symbols)}')
    schemas = get_all_schemas(symbols)
    print(f'Schemas found: {len(schemas)}')
    main_process(0, schemas)
