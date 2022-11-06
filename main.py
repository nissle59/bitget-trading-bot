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
url = 'https://api.binance.com'
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
    base_currency = ''
    second_currency = ''
    third_currency = ''

    first_pair = ''
    second_pair = ''
    third_pair = ''

    first_rate = 0
    second_rate = 0
    third_rate = 0

    base_count = 100
    second_count = 0.00
    final_count = 0.00

    fee = 0.00075
    final_percent = 0.00

    __first_symbol: dict
    __second_symbol: dict
    __third_symbol: dict
    error = False
    proxy_currency = ''
    proxy_count = 0.00

    def __init__(self, first_pair, second_pair, base_count=100):
        self.first_pair = first_pair
        self.second_pair = second_pair
        self.base_count = base_count

    def calculate_schema(self, symbols: dict):
        self.__first_symbol = symbols[self.first_pair]
        self.base_currency = self.__first_symbol['quoteAsset']
        self.second_currency = self.__first_symbol['baseAsset']
        self.__second_symbol = symbols[self.second_pair]
        self.third_currency = self.__second_symbol['baseAsset']
        # self.__third_symbol = symbols[self.__third_pair]
        try:
            self.proxy_currency = self.__first_symbol['baseAsset']
            self.first_rate = float(self.__first_symbol['askPrice'])
            self.proxy_count = self.base_count / self.first_rate * (1 - self.fee)
            self.proxy_currency = self.__second_symbol['quoteAsset']
            self.second_rate = float(self.__second_symbol['askPrice'])
            self.proxy_count = self.proxy_count / self.second_rate * (1 - self.fee)
            self.second_count = self.proxy_count
            # self.proxy_currency = self.__third_symbol['baseAsset']
            # self.proxy_count = self.second_count * self.__third_symbol['bidPrice']
            self.third_pair = self.__second_symbol['baseAsset'] + self.__first_symbol['quoteAsset']
            if self.__second_symbol['quoteAsset'] != self.__first_symbol['baseAsset']: self.error = True
            if self.third_pair == self.second_pair: self.error = True
            self.__third_symbol = symbols[self.third_pair]
            self.proxy_currency = self.__third_symbol['baseAsset']
            self.third_rate = float(self.__third_symbol['bidPrice'])
            self.final_count = self.second_count * self.third_rate * (1 - self.fee)
            # self.final_count = self.proxy_count
            self.calculate_percent(self.base_count, self.final_count)
        except:
            self.error = True
            pass

    def calculate_percent(self, start, final):
        perc = final / start - 1
        self.final_percent = perc
        return perc


#-----------------------------------------------------------

def get_symbol_market(symbol_name: str):
    path = '/api/v3/ticker/bookTicker?symbol=' + symbol_name
    r = requests.get(url + path)
    buf = r.content.decode('utf-8')
    js = json.loads(buf)
    bidPrice = float(js['bidPrice'])
    askPrice = float(js['askPrice'])
    bidQty = float(js['bidQty'])
    askQty = float(js['askQty'])
    o = {'symbol': symbol_name,
         'bidPrice': bidPrice,
         'bidQty': bidQty,
         'askPrice': askPrice,
         'askQty': askQty}
    return o


def get_exchange_info(fname):
    path = '/api/v3/exchangeInfo'
    r = requests.get(url + path)
    d = json.loads(r.content.decode('utf-8'))
    f = open(fname, 'wb')
    f.write(json.dumps(d, indent=4).encode('utf-8'))
    f.close()
    for symbol in d['symbols']:
        symbols.update({symbol['symbol']: symbol})
    f = open('symbols_' + fname, 'wb')
    f.write(json.dumps(symbols, indent=4).encode('utf-8'))
    f.close()
    return d


def get_symbols_data(fname):
    f = open(fname, 'rb')
    time_period = 0
    time_limit = 0
    d = json.loads(f.read().decode('utf-8'))
    f.close()
    rateLimits = info['rateLimits']
    max_requests_in_second = 0
    for limit in rateLimits:
        if limit['rateLimitType'] == 'RAW_REQUESTS':
            time_period = time_const[limit['interval']].__int__() * limit['intervalNum'].__int__()
            time_period_minutes = time_period // 60
            time_limit = limit['limit'].__int__()
            max_requests_in_second = time_limit // time_period
    s_count = 500
    urls = []
    path = '/api/v3/ticker/bookTicker'
    s_names = []
    i = 0
    k = 0
    for symbol in d:
        k += 1
        if i < s_count:
            if k < len(d):
                s_names.append(symbol)
                i += 1
            else:
                s_names.append(symbol)
                # print(symbol)
                buf = '%5B%22' + '%22,%22'.join(s_names) + '%22%5D'
                buf = '?symbols=' + buf
                i = 0
                buf = url + path + buf
                # print(buf)
                urls.append(buf)
                s_names = []
        else:
            buf = '%5B%22' + '%22,%22'.join(s_names) + '%22%5D'
            buf = '?symbols=' + buf
            i = 0
            buf = url + path + buf
            # print(buf)
            urls.append(buf)
            s_names = []
    now = datetime.datetime.now()
    i = 0

    def go(d, u, i):
        r = requests.get(u)
        i += 1
        buf = r.content.decode('utf-8')
        js = json.loads(buf)
        now_time = datetime.datetime.now().strftime('%d.%m.%Y %H:%M:%S')
        for item in js:
            bdict = d[item['symbol']]
            bdict.update({'bidPrice': item['bidPrice'],
                          'bidQty': item['bidQty'],
                          'askPrice': item['askPrice'],
                          'askQty': item['askQty'],
                          'updTime': now_time})
            d.update({item['symbol']: bdict})

    for u in urls:
        if datetime.datetime.now() > now:
            time.sleep(1)
            now = datetime.datetime.now()
            go(d, u, i)
        else:
            if i < max_requests_in_second:
                go(d, u, i)
            else:
                time.sleep(1)
                now = datetime.datetime.now()
                go(d, u, i)
    f = open('s.json', 'wb')
    f.write(json.dumps(d, indent=4).encode('utf-8'))
    f.close()
    return d


def get_exchange_symbols(fname):
    f = open(fname, 'rb')
    d = json.loads(f.read().decode('utf-8'))
    f.close()
    dd = {}
    for s in d:
        if ('SPOT' in d[s]['permissions']) and (d[s]['status'] == 'TRADING'):
            dd.update({s: d[s]})
    # print(dd)
    return dd


def find_3rd_pair(first_pair, second_pair, symbols_dict):
    third_pair = 'NONE'
    try:
        third_pair = symbols_dict[second_pair]['baseAsset'] + symbols_dict[first_pair]['quoteAsset']
        # print(third_pair)
    except:
        third_pair = 'NONE'
    if third_pair == 'NONE':
        return False
    else:
        return True


def check_schema(schema: Schema):
    s_names = [schema.first_pair, schema.second_pair, schema.third_pair]
    buf = '%5B%22' + '%22,%22'.join(s_names) + '%22%5D'
    path = '/api/v3/ticker/bookTicker?symbols=' + buf
    r = requests.get(url + path)
    d = {}
    js = json.loads(r.content.decode('utf-8'))
    for symbol in js:
        d.update({symbol['symbol']: symbol})
    first_symbol = d[schema.first_pair]
    second_symbol = d[schema.second_pair]
    third_symbol = d[schema.third_pair]
    first_rate = float(first_symbol['askPrice'])
    proxy_count = schema.base_count / first_rate * (1 - schema.fee)
    second_rate = float(second_symbol['askPrice'])
    proxy_count = proxy_count / second_rate * (1 - schema.fee)
    second_count = proxy_count
    third_rate = float(third_symbol['bidPrice'])
    final_count = second_count * third_rate * (1 - schema.fee)
    schema.first_rate = first_rate
    schema.second_rate = second_rate
    schema.third_rate = third_rate
    schema.final_count = final_count
    schema.final_percent = schema.calculate_percent(schema.base_count, schema.final_count)
    logger_stream.debug(
        f'{first_symbol["symbol"]} ({first_rate}) -> {second_symbol["symbol"]} ({second_rate}) -> {third_symbol["symbol"]} ({third_rate}) -> {base_currency} ({final_count})')
    o = final_count - base_count
    return o


if __name__ == '__main__':
    base_count = 100
    base_currency = 'BUSD'
    stf = 'BOT STARTED with {0} {1}\n'.format(base_count.__str__(), base_currency)
    logger_main.info(stf)
    schemas = []
    full_income = 0
    start_time = datetime.datetime.now()
    info = get_exchange_info('info.json')
    symbols = get_symbols_data('symbols_info.json')
    symbols = get_exchange_symbols('s.json')
    for first_symbol in symbols:
        for second_symbol in symbols:
            # print(first_symbol + " : "+ second_symbol)
            status = find_3rd_pair(first_symbol, second_symbol, symbols)
            if status:
                s = Schema(first_symbol, second_symbol)
                s.calculate_schema(symbols)
                if not (s.error):
                    if s.base_currency in ['BUSD', 'USDT', 'USDC']:
                        schemas.append(s)
    logger_main.info(f'Total schemas: {str(len(schemas))}')
    threshold = 0
    logger_main.info(f'Current profit threshold: {str(threshold)}')
    pairs = []
    for item in schemas:
        pairs.append(item.first_pair)
        pairs.append(item.second_pair)
        pairs.append(item.third_pair)
    print(len(pairs))
    pairs = set(pairs)
    logger_main.info(f'Updating {str(len(pairs))} pairs...')
    print(len(pairs))
    while True:
        symbols = get_exchange_symbols('s.json')
        for sch in schemas:
            sch.calculate_schema(symbols)
            if (sch.base_currency in ['BUSD', 'USDT', 'USDC']) and (sch.final_count - sch.base_count > threshold):
                i = check_schema(sch)
                if i > threshold:
                    full_income += i
                    """
                    st = '------------------------------------------------------------------\n'
                    st += 'Base count: 100 ' + sch.base_currency + '\n'
                    st += sch.first_pair + ' ' + sch.first_rate.__str__() + ' ' + sch.second_count.__str__() + ' ' + ';'.join(symbols[sch.first_pair]['permissions']) +'\n'
                    st += sch.second_pair + ' ' + sch.second_rate.__str__() + ' ' + sch.proxy_count.__str__() + '\n'
                    st += sch.third_pair + ' ' + sch.third_rate.__str__() + ' ' + sch.final_count.__str__() + '\n'
                    st += 'In fact profit will be '+i.__str__()+' '+sch.base_currency + '\n'
                    st += 'Total in '+ (datetime.datetime.now() - start_time).__str__() + ': +'+ round(full_income,2).__str__() + ' USD\n'
                    st += 'Difference: {0}'.format(float(sch.final_count) - i)+'\n'
                    st += '------------------------------------------------------------------\n\n'
                    """
                    stf = '[{0}]: {1} {2} -> {3} ({4}) -> {5} ({6}) -> {7} ({8}) =>> {9}; FULL: {10}\n'.format(
                        datetime.datetime.now().__str__(), sch.base_count, sch.base_currency, sch.first_pair,
                        sch.first_rate, sch.second_pair, sch.second_rate, sch.third_pair, sch.third_rate,
                        sch.base_count + i, full_income.__str__())
                    logger_main.info(stf)
                    logger_stream.info(stf)
                else:
                    st = '{0} -> {1} -> {2} -> {0}: Profit: {3}, Difference: {4}'.format(sch.base_currency,
                                                                                         sch.second_currency,
                                                                                         sch.third_currency, i, float(
                            sch.final_count - sch.base_count) - i)
                    logger_stream.info(st)
                    print(st)
