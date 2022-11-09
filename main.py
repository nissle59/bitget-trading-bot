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
    third_count = 0.00
    final_count = 0.00

    fee = 0.002
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
        self.base_currency = self.__first_symbol['main']['quoteCoin']
        self.second_currency = self.__first_symbol['main']['baseCoin']
        self.__second_symbol = symbols[self.second_pair]
        self.third_currency = self.__second_symbol['main']['baseCoin']
        # self.__third_symbol = symbols[self.__third_pair]
        try:
            self.proxy_currency = self.__first_symbol['main']['baseCoin']
            self.first_rate = float(self.__first_symbol['trade_data']['sellOne'])
            fee = float(self.__first_symbol['main']['takerFeeRate'])
            self.proxy_count = self.base_count / self.first_rate * (1 - fee)
            self.second_count = self.proxy_count
            self.proxy_currency = self.__second_symbol['main']['quoteCoin']
            self.second_rate = float(self.__second_symbol['trade_data']['sellOne'])
            fee = float(self.__second_symbol['main']['takerFeeRate'])
            self.proxy_count = self.proxy_count / self.second_rate * (1 - fee)
            self.third_count = self.proxy_count
            # self.proxy_currency = self.__third_symbol['baseCoin']
            # self.proxy_count = self.second_count * self.__third_symbol['trade_data']['sellOne']
            self.third_pair = self.__second_symbol['main']['baseCoin'] + self.__first_symbol['main']['quoteCoin']
            if self.__second_symbol['main']['quoteCoin'] != self.__first_symbol['main']['baseCoin']: self.error = True
            if self.third_pair == self.second_pair: self.error = True
            self.__third_symbol = symbols[self.third_pair]
            self.proxy_currency = self.__third_symbol['main']['baseCoin']
            self.third_rate = float(self.__third_symbol['trade_data']['buyOne'])
            fee = float(self.__third_symbol['main']['takerFeeRate'])
            self.final_count = self.third_count * self.third_rate * (1 - fee)
            # self.final_count = self.proxy_count
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
                s = Schema(first_symbol, second_symbol)
                s.calculate_schema(symbols)
                if not (s.error):
                    # if s.base_currency in ['BUSD', 'USDT', 'USDC']:
                    logger_stream.info(s.first_pair + ' ' + s.second_pair + ' ' + s.third_pair)
                    schemas.append(s)
    #print(len(schemas))
    return schemas


def check_schema(schema: Schema,symbols_dict:dict):
    s_names = [schema.first_pair, schema.second_pair, schema.third_pair]
    path = '/api/spot/v1/market/depth?'
    tickers = {}
    for s_name in s_names:
        buf = f'symbol={s_name}_SPBL&type=step0&limit=3'
        u = url + path + buf
        r = requests.get(u)
        js = json.loads(r.content.decode('utf-8'))
        tickers.update({s_name:{'ask':float(js['data']['asks'][0][0]),
                                'bid':float(js['data']['bids'][0][0])}})
    for ticker in tickers:
        symbols_dict[ticker]['trade_data']['sellOne'] = tickers[ticker]['ask']
        symbols_dict[ticker]['trade_data']['buyOne'] = tickers[ticker]['bid']
        if ticker == schema.first_pair: schema.first_rate = tickers[ticker]['ask']
        elif ticker == schema.second_pair: schema.second_rate = tickers[ticker]['ask']
        elif ticker == schema.third_pair: schema.third_rate = tickers[ticker]['bid']
    schema.calculate_schema(symbols_dict)
    schema.final_percent = schema.calculate_percent(schema.base_count, schema.final_count)
    o = schema.final_count - schema.base_count
    return o


def main_process(threshold, schemas):
    f = open('full-income', 'r')
    full_income = float(f.read())
    f.close()
    #full_income = 0
    start_time = datetime.datetime.now()
    while True:
        curr_time = datetime.datetime.now()
        if (curr_time - start_time).total_seconds() < 1:
            time.sleep(1)
            start_time = datetime.datetime.now()
        symbols = get_symbols_data('symbols.json')
        schemas = get_all_schemas(symbols)
        f_st_time = datetime.datetime.now()
        for sch in schemas:
            # sch.calculate_schema(symbols)
            # if (sch.final_count - sch.base_count) > (threshold-1):
            #print(sch.final_count)
            if sch.base_currency in ['USDT']:
                i = check_schema(sch, symbols)
                if (sch.final_count - sch.base_count) > threshold:
                    #i = check_schema(sch, symbols)
                    c_st_time = datetime.datetime.now()
                    if (c_st_time - f_st_time).total_seconds() < 0.05:
                        time.sleep(0.05)
                        f_st_time = datetime.datetime.now()
                    full_income += sch.final_count - sch.base_count
                    f = open('full-income', 'w')
                    f.write(str(full_income))
                    f.close()
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
                    stf = '[{0}]: {1} {2} -> {3} ({4}) -> {5} ({6}) -> {7} ({8}) =>> {9}; FULL: {10}'.format(
                        datetime.datetime.now().__str__(), sch.base_count, sch.base_currency, sch.first_pair,
                        sch.first_rate, sch.second_pair, sch.second_rate, sch.third_pair, sch.third_rate,
                        sch.final_count, full_income.__str__())
                    logger_main.info(stf)
                    logger_stream.info(stf)
                    print(stf)
                else:
                    st = f'[{sch.final_count-sch.base_count}]\t{sch.first_pair} r({sch.first_rate} [{sch.second_count}]\t -> {sch.second_pair} r({sch.second_rate} [{sch.third_count}]\t -> {sch.third_pair} r({sch.third_rate} [{sch.final_count}])'
                    logger_stream.info(st)
                    print(st)


if __name__ == '__main__':
    symbols = get_symbols_data('symbols.json')
    schemas = get_all_schemas(symbols)
    main_process(0, schemas)