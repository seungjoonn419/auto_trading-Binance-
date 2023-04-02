import time
import datetime
import logging
import logging.handlers
import requests
import pandas as pd
import json
import ccxt
import pprint
from binance.client import Client


INTERVAL = 1                                        # 매수 시도 interval (1초 기본)
DEBUG = False                                       # True: 매매 API 호출 안됨, False: 실제로 매매 API 호출
COIN_NUM = 0                                        # 분산 투자 코인 개수 (자산/COIN_NUM를 각 코인에 투자)
LARRY_K = 0.5
TRAILLING_STOP_GAP = 0.05                           # 최고점 대비 15% 하락시 매도
RESET_TIME = 20

# logger instance 생성
logger = logging.getLogger(__name__)

# formatter 생성
formatter = logging.Formatter('[%(asctime)s][%(levelname)s|%(filename)s:%(lineno)s] >> %(message)s')

# handler 생성 (stream, file)
streamHandler = logging.StreamHandler()
timedfilehandler = logging.handlers.TimedRotatingFileHandler(filename='logfile', when='midnight', interval=1, encoding='utf-8')

# logger instance에 fomatter 설정
streamHandler.setFormatter(formatter)
timedfilehandler.setFormatter(formatter)
timedfilehandler.suffix = "%Y%m%d"

# logger instance에 handler 설정
logger.addHandler(streamHandler)
logger.addHandler(timedfilehandler)

# logger instnace로 log 찍기
logger.setLevel(level=logging.DEBUG)


# Load account
with open("config.txt") as f:
    lines = f.readlines()
    api_key = lines[0].strip()
    secret  = lines[1].strip()
    binance = ccxt.binance(config={
        'apiKey': api_key,
        'secret': secret,
        'enableRateLimit': True,
        'options': {
            'defaultType': 'future'
        }
    })


def make_sell_times(now):
    '''
    금일 09:01:00 시각과 09:01:10초를 만드는 함수
    param now: 현재 시간
    '''
    today = now
    sell_time = datetime.datetime(year=today.year,
            month=today.month,
            day=today.day,
            hour=9,
            minute=1,
            second=0)
    sell_time_after_10secs = sell_time + datetime.timedelta(seconds=20)
    return sell_time, sell_time_after_10secs


def make_setup_times(now):
    '''
    익일 09:01:00 시각과 09:01:10초를 만드는 함수
    param now: 현재 시간
    '''
    tomorrow = now + datetime.timedelta(1)
    midnight = datetime.datetime(year=tomorrow.year,
            month=tomorrow.month,
            day=tomorrow.day,
            hour=9,
            minute=1,
            second=0)
    midnight_after_10secs = midnight + datetime.timedelta(seconds=20)
    return midnight, midnight_after_10secs


def get_cur_prices(tickers):
    '''
    모든 가상화폐에 대한 현재가 조회
    param tickers: 선물 거래의 모든 종목
    :return: 현재가
    '''
    try:
        cur_prices = {}
        for ticker in tickers:
            cur_price = binance.fetch_ticker(ticker)
            cur_prices[ticker] = cur_price['last']

        return cur_prices
    except Exception as e:
        logger.info('get_cur_prices() Exception occur')
        logger.info(e)
        return None

def get_df(ticker):
    '''
    ticker에 대한 df를 조회
    '''
    try:
        btc = binance.fetch_ohlcv(
            symbol=ticker, 
            timeframe='1d', 
            since=None, 
            limit=10)

        df = pd.DataFrame(btc, columns=['datetime', 'open', 'high', 'low', 'close', 'volume'])
        df['datetime'] = pd.to_datetime(df['datetime'], unit='ms')
        df.set_index('datetime', inplace=True)

        return df
    except Exception as e:
        logger.error('get_df() Exception occur')
        logger.error(e)
        return Non

def cal_target(ticker):
    '''
    각 코인에 대한 목표가 저장
    :param ticker: 티커, 'BTC'
    '''
    try:
        df = get_df(ticker)

        yesterday = df.iloc[-2]
        today_open = yesterday['close']
        yesterday_high = yesterday['high']
        yesterday_low = yesterday['low']
        target = today_open + (yesterday_high - yesterday_low) * LARRY_K

        return today_open, target
    except Exception as e:
        logger.error('cal_target Exception occur')
        logger.error('ticker: %s', ticker)
        logger.error(e)

        # 절대 매수를 하지 못 하도록 높은 값을 설정
        return 100000000000000000000, 100000000000000000000


def set_targets(tickers):
    '''
    티커 코인들에 대한 목표가 계산
    :param tickers: 코인에 대한 티커 리스트
    '''
    closes = {}
    targets = {}
    for ticker in tickers:
        closes[ticker], targets[ticker] = cal_target(ticker)
        time.sleep(0.1)
    return closes, targets


def cal_volume(ticker):
    '''
    각 코인에 대한 전일 거래량 * 전일 종가 = 거래대금(대략)
    '''
    try:
        df = get_df(ticker)
        yesterday = df.iloc[-2]
        yesterday_volume = yesterday['volume']
        yesterday_close = yesterday['close']

        return yesterday_volume * yesterday_close 
    except Exception as e:
        logger.error('cal_volume Exception occur')
        logger.error(e)
        return 0

def set_volumes(tickers):
    '''
    티커 코인들에 대한 전일 거래대금(대략)
    '''
    volumes = {}
    each_volume = {}
    total_volume = 0

    for ticker in tickers:
        volume = cal_volume(ticker)
        volumes[ticker] = volume
        total_volume += volume
        time.sleep(0.1)

    for ticker in tickers:
        each_volume[ticker] = volumes[ticker]/total_volume

    return volumes, total_volume, each_volume


def get_portfolio(tickers, prices, targets):
    '''
    매수 조건 확인 및 매수 시도
    :param tickers: 코인 리스트
    :param prices: 각 코인에 대한 현재가
    :param targets: 각 코인에 대한 목표가
    '''
    portfolio = []
    try:
        for ticker in tickers:
            price = prices[ticker]              # 현재가
            target = targets[ticker]            # 목표가

            # 현재가가 목표가 이상
            if price >= target:
                portfolio.append(ticker)

        return portfolio
    except Exception as e:
        logger.error('get_portfolio Exception occur')
        logger.error(e)
        return None

def buy_volume(coin, prices, targets, holdings, budget_list):
    '''
    매수 조건 확인 및 매수 시도
    '''
    try:
        target = targets[coin]
        price = prices[coin]
        budget = budget_list[coin]

        logger.info('-----buy_volume()-----')
        logger.info('ticker: %s', coin)
        logger.info('budget(Margin): %s', budget)
        logger.info('price: %s', price)
        logger.info('target: %s', target)


        # 현재 보유하지 않은 상태 
        if holdings[coin] is False: 

            budget = budget_list[coin] 

            if DEBUG is False:
                market = binance.market(coin)

                # 레버리지 설정
                market = binance.market(coin)
                leverage = 5

                resp = binance.fapiPrivate_post_leverage({
                    'symbol': market['id'],
                    'leverage': leverage
                })

                # 매수 주문
                order_amount = (budget/price) * leverage * 0.99

                ret = binance.create_market_buy_order(
                    symbol=coin,
                    amount=order_amount
                )
                logger.info('----------buy_market_order ret-----------')
                logger.info('Ticker: %s', coin)
                logger.info(ret)
            else:
                logger.info('BUY API CALLED: %s', coin)

            #time.sleep(INTERVAL)
        else:
            logger.info('Already have: %s', coin)
    except Exception as e:
        logger.error('buy_volume Exception occur')
        logger.error(e)


def get_balance_unit(tickers):
    '''
    잔고 조회
    '''
    try:
        balance = binance.fetch_balance()
        positions = balance['info']['positions']
        units = {ticker:0 for ticker in tickers}

        for position in positions:

            if float(position['positionAmt']) > 0:
                logger.info('position: %s', position)
                length = len(position['symbol']) - 4
                unit = position['symbol'][:length] + "/USDT:USDT"
                units[unit] = float(position['positionAmt'])

        return units
    except Exception as e:
        logger.info('get_balance_unit() Exception occur')
        logger.info(e)


def try_sell(tickers):
    '''
    보유하고 있는 모든 코인에 대해 전량 매도
    '''
    try:
        # 잔고조회
        units = get_balance_unit(tickers)

        logger.info('----------try_sell(tickers)---------')
        logger.info('try_sell before sell units')
        logger.info(units)

        for ticker in tickers:
            unit = units.get(ticker, 0)                     # 보유 수량

            logger.info('ticker: ', ticker)
            logger.info('try_sell unit: ', unit)

            if unit > 0:
                if DEBUG is False:
                    ret = binance.create_market_sell_order(
                        symbol=ticker,
                        amount=unit
                    )

                    logger.info('----------sell_market_order ret-----------')
                    logger.info(ret)
                    #time.sleep(INTERVAL)

                else:
                    print("SELL API CALLED", ticker, buy_price, min_unit)

        # 매도 이후에 잔고를 재조회하여 확인한다
        logger.info('try_sell after sell units')
        units = get_balance_unit(tickers)
        logger.info(units)

    except Exception as e:
        logger.error('try_sell Exception occur')
        logger.error(e)


def new_set_budget(tickers, each_volume):
    '''
    코인별 투자할 투자 금액 계산
    :return: 원화잔고 * 코인별 투자 비율
    '''
    try:
        budget_list = {}

        balance = binance.fetch_balance(params={"type": "future"})
        free_balance = balance['USDT']['free']
        logger.info('free_balance: %s', free_balance)

        for ticker in tickers:
            budget_list[ticker] = each_volume[ticker] * free_balance

        return budget_list

    except Exception as e:
        logger.error('new_set_budget Exception occur')
        logger.error(e)
        return 0


def set_holdings(tickers):
    '''
    현재 보유 중인 종목
    :return: 보유 종목 리스트
    '''
    try:
        units = get_balance_unit(tickers)                   # 잔고 조회
        holdings = {ticker:False for ticker in tickers}        

        for ticker in tickers:
            unit = units.get(ticker, 0)                     # 보유 수량

            if unit > 0:
                holdings[ticker] = True

        return holdings
    except Exception as e:
        logger.error('set_holdings() Exception error')
        logger.error(e)


def print_status(portfolio, prices, targets, closes):
    '''
    코인별 현재 상태를 출력
    :param tickers: 티커 리스트
    :param prices: 가격 리스트
    :param targets: 목표가 리스트
    :param closes: 종가 리스트
    '''
    try:
        for ticker in portfolio:
            close = closes[ticker]
            price = prices[ticker]
            target = targets[ticker]
            ascent = (price - close) / close                # 상승률
            gain = (price - target) / target                # 이익률

            logger.info('-------------------------------------------')
            logger.info(ticker)
            logger.info('목표가')
            logger.info(target)
            logger.info('현재가')
            logger.info(price)
            logger.info('상승률')
            logger.info(ascent)
            logger.info('목표가 대비 상승률')
            logger.info(gain)
        logger.info('-------------------------------------------')
    except:
        pass


def get_tickers():
    '''
    선물 종목만 조회
    비트코인과 이더리움은 제외('거래량 * 전일 종가'의 비중이 너무 커서 다른 종목의 매수 비중이 작아진다)
    return: 비트코인과 이더리움을 제외한 선물 종목
    '''
    try:
        markets = binance.load_markets()
        tickers = list()
        for sym in markets:
            if sym[-5:] == ':USDT' and sym[:8] != 'BTC/USDT' and sym[:8] != 'ETH/USDT':
                # Set MarginType Isolated
                length = len(sym) - 10
                unit = sym[:length] + "USDT"
                set_marginType(unit)

                df = get_df(sym)
                if df.iloc[-2]['volume'] > 0:
                    tickers.append(sym)

        return tickers
    except Exception as e:
        logger.error('get_tickers() Exception error')
        logger.error(e)

def set_marginType(ticker):
    '''
    마진을 Isolated로 설정
    '''
    try:
        ret = binance.fapiPrivate_post_margintype({'symbol': ticker, 'marginType': 'ISOLATED'})
        logger.info(ret)
    except Exception as e:
        logger.info('set_marginType() Exception occur')
        logger.info(e)

#----------------------------------------------------------------------------------------------------------------------
# 매매 알고리즘 시작
#---------------------------------------------------------------------------------------------------------------------
now = datetime.datetime.now()                                            # 현재 시간 조회
sell_time1, sell_time2 = make_sell_times(now)                            # 초기 매도 시간 설정
setup_time1, setup_time2 = make_setup_times(now)                         # 초기 셋업 시간 설정

tickers = get_tickers()
COIN_NUM = len(tickers)
closes, targets = set_targets(tickers)                                   # 코인별 목표가 계산

volume_list = {}                                                         # 전일 거래대금을 저장

# upbit API의 호출 제한 때문에 2초간의 딜레이를 준다.
time.sleep(2)
volume_list, total_volume, each_volume = set_volumes(tickers)            # 전일 거래량
logger.info('volume_list: %s', volume_list)
logger.info('total_volume: %f', total_volume)
logger.info('each_volume: %s', each_volume)

budget_list = new_set_budget(tickers, each_volume)                       # 코인별 최대 배팅 금액 계산
logger.info('budget_list(Margin): %s', budget_list)                      # 선물 거래에서는 증거금에 해당

while True:

    now = datetime.datetime.now()

    # 새로운 거래일에 대한 데이터 셋업 (09:01:00 ~ 09:01:20)
    # 금일, 익일 포함
    if (sell_time1 < now < sell_time2) or (setup_time1 < now < setup_time2):
        logger.info('New Date SetUp Start')

        # 시가에 매도
        try_sell(tickers)     

        setup_time1, setup_time2 = make_setup_times(now)                 # 다음 거래일 셋업 시간 갱신

        tickers = get_tickers()                                          # 티커 리스트 얻기
        COIN_NUM = len(tickers)
        closes, targets = set_targets(tickers)                           # 목표가 갱신

        logger.info('Targets: %s', targets)

        volume_list = {}                                                 # 전일 대비 거래량 순위

        # upbit API의 호출 제한 때문에 2초간의 딜레이를 준다.
        time.sleep(2)
        volume_list, total_volume, each_volume = set_volumes(tickers)    # 전일 거래량
        logger.info('volume_list: %s', volume_list)
        logger.info('total_volume: %f', total_volume)
        logger.info('each_volume: %s', each_volume)

        budget_list = new_set_budget(tickers, each_volume)               # 코인별 최대 배팅 금액 계산
        logger.info('budget_list: %s', budget_list)

        logger.info('New Date SetUp End')
        time.sleep(10)

    prices = get_cur_prices(tickers)                                     # 현재가 계산

    portfolio = get_portfolio(tickers, prices, targets)       
    logger.info('Portfolio: %s', portfolio)

    #print_status(portfolio, prices, targets, closes)

    # 매수
    holdings = set_holdings(tickers)
    for coin in portfolio:
        buy_volume(coin, prices, targets, holdings, budget_list)

    time.sleep(INTERVAL)
