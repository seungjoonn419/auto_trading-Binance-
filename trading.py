import pyupbit
import schedule
import time
import datetime
import logging
import logging.handlers
import requests
import pandas as pd
import json

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
with open("upbit.txt") as f:
    lines = f.readlines()
    key = lines[0].strip()
    secret = lines[1].strip()
    upbit = pyupbit.Upbit(key, secret)

def make_sell_times(now):
    '''
    금일 09:01:00 시각과 09:01:10초를 만드는 함수
    :param now: DateTime
    :return:
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
    :param now:
    :return:
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

def make_volume_times(now):
    today = now
    sell_time = datetime.datetime(year=today.year,
            month=today.month,
            day=today.day,
            hour=13,
            minute=0,
            second=0)
    return sell_time

def make_portfolio_today_times(now):
    '''
    금일 09:30:00 시각과 09:30:10초를 만드는 함수
    :param now: DateTime
    :return:
    '''
    today = now
    sell_time1 = datetime.datetime(year=today.year,
            month=today.month,
            day=today.day,
            hour=9,
            minute=30,
            second=0)
    sell_time2 = sell_time1 + datetime.timedelta(seconds=10)

    sell_time3 = datetime.datetime(year=today.year,
            month=today.month,
            day=today.day,
            hour=13,
            minute=30,
            second=0)
    sell_time4 = sell_time3 + datetime.timedelta(seconds=10)
    return sell_time1, sell_time2, sell_time3, sell_time4

def get_cur_prices(tickers):
    '''
    모든 가상화폐에 대한 현재가 조회
    :return: 현재가, {'KRW-BTC': 7200000, 'KRW-XRP': 500, ...}
    '''
    try:
        return pyupbit.get_current_price(tickers)
    except:
        return None

def inquiry_high_prices(tickers):
    try:
        high_prices = {}
        for ticker in tickers:
            df = pyupbit.get_ohlcv(ticker, interval="day", count=10)
            today = df.iloc[-1]
            today_high = today['high']
            high_prices[ticker] = today_high

        return high_prices
    except:
        return  {ticker:0 for ticker in tickers}

def cal_target(ticker):
    '''
    각 코인에 대한 목표가 저장
    :param ticker: 티커, 'BTC'
    :return: 목표가
    '''
    try:
        df = pyupbit.get_ohlcv(ticker, interval="day", count=10)
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
        return 100000000000000000000000, 1000000000000000000000000


def set_targets(tickers):
    '''
    티커 코인들에 대한 목표가 계산
    :param tickers: 코인에 대한 티커 리스트
    :return:
    '''
    closes = {}
    targets = {}
    for ticker in tickers:
        closes[ticker], targets[ticker] = cal_target(ticker)
        time.sleep(0.1)
    return closes, targets

def cal_volume(ticker):
    '''
    각 코인에 대한 전일 거래량
    :param ticker: 티커
    :return: 전일대비 거래량
    '''
    try:
        df = pyupbit.get_ohlcv(ticker, interval="day", count=10)
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
    티커 코인들에 대한 전일 거래량
    :param tickers: 코인에 대한 티커 리스트
    :return:
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


def get_portfolio(tickers, prices, targets, blackList):
    '''
    매수 조건 확인 및 매수 시도
    :param tickers: 코인 리스트
    :param prices: 각 코인에 대한 현재가
    :param targets: 각 코인에 대한 목표가
    :return:
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

def buy_volume(volume_list, prices, targets, holdings, budget_list, blackList, high_prices):
    '''
    매수 조건 확인 및 매수 시도
    '''
    try:
        for ticker in volume_list:
            #tick = ticker[0]
            tick = ticker
            high = high_prices[tick]
            target = targets[tick]
            price = prices[tick]

            logger.info('-----buy_volume()-----')
            logger.info('ticker')
            logger.info(ticker)
            logger.info('price')
            logger.info(price)
            logger.info('high')
            logger.info(high)
            logger.info('target')
            logger.info(target)


            # 현재 보유하지 않은 상태 
            if holdings[tick] is False: 
                logger.info('Ticker')
                logger.info(ticker)
                fee = budget_list[tick] * 0.0005

                if DEBUG is False:
                    ret = upbit.buy_market_order(tick, budget_list[tick] - fee)
                    logger.info('----------buy_market_order ret-----------')
                    logger.info(ret)
                else:
                    logger.info('BUY VOLUME')
                    print("BUY API CALLED", tick)

                time.sleep(INTERVAL)
    except Exception as e:
        logger.error('buy_volume Exception occur')
        logger.error(e)

# 잔고 조회
def get_balance_unit(tickers):
    balances = upbit.get_balances()
    units = {ticker:0 for ticker in tickers}

    for balance in balances:
        if balance['currency'] == "KRW":
            continue

        ticker = "KRW-" + balance['currency']        # XRP -> KRW-XRP
        unit = float(balance['balance'])
        units[ticker] = unit
    return units


def sell_holdings(tickers, portfolio, prices, targets, blackList):
    '''
    보유하고 있는 모든 코인에 대해 전량 매도
    :param tickers: 업비트에서 지원하는 암호화폐의 티커 목록
    :return:
    '''
    try:
        # 잔고조회
        units = get_balance_unit(tickers)

        for ticker in tickers:
            unit = units.get(ticker, 0)                     # 보유 수량
            price = prices[ticker]
            target = targets[ticker]
            gain = (price-target)/target

            if unit > 0:
                orderbook = pyupbit.get_orderbook(ticker)['orderbook_units'][0]
                buy_price = int(orderbook['bid_price'])                                 # 최우선 매수가
                buy_unit = orderbook['bid_size']                                        # 최우선 매수수량
                min_unit = min(unit, buy_unit)

                # 보유 중인 코인이 포트폴리오에 없으면 매도한다.
                if ticker not in portfolio:
                    if DEBUG is False:
                        upbit.sell_market_order(ticker, unit)
                    else:
                        print("SELL HOLDINGS API CALLED", ticker, buy_price, min_unit)

                # 손실이 -2%를 넘으면 매도한다.
                if gain <= -0.02:
                    if DEBUG is False:
                        upbit.sell_market_order(ticker, unit)
                    else:
                        print("SELL HOLDINGS API CALLED", ticker, buy_price, min_unit)

                #blackList[ticker] = True

    except Exception as e:
        logger.error('sell_holdings Exception occur')
        logger.error(e)



def try_sell(tickers):
    '''
    보유하고 있는 모든 코인에 대해 전량 매도
    :param tickers: 업비트에서 지원하는 암호화폐의 티커 목록
    :return:
    '''
    try:
        # 잔고조회
        units = get_balance_unit(tickers)

        logger.info('----------try_sell(tickers)---------')
        logger.info('try_sell before sell units')
        logger.info(units)

        for ticker in tickers:
            short_ticker = ticker.split('-')[1]
            unit = units.get(ticker, 0)                     # 보유 수량

            logger.info('-----------ticker-----------')
            logger.info(ticker)
            logger.info('-----------try_sell unit---------')
            logger.info(unit)

            if unit > 0:
                orderbook = pyupbit.get_orderbook(ticker)['orderbook_units'][0]
                logger.info('----------orderbook----------')
                logger.info(orderbook)
                buy_price = int(orderbook['bid_price'])                                 # 최우선 매수가
                buy_unit = orderbook['bid_size']                                        # 최우선 매수수량
                min_unit = min(unit, buy_unit)

                if DEBUG is False:
                    ret = upbit.sell_market_order(ticker, unit)
                    logger.info('----------sell_market_order ret-----------')
                    logger.info(ret)
                    time.sleep(INTERVAL)

                else:
                    print("SELL API CALLED", ticker, buy_price, min_unit)

        logger.info('try_sell after sell units')
        logger.info(units)

    except Exception as e:
        logger.error('try_sell Exception occur')
        logger.error(e)

def sell(ticker, unit):
    orderbook = pyupbit.get_orderbook(ticker)['orderbook_units'][0]
    buy_price = int(orderbook['bid_price'])                                 # 최우선 매수가
    buy_unit = orderbook['bid_size']                                        # 최우선 매수수량

    if DEBUG is False:
        pyupbit.sell_market_order(tick, unit)
    else:
        print("trailing stop", tick, buy_price, unit)

def try_trailling_stop(volume_list, prices, closes, targets, high_prices, blackList):
    '''
    trailling stop
    :param portfolio: 포트폴리오
    :param prices: 현재가 리스트
    :param closes: 전일 종가
    :param targets: 목표가 리스트
    :param high_prices: 각 코인에 대한 당일 최고가 리스트
    :return:
    '''
    try:
        # 잔고 조회
        units = get_balance_unit(tickers)

        for ticker in volume_list:
            tick = ticker[0]
            price = prices[tick]                          # 현재가
            target = targets[tick]                        # 목표가
            high_price = high_prices[tick]                # 당일 최고가
            close = closes[tick]                          # 전일 종가
            span_a = spans_a[tick]                        # 선행 스팬 1
            span_b = spans_b[tick]                        # 선행 스팬 2
            unit = units.get(tick, 0)                     # 보유 수량

            gain = (price - target) / target                # 이익률
            ascent = (price - close) / close                # 상승률
            gap_from_high = 1 - (price/high_price)          # 고점과 현재가 사이의 갭

            if unit > 0:
                logger.info('TRY_TRAILLING_STOP() Ticker')
                logger.info(tick)
                if ticker[1] < 1 and gain * 100 >= 0.5:
                    logger.info('Condition 1')
                    sell(tick, unit)
                    blackList[tick] = True

                elif ticker[1] >= 1 and ticker[1] < 5 and gain * 100 >= 1 :
                    logger.info('Condition 2')
                    sell(tick, unit)
                    blackList[tick] = True

                elif ticker[1] >= 5 and ticker[1] < 10 and gain * 100 >= 1.5 :
                    logger.info('Condition 2')
                    sell(tick, unit)
                    blackList[tick] = True

                elif ticker[1] >= 10 and gain * 100 >= 10:
                    logger.info('Condition 3')
                    sell(tick, unit)
                    blackList[tick] = True

    except Exception as e:
        logger.error('try trailing stop error')
        logger.error(e)

def set_budget():
    '''
    한 코인에 대해 투자할 투자 금액 계산
    :return: 원화잔고/투자 코인 수
    '''
    try:
        balances = upbit.get_balances()
        krw_balance = 0

        #logger.info('balance: %s', balances)

        for balance in balances:
            if balance['currency'] == 'KRW':
                krw_balance = float(balance['balance'])
        logger.info('krw_balance: %s', krw_balance)

        holding_count = len(balances) - 3
        logger.info('holding_count: %d', holding_count)
        logger.info('COIN_NUM: %d', COIN_NUM)

        if COIN_NUM - holding_count > 0:
            return int(krw_balance / (COIN_NUM - holding_count))
        else:
            return 0
    except Exception as e:
        logger.error('set_budget Exception occur')
        logger.error(e)
        return 0


def new_set_budget(tickers, each_volume):
    '''
    코인별 투자할 투자 금액 계산
    :return: 원화잔고 * 코인별 투자 비율
    '''
    try:
        budget_list = {}
        balances = upbit.get_balances()
        krw_balance = 0

        for balance in balances:
            if balance['currency'] == 'KRW':
                krw_balance = float(balance['balance'])
        logger.info('krw_balance: %s', krw_balance)

        for ticker in tickers:
            budget_list[ticker] = each_volume[ticker] * krw_balance

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


def update_high_prices(tickers, high_prices, cur_prices):
    '''
    모든 코인에 대해서 당일 고가를 갱신하여 저장
    :param tickers: 티커 목록 리스트
    :param high_prices: 당일 고가
    :param cur_prices: 현재가
    :return:
    '''
    try:
        for ticker in tickers:
            cur_price = cur_prices[ticker]
            high_price = high_prices[ticker]
            if cur_price > high_price:
                high_prices[ticker] = cur_price
    except:
        pass

def print_status(portfolio, prices, targets, closes):
    '''
    코인별 현재 상태를 출력
    :param tickers: 티커 리스트
    :param prices: 가격 리스트
    :param targets: 목표가 리스트
    :param high_prices: 당일 고가 리스트
    :param kvalues: k값 리스트
    :return:
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

def reset_orderlist(orderList):
    try:
        logger.info('orderlist reset() RUN')
        logger.info(orderList)
        for order in orderList:
            orderList[order] = False
    except:
        logger.error('orderList reset Exception Occur')
        pass


#----------------------------------------------------------------------------------------------------------------------
# 매매 알고리즘 시작
#---------------------------------------------------------------------------------------------------------------------
now = datetime.datetime.now()                                            # 현재 시간 조회
sell_time1, sell_time2 = make_sell_times(now)                            # 초기 매도 시간 설정
setup_time1, setup_time2 = make_setup_times(now)                         # 초기 셋업 시간 설정
portfolio_time1, portfolio_time2, portfolio_time3, portfolio_time4 = make_portfolio_today_times(now)
volume_time = make_volume_times(now)                                     # 오후 거래량 시간 설정

tickers = pyupbit.get_tickers(fiat="KRW")                                # 티커 리스트 얻기
tickers.remove('KRW-BTT')
COIN_NUM = len(tickers)
closes, targets = set_targets(tickers)                                   # 코인별 목표가 계산

volume_holdings = {ticker:False for ticker in tickers}                   # 전일 대비 거래량 기준 보유 상태 초기화
high_prices = inquiry_high_prices(tickers)                               # 코인별 당일 고가 저장
blackList = {ticker:False for ticker in tickers}                         # 한 번 익절한 코인

volume_list = {}

# upbit API의 호출 제한 때문에 2초간의 딜레이를 준다.
time.sleep(2)
volume_list, total_volume, each_volume = set_volumes(tickers)            # 전일 거래량
logger.info('volume_list: %s', volume_list)
logger.info('total_volume: %f', total_volume)
logger.info('each_volume: %s', each_volume)

budget_list = new_set_budget(tickers, each_volume)                                # 코인별 최대 배팅 금액 계산
logger.info('budget_list: %s', budget_list)

while True:

    now = datetime.datetime.now()
    schedule.run_pending()                                               # 20분 마다 블랙리스트를 초기화

    # 새로운 거래일에 대한 데이터 셋업 (09:01:00 ~ 09:01:20)
    # 금일, 익일 포함
    if (sell_time1 < now < sell_time2) or (setup_time1 < now < setup_time2):
        logger.info('새로운 거래일 데이터 셋업')

        # 시가에 매도
        try_sell(tickers)     

        setup_time1, setup_time2 = make_setup_times(now)                 # 다음 거래일 셋업 시간 갱신
        volume_time = make_volume_times(now)                             # 오후 거래량 시간 설정
        portfolio_time1, portfolio_time2, portfolio_time3, portfolio_time4 = make_portfolio_today_times(now)

        tickers = pyupbit.get_tickers(fiat="KRW")                        # 티커 리스트 얻기
        tickers.remove('KRW-BTT')
        COIN_NUM = len(tickers)
        closes, targets = set_targets(tickers)                           # 목표가 갱신

        logger.info('Targets: %s', targets)

        volume_holdings = {ticker:False for ticker in tickers}           # 전일 대비 거래량 기준 보유 상태 초기화
        high_prices = {ticker: 0 for ticker in tickers}                  # 코인별 당일 고가 초기화

        blackList = {ticker:False for ticker in tickers}                 # 한 번 익절한 코인

        volume_list = {}                                                 # 전일 대비 거래량 순위

        # upbit API의 호출 제한 때문에 2초간의 딜레이를 준다.
        time.sleep(2)
        volume_list, total_volume, each_volume = set_volumes(tickers)    # 전일 거래량
        logger.info('volume_list: %s', volume_list)
        logger.info('total_volume: %f', total_volume)
        logger.info('each_volume: %s', each_volume)

        budget_list = new_set_budget(tickers, each_volume)               # 코인별 최대 배팅 금액 계산
        logger.info('budget_list: %s', budget_list)

        logger.info('새로운 거래일 데이터 셋업 마무리')
        time.sleep(10)

    prices = get_cur_prices(tickers)                                     # 현재가 계산
    update_high_prices(tickers, high_prices, prices)                     # 고가 갱신

    portfolio = get_portfolio(tickers, prices, targets, blackList)       

    print_status(portfolio, prices, targets, closes)

    # 매수
    holdings = set_holdings(tickers)
    buy_volume(portfolio, prices, targets, holdings, budget_list, blackList, high_prices)

    time.sleep(INTERVAL)
