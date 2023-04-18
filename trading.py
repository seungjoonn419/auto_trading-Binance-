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


# 바이낸스 API 호출 제한
# 1,200 request weight per minute
# 50 orders pe 10 seconds
INTERVAL = 0.5                                      # API 호출 간격
DEBUG = False                                       # True: 매매 API 호출 안됨, False: 실제로 매매 API 호출
COIN_NUM = 1                                        # 분산 투자 코인 개수 (자산/COIN_NUM를 각 코인에 투자)
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


def get_cur_price(ticker):
    '''
    모든 가상화폐에 대한 현재가 조회
    param tickers: 선물 거래의 모든 종목
    :return: 현재가
    '''
    try:
        price = binance.fetch_ticker(ticker)
        cur_price = price['last']

        return cur_price
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
        return None

def set_target(ticker):
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
        target_long = today_open + (yesterday_high - yesterday_low) * LARRY_K
        target_short = today_open - (yesterday_high - yesterday_low) * LARRY_K

        # Stop Limitt 0.5%로 지정
        target_long_sl = target_long * 0.995
        target_short_sl = target_short * 1.005

        return today_open, target_long, target_short, target_long_sl, target_short_sl
    except Exception as e:
        logger.error('cal_target Exception occur')
        logger.error('ticker: %s', ticker)
        logger.error(e)

        # 절대 매수를 하지 못 하도록 높은 값을 설정
        return 100000000000000000000, 100000000000000000000


def set_volumes(tickers, holdings):
    '''
    코인들에 대한 24시간 동안 거래대금
    현재 보유 중인 포지션에 대해서는 거래대금을 0으로 한다
    '''
    volumes = {}
    each_volume = {}
    total_volume = 0

    quote_tickers = binance.fetch_tickers()

    for ticker in tickers:

        if holdings[ticker] == False:                       # 보유
            volume = quote_tickers[ticker]['quoteVolume']
            volumes[ticker] = volume
            total_volume += volume
        else:                                               # 보유하지 않음
            volumes[ticker] = 0

    for ticker in tickers:
        each_volume[ticker] = volumes[ticker]/total_volume

    return volumes, total_volume, each_volume


def get_portfolio(ticker, price, target_long, target_short):
    '''
    매수 조건 확인 및 매수 시도
    :param ticker: 코인
    :param price: 코인에 대한 현재가
    :param target_open: 코인에 대한 롱 표지션 목표가
    :param target_short: 코인에 대한 숏 포지션 목표가
    '''
    portfolio_long = []
    portfolio_short = []
    try:
        # 현재가가 롱 포지션 목표가 이상
        # 현재가가 숏 포지션 목표가 이하
        if price >= target_long:
            portfolio_long.append(ticker)
        elif price <= target_short:
            portfolio_short.append(ticker)

        return portfolio_long, portfolio_short
    except Exception as e:
        logger.error('get_portfolio Exception occur')
        logger.error(e)
        return None

def long_open(coin, price, target_long, target_long_sl, holding):
    '''
    매수 조건 확인 및 매수 시도
    '''
    try:
        if holding is False:                                        # 현재 보유하지 않은 상태
            if DEBUG is False:
                # 레버리지 설정
                market = binance.market(coin)
                leverage = 10
                resp = binance.fapiPrivate_post_leverage({
                    'symbol': market['id'],
                    'leverage': leverage
                })

                budget = set_budget(ticker)                         # 마진 계산
                order_amount = (budget/price) * leverage * 0.99     # 롱 포지션

                logger.info('----------long_open()-----------')
                logger.info('Ticker: %s', coin)
                logger.info('budget(Margin): %s', budget)
                logger.info('price: %s', price)
                logger.info('target_open: %s', target_long)
                logger.info('target_open_sl: %s', target_long_sl)
                logger.info('order_amount: %s', order_amount)

                # 시장가 주문
                for i in range(0, 20):
                    ret = binance.create_order(
                        symbol=coin,
                        type="MARKET",
                        side="buy",
                        amount=order_amount/20
                    )
                    logger.info('ret: %s', ret)
                    time.sleep(0.05)

                # 남은 margin을 모두 position open
                # 현재 남은 budget으로 계산하기 위해 값을 새로 가져온다
                budget = set_budget(ticker)                        # 마진 계산
                order_amount = (budget/price) * leverage * 0.99    # 롱 포지션
                logger.info('budget(Margin): %s', budget)
                logger.info('order_amount: %s', order_amount)
                ret = binance.create_order(
                    symbol=coin,
                    type="MARKET",
                    side="buy",
                    amount=order_amount
                )
                logger.info('ret: %s', ret)

                # stop loss 주문
                units = get_balance_unit('BTC/USDT:USDT')          # 잔고 조회
                unit = units.get(ticker, 0)              
                ret_sl = binance.create_order(
                    symbol=coin,
                    type="STOP_MARKET",
                    side="sell",
                    amount=unit,
                    params={'stopPrice': target_long_sl}
                )
                logger.info('ret_sl: %s', ret_sl)

            else:
                logger.info('BUY API CALLED: %s', coin)

        else:
            logger.info('Already have: %s', coin)
    except Exception as e:
        logger.error('long_open() Exception occur')
        logger.error(e)


def short_open(coin, price, target_short, target_short_sl, holding):
    '''
    매도 조건 확인 및 매도 시도
    '''
    try:
        if holding is False:                                            # 현재 보유하지 않은 상태
            if DEBUG is False:
                # 레버리지 설정
                market = binance.market(coin)
                leverage = 10
                resp = binance.fapiPrivate_post_leverage({
                    'symbol': market['id'],
                    'leverage': leverage
                })
                
                budget = set_budget(ticker)                             # 마진 계산
                order_amount = (budget/price) * leverage * 0.99         # 숏 포지션 

                logger.info('----------short_open()-----------')
                logger.info('Ticker: %s', coin)
                logger.info('budget(Margin): %s', budget)
                logger.info('price: %s', price)
                logger.info('target_short: %s', target_short)
                logger.info('target_short_sl: %s', target_short_sl)
                logger.info('order_amount: %s', order_amount)

                # market price
                for i in range(0, 20):
                    ret = binance.create_order(
                        symbol=coin,
                        type="MARKET",
                        side="sell",
                        amount=order_amount/20
                    )
                    logger.info('ret: %s', ret)
                    time.sleep(0.05)

                # 남은 margin을 모두 position open
                # 현재 남은 budget으로 계산하기 위해 값을 새로 가져온다
                budget = set_budget(ticker)                             # 마진 계산
                order_amount = (budget/price) * leverage * 0.99         # 숏 포지션 
                ret = binance.create_order(
                    symbol=coin,
                    type="MARKET",
                    side="sell",
                    amount=order_amount
                )
                logger.info('ret: %s', ret)

                # stop loss
                units = get_balance_unit('BTC/USDT:USDT')               # 잔고 조회
                unit = units.get(ticker, 0)              
                ret_sl = binance.create_order(
                    symbol=coin,
                    type="STOP_MARKET",
                    side="buy",
                    amount=unit,
                    params={'stopPrice': target_short_sl}
                )
                logger.info('ret_sl: %s', ret_sl)

            else:
                logger.info('BUY API CALLED: %s', coin)

        else:
            logger.info('Already have: %s', coin)
    except Exception as e:
        logger.error('short_open() Exception occur')
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
            if float(position['positionAmt']) != 0:
                length = len(position['symbol']) - 4
                unit = position['symbol'][:length] + "/USDT:USDT"
                units[unit] = float(position['positionAmt'])

        return units
    except Exception as e:
        logger.info('get_balance_unit() Exception occur')
        logger.info(e)


def close_position(ticker):
    '''
    보유하고 있는 모든 코인에 대해 전량 매도
    '''
    try:
        # 잔고조회
        units = get_balance_unit(ticker)

        logger.info('----------try_sell(tickers)---------')
        logger.info('try_sell before sell units')
        logger.info(units)

        unit = units.get(ticker, 0)                     # 보유 수량

        logger.info('ticker: ', ticker)
        logger.info('try_sell unit: ', unit)

        # 롱 포지션 정리
        if unit > 0:
            if DEBUG is False:
                logger.info('----------close long position ret-----------')
                for i in range(0,20):
                    ret = binance.create_market_sell_order(
                        symbol=ticker,
                        amount=unit/20
                    )
                    logger.info(ret)
                    time.sleep(0.05)

            else:
                logger.info('Long position close(): %s', ticker)
        # 숏 포지션 정리
        elif unit < 0:
            if DEBUG is False:
                logger.info('----------close short position ret-----------')
                for i in range(0,20):
                    ret = binance.create_market_buy_order(
                        symbol=ticker,
                        amount=-unit/20
                    )
                    logger.info(ret)
                    time.sleep(0.05)

            else:
                logger.info('Short position close(): %s', ticker)

        # 매도 이후에 잔고를 재조회하여 확인한다
        logger.info('try_sell after sell units')
        units = get_balance_unit(ticker)
        logger.info(units)

    except Exception as e:
        logger.error('try_sell Exception occur')
        logger.error(e)


def set_budget(ticker):
    '''
    투자 금액 계산
    :return: 원화잔고
    '''
    try:
        balance = binance.fetch_balance(params={"type": "future"})
        free_balance = balance['USDT']['free']

        return free_balance
    except Exception as e:
        logger.error('new_set_budget Exception occur')
        logger.error(e)
        return 0


def set_holding(ticker):
    '''
    현재 보유 중인 종목
    :return: 보유 종목 리스트
    '''
    try:
        units = get_balance_unit(ticker)                   # 잔고 조회
        holding = False        

        unit = units.get(ticker, 0)                     # 보유 수량

        if unit != 0:
            holding = True

        return holding
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

def get_filtered(ticker):
    '''
    전일 거래량을 조건으로 필터링
    '''
    try:
        df = get_df(ticker)

        # 전일 거래량이 존재
        if df.iloc[-2]['volume'] > 0:
            return True
        else:
            return False
    except Exception as e:
        logger.info('get_condition() Exception occur')
        logger.info(e)
        return False


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
                
                # 전일 거래량이 존재하는지 필터링
                filtered = get_filtered(sym)

                if filtered == True:
                    tickers.append(sym)
                else:
                    logger.info('Ticker: %s Not in tickers', sym) 

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
logger.info('---------------------------------------------------------')
now = datetime.datetime.now()                                            # 현재 시간 조회
sell_time1, sell_time2 = make_sell_times(now)                            # 초기 매도 시간 설정
setup_time1, setup_time2 = make_setup_times(now)                         # 초기 셋업 시간 설정

ticker = "BTC/USDT:USDT"

# 목표가 계산
close, target_long, target_short, target_long_sl, target_short_sl = set_target(ticker)
logger.info('Long Target: %s', target_long)
logger.info('Long sl Target: %s', target_long_sl)
logger.info('Short Target: %s', target_short)
logger.info('Short sl Target: %s', target_short_sl)

while True:

    now = datetime.datetime.now()

    # 새로운 거래일에 대한 데이터 셋업 (09:01:00 ~ 09:01:20)
    # 금일, 익일 포함
    if (sell_time1 < now < sell_time2) or (setup_time1 < now < setup_time2):
        logger.info('New Date Set Up Start')

        close_position(ticker)                                           # 포지션 정리

        setup_time1, setup_time2 = make_setup_times(now)                 # 다음 거래일 셋업 시간 갱신

        # 목표가 계산
        close, target_long, target_short, target_long_sl, target_short_sl = set_target(ticker)

        logger.info('Long Target: %s', target_long)
        logger.info('Long sl Target: %s', target_long_sl)
        logger.info('Short Target: %s', target_short)
        logger.info('Short sl Target: %s', target_short_sl)

        logger.info('New Date Set Up End')
        time.sleep(20)

    price = get_cur_price(ticker)                                        # 현재가 계산

    holding = set_holding(ticker)                                        # 현재 포지션 유무 확인
    logger.info('Is holding: %s', holding)

    portfolio_long, portfolio_short = get_portfolio(ticker, price, target_long, target_short)       
    logger.info('portfolio_long: %s', portfolio_long)
    logger.info('portfolio_short: %s', portfolio_short)

    # 롱 오픈 포지션
    for coin in portfolio_long:
        long_open(coin, price, target_long, target_long_sl, holding)

    # 숏 오픈 포지션
    for coin in portfolio_short:
        short_open(coin, price, target_short, target_short_sl, holding)

    time.sleep(INTERVAL)
