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
import slack_bot

# 바이낸스 API 호출 제한
# 1,200 request weight per minute
INTERVAL = 0.15                                     # API 호출 간격
DEBUG = False                                       # True: 매매 API 호출 안됨, False: 실제로 매매 API 호출
COIN_NUM = 10                                       # 분산 투자 코인 개수 (자산/COIN_NUM를 각 코인에 투자)
LARRY_K = 0.5
LEVERAGE = 4


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

# Load Slcak Token
with open("slack_token.txt") as f:
    lines = f.readlines()
    token = lines[0].strip()

# Slack 초기화
def slack_init():
    try:
        channel_name = "자동매매"
        slack = slack_bot.SlackAPI(token)
        channel_id = slack.get_channel_id(channel_name)
        return slack, channel_id
    except Exception as e:
        logger.info('slack_init() Exception occur: %s', e)

# 슬랙 메시지 전송
def post_message(slack, channel_id, ticker, msg):
    try:
        message = ticker + ': ' + msg
        slack.post_message(channel_id, message)
    except Exception as e:
        logger.info('post_message() Exception occur: %s', e)


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
        cur_prices = {}
        for ticker in tickers:
            cur_price = binance.fetch_ticker(ticker)
            cur_prices[ticker] = cur_price['last']

        return cur_prices
    except Exception as e:
        logger.info('get_cur_prices() Exception occur: %s', e)
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
        logger.error('get_df() Exception occur: %s', e)
        return None

def cal_target(ticker):
    '''
    각 코인에 대한 목표가 계산
    '''
    try:
        df = get_df(ticker)

        yesterday = df.iloc[-2]
        today_open = yesterday['close']
        yesterday_high = yesterday['high']
        yesterday_low = yesterday['low']
        target_long = today_open + (yesterday_high - yesterday_low) * LARRY_K
        target_short = today_open - (yesterday_high - yesterday_low) * LARRY_K

        return today_open, target_long, target_short
    except Exception as e:
        logger.error('cal_target Exception occur')
        logger.error('ticker: %s', ticker)
        logger.error(e)

        # 절대 매수를 하지 못 하도록 높은 값을 설정
        return float("inf"), float("inf"), float("-inf")

def set_target(tickers):
    '''
    각 코인에 대한 목표가 저장
    :param ticker: 티커, 
    '''
    try:
        closes = {}
        target_longs = {}
        target_shorts = {}

        for ticker in tickers:
            closes[ticker], target_longs[ticker], target_shorts[ticker] = cal_target(ticker)
            time.sleep(INTERVAL)
        return closes, target_longs, target_shorts

    except Exception as e:
        logger.error('set_target Exception occur')
        logger.error('ticker: %s', ticker)
        logger.error(e)

        return float("inf"), float("inf"), float("-inf")


def get_portfolio(tickers, prices, target_longs, target_shorts):
    '''
    매수 조건 확인 및 매수 시도
    :param tickers: 코인 리스트
    :param prices: 각 코인에 대한 현재가
    :param target_opens: 각 코인에 대한 롱 표지션 목표가
    :param target_shorts: 각 코인에 대한 숏 포지션 목표가
    '''
    try:
        portfolio_long = []
        portfolio_short = []

        for ticker in tickers:
            price = prices[ticker]                # 현재가
            target_long = target_longs[ticker]    # 롱 포지션 목표가
            target_short = target_shorts[ticker]  # 숏 포지션 목표가

            # 현재가가 롱 포지션 목표가 이상
            # 현재가가 숏 포지션 목표가 이하
            if price >= target_long:
                portfolio_long.append(ticker)
            elif price <= target_short:
                portfolio_short.append(ticker)

        return portfolio_long, portfolio_short
    except Exception as e:
        logger.error('get_portfolio Exception occur: %s', e)
        return None


def create_order_long(ticker, order_amount):
    try:
        ret = binance.create_order(
            symbol=ticker,
            type="MARKET",
            side="buy",
            amount=order_amount
        )
        return ret
    except Exception as e:
        logger.info('create_order_long() Exception occur: %s', e)
        return None


def create_order_sell_sl(ticker, unit, target_sell_sl):
    try:
        ret_sl = binance.create_order(
            symbol=ticker,
            type="STOP_MARKET",
            side="sell",
            amount=unit,
            params={'stopPrice': target_sell_sl}
        )
        return ret_sl
    except Exception as e:
        logger.info('create_order_sell_sl() Exception occur: %s', e)
        return None

def create_order_sell_tp(ticker, unit, target_sell_tp):
    try:
        ret_sl = binance.create_order(
            symbol=ticker,
            type="TAKE_PROFIT_MARKET",
            side="sell",
            amount=unit,
            params={'stopPrice': target_sell_tp}
        )
        return ret_sl
    except Exception as e:
        logger.info('create_order_sell_tp() Exception occur: %s', e)
        return None

def set_budget():
    '''
    현재 잔고 계산
    '''
    try:
        balance = binance.fetch_balance(params={"type": "future"})
        free_balance = balance['USDT']['free']

        return free_balance
    except Exception as e:
        logger.error('set_budget Exception occur: %s', e)
        return 0

def set_margin(holdings):
    '''
    투자 금액 계산
    '''
    try:
        free = set_budget()
        holded = sum(holdings.values())

        logger.info('free: %s', free)
        logger.info('holded: %s', holded)
        return free / (COIN_NUM - holded)
    except Exception as e:
        logger.error('set_margin Exception occur: %s', e)
        return 0

    

def long_open(ticker, price, target_long, holding, margin, slack, channel_id):
    '''
    매수 조건 확인 및 매수 시도
    '''
    try:
        if holding is False:                                          # 현재 보유하지 않은 상태

            # 레버리지 설정
            market = binance.market(ticker)
            resp = binance.fapiPrivate_post_leverage({
                'symbol': market['id'],
                'leverage': LEVERAGE
            })

            order_amount = (margin/price) * LEVERAGE * 0.99           # 롱 포지션 

            logger.info('----------long_open()-----------')
            logger.info('Ticker: %s', ticker)
            logger.info('price: %s', price)
            logger.info('target_open: %s', target_long)
            logger.info('order_amount: %s', order_amount)

            # Slack message 전송
            post_message(slack, channel_id, ticker, "Long Open")     
            post_message(slack, channel_id, "price", str(price))   
            post_message(slack, channel_id, "target price", str(target_long))   
            post_message(slack, channel_id, "Budget", str(budget))   

            # 시장가 주문
            '''
            for i in range(0, 20):
                ret = create_order_long(ticker, order_amount/20)
                logger.info('ret: %s', ret)
                time.sleep(0.05)
            '''
            ret = create_order_long(ticker, order_amount)
            logger.info('ret: %s', ret)

    except Exception as e:
        logger.error('long_open() Exception occur')
        logger.error(e)


def create_order_short(ticker, order_amount):
    try:
        ret = binance.create_order(
            symbol=ticker,
            type="MARKET",
            side="sell",
            amount=order_amount
        )
        return ret
    except Exception as e:
        logger.info('create_order_short() Exception occur: %s', e)
        return None


def create_order_buy_sl(ticker, unit, target_buy_sl):
    try:
        ret_sl = binance.create_order(
            symbol=ticker,
            type="STOP_MARKET",
            side="buy",
            amount=unit,
            params={'stopPrice': target_buy_sl}
        )
        return ret_sl
    except Exception as e:
        logger.info('create_order_buy_sl() Exception occur: %s', e)
        return None


def create_order_buy_tp(ticker, unit, target_buy_tp):
    try:
        ret_sl = binance.create_order(
            symbol=ticker,
            type="TAKE_PROFIT_MARKET",
            side="buy",
            amount=unit,
            params={'stopPrice': target_buy_tp}
        )
        return ret_sl
    except Exception as e:
        logger.info('create_order_buy_tp() Exception occur: %s', e)
        return None


def short_open(ticker, price, target_short, holding, margin, slack, channel_id):
    '''
    매도 조건 확인 및 매도 시도
    '''
    try:
        if holding is False:

            # 레버리지 설정
            market = binance.market(ticker)
            resp = binance.fapiPrivate_post_leverage({
                'symbol': market['id'],
                'leverage': LEVERAGE
            })
 
            order_amount = (margin/price) * LEVERAGE * 0.99          # 숏 포지션 

            logger.info('----------short_open()-----------')
            logger.info('Ticker: %s', ticker)
            logger.info('price: %s', price)
            logger.info('target_short: %s', target_short)
            logger.info('order_amount: %s', order_amount)

            # Slack message 전송
            post_message(slack, channel_id, ticker, "Short Open")     
            post_message(slack, channel_id, "price", str(price))   
            post_message(slack, channel_id, "target price", str(target_short))   
            post_message(slack, channel_id, "Budget", str(budget))   

            # 시장가 주문
            '''
            for i in range(0, 20):
                ret = create_order_short(ticker, order_amount/20)
                logger.info('ret: %s', ret)
                time.sleep(0.05)
            '''
            ret = create_order_short(ticker, order_amount)
            logger.info('ret: %s', ret)

    except Exception as e:
        logger.error('short_open() Exception occur: %s', e)


def get_balance_unit():
    '''
    잔고 조회
    '''
    try:
        balance = binance.fetch_balance()
        positions = balance['info']['positions']
        units = {}

        for position in positions:
            if float(position['positionAmt']) != 0:
                unit = position['symbol']
                units[unit] = float(position['positionAmt'])

        return units
    except Exception as e:
        logger.info('get_balance_unit() Exception occur')
        logger.info(e)
        return {}


def close_position(tickers):
    '''
    보유하고 있는 모든 코인에 대해 전량 매도
    '''
    try:
        # 잔고조회
        units = get_balance_unit()

        logger.info('----------try_sell(tickers)---------')
        logger.info('try_sell before sell units')
        logger.info(units)

        for ticker in tickers:
            unit = units.get(ticker, 0)                     # 보유 수량

            logger.info('ticker: ', ticker)
            logger.info('try_sell unit: ', unit)

            # 롱 포지션 정리
            if unit > 0:
                if DEBUG is False:
                    logger.info('----------close long position ret-----------')
                    '''
                    # 20번에 나누어서 시장가로 포지션을 Close
                    for i in range(0,20):
                        ret = binance.create_market_sell_order(
                            symbol=ticker,
                            amount=unit/20
                        )
                        logger.info(ret)
                        time.sleep(0.05)
                    '''
                    ret = binance.create_market_sell_order(
                        symbol=ticker,
                        amount=unit
                    )
                    logger.info(ret)

                    # 남은 포지션을 추가로 확인하여 포지션을 Close
                    units = get_balance_unit()
                    unit = units.get(ticker, 0)
                    if unit > 0:
                        ret = binance.create_market_sell_order(
                            symbol=ticker,
                            amount=unit
                        )
                        logger.info(ret)
                else:
                    logger.info('Long position close(): %s', ticker)

            # 숏 포지션 정리
            elif unit < 0:
                if DEBUG is False:
                    logger.info('----------close short position ret-----------')
                    '''
                    for i in range(0,20):
                        ret = binance.create_market_buy_order(
                            symbol=ticker,
                            amount=-unit/20
                        )
                        logger.info(ret)
                        time.sleep(0.05)
                    '''
                    ret = binance.create_market_buy_order(
                        symbol=ticker,
                        amount=-unit
                    )
                    logger.info(ret)

                    # 남은 포지션을 추가로 확인하여 포지션을 Close
                    units = get_balance_unit()
                    unit = units.get(ticker, 0)
                    if unit < 0:
                        ret = binance.create_market_sell_order(
                            symbol=ticker,
                            amount=unit
                        )
                        logger.info(ret)
                else:
                    logger.info('Short position close(): %s', ticker)

        # 매도 이후에 잔고를 재조회하여 확인한다
        logger.info('try_sell after sell units')
        units = get_balance_unit()
        logger.info(units)

    except Exception as e:
        logger.error('try_sell Exception occur')
        logger.error(e)


def get_budget():
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

def set_holdings(tickers):
    '''
    현재 보유 중인 종목
    :return: 보유 종목 리스트
    '''
    try:
        units = get_balance_unit()                          # 잔고 조회
        holdings = {ticker:False for ticker in tickers}

        for ticker in tickers:
            unit = units.get(ticker, 0)                     # 보유 수량

            if unit != 0:
                holdings[ticker] = True

        return holdings
    except Exception as e:
        logger.error('set_holdings() Exception error: %s', e)

def set_marginType(ticker):
    '''
    마진을 Isolated로 설정
    '''
    try:
        ret = binance.fapiPrivate_post_margintype({'symbol': ticker, 'marginType': 'ISOLATED'})
        logger.info(ret)
    except Exception as e:
        logger.info('set_marginType() Exception occur :%s', e)

def get_quoteVolume(slack, channel_id):
    '''
    24시간의 거래대금 순위
    '''
    try:
        url = 'https://fapi.binance.com/fapi/v1/ticker/24hr'

        response = requests.get(url)
        data = response.json()

        volume_list = []

        for item in data:
            symbol = item['symbol']
            volume = float(item['quoteVolume'])
            if symbol.endswith('USDT'):
                volume_list.append((symbol, volume))

        volume_list.sort(key=lambda x: x[1], reverse=True)

        top_volume_list = volume_list[:COIN_NUM]
        tickers = [item[0] for item in top_volume_list]

        # Slack message 전송
        post_message(slack, channel_id, "Tickers", str(tickers))   

        return tickers

    except Exception as e:
        logger.info('get_quoteVolume() Exception occur: %s', e)
        return []
    

#----------------------------------------------------------------------------------------------------------------------
# 매매 알고리즘 시작
#---------------------------------------------------------------------------------------------------------------------
logger.info('---------------------------------------------------------')
now = datetime.datetime.now()                                            # 현재 시간 조회
sell_time1, sell_time2 = make_sell_times(now)                            # 초기 매도 시간 설정
setup_time1, setup_time2 = make_setup_times(now)                         # 초기 셋업 시간 설정

slack, channel_id = slack_init()

tickers = get_quoteVolume(slack, channel_id)

# 목표가 계산
close, target_long, target_short = set_target(tickers)
logger.info('Long Target: %s', target_long)
logger.info('Short Target: %s', target_short)

budget = get_budget()


while True:

    now = datetime.datetime.now()

    # 새로운 거래일에 대한 데이터 셋업 (09:01:00 ~ 09:01:20)
    # 금일, 익일 포함
    if (sell_time1 < now < sell_time2) or (setup_time1 < now < setup_time2):
        logger.info('New Date Set Up Start')

        close_position(tickers)                                          # 포지션 정리

        setup_time1, setup_time2 = make_setup_times(now)                 # 다음 거래일 셋업 시간 갱신

        tickers = get_quoteVolume(slack, channel_id)

        # 목표가 계산
        close, target_long, target_short = set_target(tickers)

        logger.info('Long Target: %s', target_long)
        logger.info('Short Target: %s', target_short)

        logger.info('New Date Set Up End')
        time.sleep(20)

    price = get_cur_price(tickers)                                       # 현재가 계산
    logger.info('Price: %s', price)

    holdings = set_holdings(tickers)                                     # 현재 포지션 유무 확인

    margin = set_margin(holdings)                                        # 마진 계산

    portfolio_long, portfolio_short = get_portfolio(tickers, price, target_long, target_short)       
    logger.info('portfolio_long: %s', portfolio_long)
    logger.info('portfolio_short: %s', portfolio_short)

    # 롱 오픈 포지션
    for coin in portfolio_long:
        long_open(coin, price[coin], target_long[coin], holdings[coin], margin, slack, channel_id)

    # 숏 오픈 포지션
    for coin in portfolio_short:
        short_open(coin, price[coin], target_short[coin], holdings[coin], margin, slack, channel_id)

    time.sleep(INTERVAL)
