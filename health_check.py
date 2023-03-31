import psutil
import time
import subprocess
import datetime
import logging
import logging.handlers

INTERVAL = 1

# logger instance 생성
logger = logging.getLogger(__name__)

# formatter 생성
formatter = logging.Formatter('[%(asctime)s][%(levelname)s|%(filename)s:%(lineno)s] >> %(message)s')

# handler 생성 (stream, file)
streamHandler = logging.StreamHandler()
timedfilehandler = logging.handlers.TimedRotatingFileHandler(filename='healthlog', when='midnight', interval=1, encoding='utf-8')

# logger instance에 fomatter 설정
streamHandler.setFormatter(formatter)
timedfilehandler.setFormatter(formatter)
timedfilehandler.suffix = "%Y%m%d"

# logger instance에 handler 설정
logger.addHandler(streamHandler)
logger.addHandler(timedfilehandler)

# logger instnace로 log 찍기
logger.setLevel(level=logging.DEBUG)


def checkIsProcessRunning():
    for proc in psutil.process_iter():
      try:
          ps_name = proc.name()     # 프로세스 이름을 ps_name에 할당
          cmdline = proc.cmdline()  # 실행 명령어와 인자를 리스트 형식으로 가져와 cmdline에 할당

          if ps_name == "python3" and cmdline[1] == "trading.py":
              return True
      except Exception as e:
          pass
    return False

def runProcess():
    subprocess.run(["/home/ec2-user/start_trading.sh", "arguments"], shell=True)
    logger.info('프로세스를 재기동하였습니다.')

while True:

    EXIST = checkIsProcessRunning()

    if EXIST:
        logger.info('프로세스가 동작 중입니다.')
    else:
        logger.warning('프로세스가 없습니다.')
        runProcess()

    time.sleep(INTERVAL)
