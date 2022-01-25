import requests
import json
import sqlite3
from datetime import datetime, timedelta

# http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020101
def crawling(date):
    headers = {
        'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36',
        'Referer':'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020101',
    }
    data = {
        'bld': 'dbms/MDC/STAT/standard/MDCSTAT01501',
        'locale': 'ko_KR',
        'mktId': 'ALL',
        'trdDd': date.strftime("%Y%m%d"),
        'share': '1',
        'money': '1',
        'csvxls_isNo': 'false'}
    response = requests.post('http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd', headers=headers, data=data)
    json_response = json.loads(response.text)

    return json_response['OutBlock_1']

def insert(cur, date, prices):
    # 개장하지 않은 날은 가격 데이터가 없음
    if prices[0]["TDD_CLSPRC"] == "-":
        return

    for row in prices:
        sql = f"""
            INSERT OR REPLACE INTO 주가
            VALUES
            (
                '{date.strftime("%Y-%m-%d")}',
                '{row["ISU_SRT_CD"]}',
                '{row["ISU_ABBRV"]}',
                '{row["MKT_NM"]}',
                '{row["SECT_TP_NM"]}',
                '{row["TDD_CLSPRC"].replace(",","")}',
                '{row["CMPPREVDD_PRC"].replace(",","")}',
                '{row["FLUC_RT"]}',
                '{row["TDD_OPNPRC"].replace(",","")}',
                '{row["TDD_HGPRC"].replace(",","")}',
                '{row["TDD_LWPRC"].replace(",","")}',
                '{row["ACC_TRDVOL"].replace(",","")}',
                '{row["ACC_TRDVAL"].replace(",","")}',
                '{row["MKTCAP"].replace(",","")}',
                '{row["LIST_SHRS"].replace(",","")}'
            )
        """
        #print(sql)
        cur.execute(sql)

# create table
con = sqlite3.connect('quant.db')
cur = con.cursor()
cur.execute('''
    CREATE TABLE IF NOT EXISTS 주가
    (
        날짜 TEXT,
        종목코드 TEXT,
        종목명 TEXT,
        시장구분 TEXT,
        소속부 TEXT,
        종가 INT,
        대비 INT,
        등락률 REAL,
        시가 INT,
        고가  INT,
        저가 INT,
        거래량 INT,
        거래대금 INT,
        시가총액 INT,
        상장주식수 INT,
        CONSTRAINT PK_주가 PRIMARY KEY (날짜, 종목코드)
    )
    ''')

# START = "2021-01-01"
# END = "2021-12-31"
# start_date = datetime.strptime(START, "%Y-%m-%d")
# end_date = datetime.strptime(END, "%Y-%m-%d")

max_date = cur.execute("SELECT MAX(날짜) FROM 주가").fetchone()
# 저장된 날짜 이후 부터
if (max_date != None):
    start_date = datetime.strptime(max_date[0], "%Y-%m-%d") + timedelta(days=1)
# 저장된 데이터가 없으면 오늘만
else:
    start_date = datetime.today()
end_date = datetime.today()

while start_date <= end_date:
    # 주말은 제외
    if start_date.isoweekday() < 6:
        print(start_date)
        prices = crawling(start_date)
        insert(cur, start_date, prices)

    start_date += timedelta(days=1)

con.commit()
con.close()