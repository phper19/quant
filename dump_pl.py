#!/usr/bin/env python3

# 손익계산서 https://opendart.fss.or.kr/disclosureinfo/fnltt/dwld/main.do
import sqlite3
import csv
import codecs
import sys


if len(sys.argv) != 2:
    print("input fl file")
    exit(1)
fileName = sys.argv[1]
print(fileName)

if fileName.find("손익계산서") < 0:
    print("wrong file name")
    exit(1)

con = sqlite3.connect('quant.db')
cur = con.cursor()

# TODO 사업 보고서는 파일 컬럼이 다름. 누적분만 있어서 전기하고 계산해야함
# create table
cur.execute('''
    CREATE TABLE IF NOT EXISTS 손익계산서
    (
        재무제표종류 TEXT,
        종목코드 TEXT,
        회사명 TEXT,
        시장구분 TEXT,
        업종 TEXT,
        업종명 TEXT,
        결산월 TEXT,
        결산기준일 TEXT,
        보고서종류 TEXT,
        통화  TEXT,
        항목코드 TEXT,
        항목명 TEXT,
        당기 INT,
        당기누적 INT,
        전기 INT,
        전기누적 INT,
        CONSTRAINT PK_손익계산서 PRIMARY KEY (결산기준일, 종목코드, 항목코드)
    )
    ''')

# file to sqlite
with codecs.open(fileName, 'r', 'euc-kr') as file:
    csv_reader = csv.reader(file, delimiter='\t')
    line_number = 0
    for row in csv_reader:
        if line_number > 0:
            재무제표종류 = row[0]
            종목코드 = row[1].replace("[", "").replace("]","")
            회사명 = row[2]
            시장구분 = row[3]
            업종 = row[4]
            업종명 = row[5]
            결산월 = row[6]
            결산기준일 = row[7]
            보고서종류 = row[8]
            통화 = row[9]
            항목코드 = row[10]
            항목명 = row[11].replace("'", "")
            당기 = row[12].replace(",", "")
            당기누적 = row[13].replace(",", "")
            전기 = row[14].replace(",", "")
            전기누적 = row[15].replace(",", "")
            query = f"""
                INSERT INTO
                    손익계산서
                VALUES
                    ('{재무제표종류}','{종목코드}','{회사명}','{시장구분}','{업종}','{업종명}','{결산월}','{결산기준일}','{보고서종류}','{통화}','{항목코드}','{항목명}','{당기}','{당기누적}','{전기}', '{전기누적}')
             """
            #print(query)
            cur.execute(query)
        if (line_number % 1000 == 0):
            print(f'Processed {line_number - 1} lines.')
            con.commit()
        line_number += 1

con.commit()
con.close()