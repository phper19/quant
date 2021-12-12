#!/usr/bin/env python3

# 재무상태표
import sqlite3
import csv
import codecs
import sys


if len(sys.argv) != 2:
    print("input xls file")
    exit(1)
fileName = sys.argv[1]
print(fileName)

con = sqlite3.connect('quant.db')
cur = con.cursor()

# create table
cur.execute('''
    CREATE TABLE IF NOT EXISTS 재무상태표
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
        당기말 INT,
        전기말 INT,
        전전기말 INT,
        CONSTRAINT PK_재무상태표 PRIMARY KEY (결산기준일, 종목코드, 항목코드)
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
            당기말 = row[12].replace(",", "")
            전기말 = row[13].replace(",", "")
            전전기말 = row[14].replace(",", "")
            query = f"""
                INSERT INTO
                    재무상태표
                VALUES
                    ('{재무제표종류}','{종목코드}','{회사명}','{시장구분}','{업종}','{업종명}','{결산월}','{결산기준일}','{보고서종류}','{통화}','{항목코드}','{항목명}','{당기말}','{전기말}','{전전기말}')
             """
            #print(query)
            cur.execute(query)
        if (line_number % 1000 == 0):
            print(f'Processed {line_number - 1} lines.')
            con.commit()
        line_number += 1

con.commit()
con.close()