import os
import mysql.connector
import datetime
import zipfile as zf
from tickers import tickers

NAME_SQL = "python_example"

def connect_sql(name_sql):
	try:
		conn = mysql.connector.connect(
			host="localhost",
			port="3306",
			user="root",
			password="",
			database=name_sql)
		return conn
	except Exception as e:
		print(e)
		return 0
	
	

def make_mysql(name_sql):
	sql = "CREATE DATABASE python_example"
	sql_2 = " (date DATE, time TIME, id CHAR(15), last FLOAT, vol INT, oper CHAR(1))"
	sql_1 = "CREATE TABLE IF NOT EXISTS "
	try:
		conn = mysql.connector.connect(
			host="localhost",
			port="3306",
			user="root",
			password="",
			database=name_sql)
		
		myCur = conn.cursor()
		# myCur.execute(sql)
		# conn.commit()
		
		# Создание таблиц
		for stock in tickers:
			exec_req = sql_1 + stock + sql_2
			print(exec_req)
			myCur.execute(exec_req)
		conn.commit()
		#
		# Просмотр таблиц
		myCur.execute("SHOW TABLES")
		for el in myCur:
			print(el)
			
		# sql = sql_3 + 'abrd' + sql_4
		# print(sql)
		# articles = [('20230502', '095951', '260.500000000', '30', '7598975605', 'S'),
		# 			('20230502', '100002', '261', '50', '7598975606', 'B'),]
		# for article in articles:
		# 	print(article)
		# 	myCur.execute(sql, article)
		conn.commit()
	except EOFError:
		print(EOFError)
	finally:
		conn.close()
		

def write_sql(directory, name_sql):
	sql_1 = "INSERT INTO "
	sql_2 = " (date, time, id, last, vol, oper) VALUES(%s, %s, %s, %s, %s, %s)"
	myBase = connect_sql(name_sql)
	myCur = myBase.cursor()
	
	for file in os.listdir(directory):
		f = os.path.join(directory, file)
		if f.endswith("zip") and len(file) == 12:
			with zf.ZipFile(f, 'r') as fzip:
				for item in fzip.namelist():
					content = fzip.read(item)
					list_all = content.split()
					stock = item.split('_')[0].lower()
					print(stock)
					for line in list_all:
						sql = sql_1 + stock + sql_2
						str_line = line.decode('utf-8').split(';')
						new_tutple = ['0', '0', '0', '0', '0', '0']
						if str_line[0] != '<DATE>' and len(str_line) == 6:
							dat, tim, last, vol, id, oper = str_line
							new_tutple[0] = dat[:4] + '-' + dat[4:6] + '-' + dat[-2:]
							new_tutple[1] = tim[:2] + ':' + tim[2:4] + ':' + tim[-2:]
							new_tutple[2] = id
							new_tutple[3] = float(last)
							new_tutple[4] = int(vol)
							new_tutple[5] = oper
							entities = tuple(new_tutple)
							try:
								myCur.execute(sql, entities)
							except Exception as e:
								print(f'Что-то не так у {stock} с записью {entities}')
								continue
				myBase.commit()
	myBase.close()
	
	
def modify_sql(name_sql):
	sql_1 = 'ALTER TABLE '
	sql_2 = ' MODIFY COLUMN `id` VARCHAR(15)'
	myBase = connect_sql(name_sql)
	myCur = myBase.cursor()
	for stock in tickers:
		sql = sql_1 + stock + sql_2
		myCur.execute(sql)
		myBase.commit()
		print(sql)
	myBase.close()
	
	
def delete_tables_sql(name_sql):
	myBase = connect_sql(name_sql)
	myCur = myBase.cursor()
	for stock in tickers:
		sql = 'TRUNCATE ' + stock
		myCur.execute(sql)
		myBase.commit()
		print(sql)
	myBase.close()


if __name__ == '__main__':
	direct = os.getcwd()
	#make_mysql(NAME_SQL)
	write_sql(direct, NAME_SQL)
	#modify_sql(NAME_SQL)
	#delete_tables_sql(NAME_SQL)