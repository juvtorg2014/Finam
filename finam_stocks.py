# Нужен запрос приблизительно такого формата
# export.finam.ru/SBER_20170101_20171231.csv?market=0&em=3
# &code=SBER&apply=0&df=1&mf=0&yf=2017
# &from=2017-01-01&dt=31&mt=11&yt=2017
# &to=2017-12-31&p=7&f=SBER_20170101_20171231
# &e=.csv&cn=\#SBER&dtf=1&tmf=1&MSOR=0
# &mstime=on&mstimever=1&sep=1&sep2=1&datf=1&at=1

from urllib.parse import urlencode
from urllib.request import Request, urlopen
from datetime import datetime
from tickers import tickers
import time

# пользовательские переменные
ticker = "GAZP"  # задаём тикер
tickers_list = []  # список тикеров
period = 2  # задаём период.
# Выбор из: 'tick': 1, 'min': 2, '5min': 3, '10min': 4, '15min': 5, '30min': 6, 'hour':
# 7, 'daily': 8, 'week': 9, 'month': 10
start = "01.01.1999"  # с какой даты начинать тянуть котировки
end = "31.12.2023"  # финальная дата, по которую тянуть котировки


def finam_parcing(ticker, period, start_day, end_day):
	periods = {'tick': 1, 'min': 2, '5min': 3, '10min': 4, '15min': 5, '30min': 6, 'hour': 7, 'd': 8, 'w': 9, 'm': 10}
	print("ticker="+ticker+"; period="+str(period)+"; start="+start+"; end="+end)
	FINAM_URL = "http://export.finam.ru/"# сервер, на который стучимся
	market = 0  # Это рынок, на котором торгуется бумага. Для акций работает с любой цифрой.
	# Делаем преобразования дат:
	start_date = datetime.strptime(start_day, "%d.%m.%Y").date()
	start_date_rev=datetime.strptime(start_day, '%d.%m.%Y').strftime('%Y%m%d')
	end_date = datetime.strptime(end_day, "%d.%m.%Y").date()
	end_date_rev=datetime.strptime(end_day, '%d.%m.%Y').strftime('%Y%m%d')
	# Все параметры упаковываем в единую структуру. Здесь есть дополнительные параметры См. комментарии внизу:
	params = urlencode([
						('market', market), #на каком рынке торгуется бумага
						('em', tickers[ticker]), #вытягиваем цифровой символ, который соответствует бумаге.
						('code', ticker), #тикер нашей акции
						('apply',0), #не нашёл что это значит.
						('df', start_date.day), # Начальная дата, номер дня (1-31)
						('mf', start_date.month - 1),  # Начальная дата, номер месяца (0-11)
						('yf', start_date.year),  # Начальная дата, год
						('from', start_date),  # Начальная дата полностью
						('dt', end_date.day),  # Конечная дата, номер дня
						('mt', end_date.month - 1),  # Конечная дата, номер месяца
						('yt', end_date.year), # Конечная дата, год
						('to', end_date), # Конечная дата
						('p', period), # Таймфрейм
						('f', ticker+"_" + start_date_rev + "_" + end_date_rev), #Имя сформированного файла
						('e', ".csv"),  #Расширение сформированного файла
						('cn', ticker), #ещё раз тикер акции
						('dtf', 1), # В каком формате брать даты. Выбор из 5 возможных. См. страницу https://www.finam.ru/profile/moex-akcii/sberbank/export/
						('tmf', 1), #В каком формате брать время. Выбор из 4 возможных.
						('MSOR', 0), #Время свечи (0 - open; 1 - close)
						('mstime', "on"), #Московское время
						('mstimever', 1), #Коррекция часового пояса
						('sep', 3), #Разделитель полей	(1 - запятая, 2 - точка, 3 - точка с запятой, 4 - табуляция, 5 - пробел)
						('sep2', 1), #Разделитель разрядов
						('datf', 5), #Формат записи в файл. Выбор из 6 возможных.
						('at', 1)]) #Нужны ли заголовки столбцов
	file_name = ticker + "_" + start_date_rev + "_" + end_date_rev + ".csv"
	url = FINAM_URL + file_name  + params #урл составлен!
	print("Стучимся на Финам по ссылке: " + url)
	responce = Request(url=url, headers={'User-Agent': 'Mozilla/5.0'})
	txt = urlopen(responce).readlines() #здесь лежит огромный массив данных, прилетевший с Финама.

	local_file = open(file_name, "w")
	try:
		for line in txt: #записываем свечи строку за строкой.
			local_file.write(line.strip().decode( "utf-8" )+'\n')
	except UnicodeDecodeError as e:
		print(e)
	finally:
		local_file.close()
	print("Готово. Проверьте файлы {}.csv в папке где лежит скрипт".format(ticker))



if __name__ == '__main__':
	tickers_watch = input("Введите тикер акций через пробел или ничего если все:\n").upper()
	if tickers_watch == '':
		new_list = list(tickers.keys())
		tickers_list = ' '.join(str(item) for item in new_list)
	else:
		tickers_list = tickers_watch
	timeframe = int(input("Введите тайм-фрейм числом:\n{1='tick', 2='min', 3='5min', 4='10min', 5='15min', 6='30min', 7='hour', 8='daily', 9='week', 10='month'}\n"))
	days_start = input("Введите начало загрузки (формат=13.01.2010):\n")
	days_end = input("Введите окончание загрузки (формат=29.02.2010):\n")
	if len(tickers_list) == 1:
		finam_parcing(tickers_list, timeframe, days_start, days_end)
	elif len(tickers_list) > 1:
		for i, word in enumerate(tickers_list.split()):
			time.sleep(0.3)
			finam_parcing(word, timeframe, days_start, days_end)
	elif len(tickers_list) == 0:
		for word in tickers:
			time.sleep(0.3)
			finam_parcing(word, timeframe, days_start, days_end)
	else:
		print("Нет данных !!!")
	print("Все скачано. Начинайте работать!")