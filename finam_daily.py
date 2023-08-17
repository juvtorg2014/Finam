import os
import time
import pathlib
from datetime import datetime
from tickers import tickers, FINAM_URL
from urllib.parse import urlencode
from urllib.request import Request, urlopen


def delete_null(directory):
	cur_dir = pathlib.Path(directory)
	for file in cur_dir.iterdir():
		if os.stat(file).st_size == 0:
			os.remove(file)


def down_stocks(dir_new):
	start_day = datetime(1999, 1, 1).date()
	end_date = datetime.today()
	
	for stock in tickers:
		time.sleep(0.3)
		finam_parsing(dir_new, stock, start_day, end_date)
			
	delete_null(dir_new)
	
	
def finam_parsing(new_dir, ticker, start_day, end_day):
	start_day_rev = start_day.strftime('%Y%m%d')
	end_day_rev = end_day.strftime('%Y%m%d')
	params = urlencode([
		('market', 0),  # на каком рынке торгуется бумага
		('em', tickers[ticker]),  # вытягиваем цифровой символ, который соответствует бумаге.
		('code', ticker),  # тикер нашей акции
		('apply', 0),  # не нашёл что это значит.
		('df', start_day.day),  # Начальная дата, номер дня (1-31)
		('mf', start_day.month - 1),  # Начальная дата, номер месяца (0-11)
		('yf', start_day.year),  # Начальная дата, год
		('from', start_day),  # Начальная дата полностью
		('dt', end_day.day),  # Конечная дата, номер дня
		('mt', end_day.month - 1),  # Конечная дата, номер месяца
		('yt', end_day.year),  # Конечная дата, год
		('to', end_day),  # Конечная дата
		('p', 8),  # Таймфрейм
		('f', ticker + "_" + start_day_rev + "_" + end_day_rev),  # Имя сформированного файла
		('e', ".csv"),  # Расширение сформированного файла
		('cn', ticker),  # ещё раз тикер акции
		('dtf', 1),  # Формат даты. Выбор из 5. См. https://www.finam.ru/profile/moex-akcii/sberbank/export/
		('tmf', 1),  # В каком формате брать время. Выбор из 4 возможных.
		('MSOR', 0),  # Время свечи (0 - open; 1 - close)
		('mstime', "on"),  # Московское время
		('mstimever', 1),  # Коррекция часового пояса
		('sep', 3),  # Разделитель полей	(1 - запятая, 2 - точка, 3 - точка с запятой, 4 - табуляция, 5 - пробел)
		('sep2', 1),  # Разделитель разрядов
		('datf', 5),  # Формат записи в файл. Выбор из 6 возможных.
		('at', 1)])  # Нужны ли заголовки столбцов
	file_name = ticker + '_to_' + end_day_rev + '.csv'
	url = FINAM_URL + file_name + '?' + params
	print(url)
	responce = Request(url=url, headers={'User-Agent': 'Mozilla/5.0'})
	txt = urlopen(responce).readlines()
	with open(new_dir + '\\' + file_name, 'w') as f:
		for line in txt:
			f.write(line.strip().decode("utf-8") + '\n')


if __name__ == '__main__':
	new_directory = os.getcwd() + '\\' + 'MOEX'
	if not os.path.exists(new_directory):
		os.mkdir(new_directory)
	down_stocks(new_directory)
