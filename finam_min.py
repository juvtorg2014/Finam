# Загрузка исторических минуток акций за все время,
# потому ограничение загрузки = 1 год
import time
from tickers import tickers, years, FINAM_URL
import os
import requests
from urllib.parse import urlencode
from datetime import datetime
import pathlib
import zipfile as zf


def merge_files(curr_dir):
	text = []
	fnames = sorted(curr_dir + '\\' + fname for fname in os.listdir(curr_dir))
	for fname in fnames:
		if os.stat(fname).st_size != 0:
			with open(fname, 'r') as f:
				text.append(f.read())
	name_dir = pathlib.Path(curr_dir).stem
	name_file = curr_dir + '\\' + name_dir + '.csv'
	with open(name_file, 'w', encoding='utf-8') as fout:
		fout.writelines(text)


def zip_files(curr_dir):
	"""Сжатие созданных файлов в архив."""
	cur_dir = pathlib.Path(curr_dir)
	zip_name = os.path.join(cur_dir, cur_dir.stem + '.zip')
	for file in cur_dir.iterdir():
		if os.stat(file).st_size != 0:
			if len(file.name) < 10:
				with zf.ZipFile(zip_name, mode='w') as fout:
					fout.write(file, arcname=file.name, compress_type=zf.ZIP_DEFLATED, compresslevel=9)


def delete_files(curr_dir):
	"""Удаление файлов, которые были по годам"""
	if os.path.exists(curr_dir):
		for file in pathlib.Path(curr_dir).iterdir():
			if file.name.endswith('csv'):
				os.remove(file)


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
		('p', 2),  # Таймфрейм
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
	file_name = ticker + '_' + start_day_rev + '_' + end_day_rev + '.csv'
	url = FINAM_URL + file_name + '?' + params
	print(url)
	responce = requests.get(url=url, headers={'User-Agent': 'Mozilla/5.0'})
	if responce.status_code == 200:
		txt = responce.text
		with open(new_dir + '\\' + file_name, 'w') as f:
			for line in txt.split():
				try:
					f.write(line.strip() + '\n')
				except Exception as e:
					print(line, e)
					continue
	else:
		print(f'Не доступен {responce.url}')
		

def down_min(stocks):
	if type(stocks) is str:
		new_directory = os.getcwd() + '\\' + stocks
		if not os.path.exists(new_directory):
			os.mkdir(new_directory)
		for year in years:
			time.sleep(0.5)
			start_date = datetime(year, 1, 1).date()
			end_date = datetime(year, 12, 31).date()
			finam_parsing(new_directory, stocks, start_date, end_date)
	elif type(stocks) is list:
		for item in stocks:
			stock_dir = os.getcwd() + '\\' + item
			if not os.path.exists(stock_dir):
				os.mkdir(stock_dir)
			for year in years:
				time.sleep(0.5)
				start_date = datetime(year, 1, 1).date()
				end_date = datetime(year, 12, 31).date()
				finam_parsing(stock_dir, item, start_date, end_date)
			merge_files(stock_dir)
			zip_files(stock_dir)
			delete_files(stock_dir)

	
if __name__ == '__main__':
	stock_new = input('Введите тикеры через пробел или нажмите ENTER для всех:\n').upper()
	if stock_new == '':
		new_list = list(tickers.keys())
		stock = ' '.join(str(item) for item in new_list).split()
	else:
		if len(stock_new) == 1:
			stock = stock_new
		else:
			stock = stock_new.split()
	start_time = datetime.now()
	down_min(stock)
	time_end = str(datetime.now() - start_time).split('.')[0]
	print("Все сделано за", time_end)
	