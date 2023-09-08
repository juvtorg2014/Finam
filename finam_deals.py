# Загрузка с Финама тиковых данных доступных акций и фьючерсов
import tickers
import os
import requests
from urllib.parse import urlencode
from datetime import datetime, timedelta
from fake_useragent import UserAgent
import zipfile as zf
import shutil
import pathlib
import time
import calendar
from dateutil import parser
from holidays_ru import check_holiday


ANSWER = 'Система уже обрабатывает Ваш запрос. Дождитесь окончания обработки.'


def load_day(day_trade):
	"""Перебор тикеров и отправка на сервер загрузочных url."""
	main_dir = os.getcwd() + '\\'
	name_dir = day_trade[-4:] + day_trade[3:5] + day_trade[:2]
	new_dir = main_dir + name_dir + '\\'
	day_start = datetime.strptime(day_trade, "%d.%m.%Y").date()
	day_start_new = datetime.strptime(day_trade, '%d.%m.%Y').strftime('%Y%m%d')
	
	if not os.path.exists(new_dir):
		os.mkdir(new_dir)
		
	# Извлечение данных и сохранение в файлы
	for item in tickers.tickers:
		time.sleep(1)
		finam_parsing(new_dir, item, day_start, day_start_new)
	
	# Завершение операций: сжатие и удаление
	zip_files(new_dir)
	delete_files(new_dir)
	
	
def zip_files(curr_dir):
	"""Сжатие созданных файлов в архив."""
	cur_dir = pathlib.Path(curr_dir)
	zip_name = cur_dir.stem + '.zip'
	with zf.ZipFile(zip_name, mode='w') as fout:
		for file in cur_dir.iterdir():
			if os.stat(file).st_size != 0:
				fout.write(file, arcname=file.name, compress_type=zf.ZIP_DEFLATED, compresslevel=9)
	

def delete_files(curr_dir):
	"""Удаление файлов, которые были сжаты в архивы."""
	if os.path.exists(curr_dir):
		shutil.rmtree(curr_dir)

			
def finam_parsing(dir_new, stock, start, start_new):
	"""Парсинг и загрузка данных торгов по списку акций."""
	params = urlencode([
		('market', 0),  # на каком рынке торгуется бумага
		('em', tickers.tickers[stock]),  # вытягиваем цифровой символ, который соответствует бумаге.
		('code', stock),  # тикер нашей акции
		('apply', 0),  # не нашёл что это значит.
		('df', start.day),  # Начальная дата, номер дня (1-31)
		('mf', start.month - 1),  # Начальная дата, номер месяца (0-11)
		('yf', start.year),  # Начальная дата, год
		('from', start),  # Начальная дата полностью
		('dt', start.day),  # Конечная дата, номер дня
		('mt', start.month - 1),  # Конечная дата, номер месяца
		('yt', start.year),  # Конечная дата, год
		('to', start),  # Конечная дата
		('p', 1),  # Таймфрейм
		('f', stock + "_" + start_new),  # Имя сформированного файла
		('e', ".csv"),  # Расширение сформированного файла
		('cn', stock),  # ещё раз тикер акции
		('dtf', 1),  # Формат даты. Выбор из 5. См. https://www.finam.ru/profile/moex-akcii/sberbank/export/
		('tmf', 1),  # В каком формате брать время. Выбор из 4 возможных.
		('MSOR', 0),  # Время свечи (0 - open; 1 - close)
		('mstime', "on"),  # Московское время
		('mstimever', 1),  # Коррекция часового пояса
		('sep', 3),  # Разделитель полей (1-запятая, 2-точка, 3-точка с запятой, 4-табуляция, 5-пробел)
		('sep2', 1),  # Разделитель разрядов
		('datf', 12),  # Формат записи в файл. Выбор из 6 возможных.
		('at', 1)])  # Нужны ли заголовки столбцов
	
	file_name = stock + '_' + start_new + '.csv'
	url = tickers.FINAM_URL + file_name + '?' + params
	ua = UserAgent()
	header = {'User-Agent': str(ua.random)}
	respon = requests.get(url=url, headers=header)
	if respon.status_code == 200:
		print(respon.url)
		txt = respon.text
		if txt.strip() == ANSWER:
			print('Подождите, не сразу загружается', stock)
			time.sleep(5)
			while True:
				time.sleep(5)
				respon == requests.get(url=url, headers={'User-Agent': str(ua.random)})
				if respon.status_code == 200:
					deals = respon.text
					if len(deals) > len(ANSWER) + 1:
						break
		else:
			deals = txt
		with open(dir_new + file_name, 'w') as f:
			for line in deals.split():
				try:
					f.write(line.strip() + '\n')
				except Exception as e:
					print(line, e)
					continue
	else:
		print(f'Не доступен {respon.url}')
		
		
def yesterday_work(tek_day) -> str:
	if tek_day.weekday() == 0:
		last_day = tek_day - timedelta(3)
		return last_day.strftime('%d.%m.%Y')
	elif tek_day.weekday() == 7:
		last_day = tek_day - timedelta(2)
		return last_day.strftime('%d.%m.%Y')
	else:
		last_day = tek_day - timedelta(1)
		return last_day.strftime('%d.%m.%Y')
	

def make_month(my_date) -> tuple:
	month = my_date.split(' ')[0]
	month = '0' + month if len(month) == 1 else month
	year = '20' + my_date.split(' ')[1]
	first = '01.' + month + '.' + year
	last_day = calendar.monthrange(int(year), int(month))[1]
	second = str(last_day) + first[2:]
	return first, second
	
	
def make_list_days(first, second) -> list:
	days = [d for d in range(first, second + 1)]
	weeks = [days[i:i + 7] for i in range(0, len(days), 7)]
	work_weeks = [week[0:5] for week in weeks]
	list_days = sum(work_weeks, [])
	return list_days

	
def day_delta(first, last) -> list:
	""" Формирует список дат, которые нужно загрузить """
	list_days = []
	first_day = datetime.strptime(first, "%d.%m.%Y").date()
	second_day = datetime.strptime(last, "%d.%m.%Y").date()
	while first_day <= second_day:
		if not check_holiday(first_day):
			list_days.append(first_day.strftime("%d.%m.%Y"))
		first_day += timedelta(days=1)
	return list_days


def choose_days(first_day, last_day) -> tuple:
	""" Формирует даты в нужном формате из введенных пользователем """
	day_1 = parser.parse(first_day).date()
	day_2 = parser.parse(last_day).date()
	day_1 = day_1.strftime("%d.%m.%Y")
	day_2 = day_2.strftime("%d.%m.%Y")
	return day_1, day_2
	
	
if __name__ == '__main__':
	choise = input('Введите дату в формате <<ММ.ДД.ГГ>> или номер месяца с годом через пробел'
				   'в формате <3 23>\n или диапозон дат в формате <<ДД.ММ.ГГ-ДД.ММ.ГГ> \n')
	start_time = datetime.now()
	if '-' in choise:
		argv = choise.split('-')
		first, second = choose_days(argv[0], argv[1])
		day_list = day_delta(first, second)
		for day in day_list:
			#load_day(day)
			print(day)
	elif ' ' in choise:
		first, second = make_month(choise)
		day_list = day_delta(first, second)
		for day in day_list:
			#load_day(day)
			print(day)
	else:
		day = parser.parse(choise).date()
		#load_day(day)
		print(day)
	print("Все сделано за", str(datetime.now() - start_time).split('.')[0])
	
	