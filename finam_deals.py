# Загрузка с Финама тиковых данных доступных акций и фьючерсов

from tickers import tickers, FINAM_URL
import os
import requests
from urllib.parse import urlencode
from datetime import datetime, timedelta
from fake_useragent import UserAgent
import zipfile as zf
import shutil
import pathlib
import time

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
	for item in tickers:
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
		('em', tickers[stock]),  # вытягиваем цифровой символ, который соответствует бумаге.
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
	url = FINAM_URL + file_name + '?' + params
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

		
if __name__ == '__main__':
	day_d = input('Введите дату в формате <<ДД.ММ.ГГГГ>> или ENTER для вчерашнего:\n')
	# day_d = ''
	if len(day_d) == 10:
		day_down = day_d
	else:
		yes_day = datetime.today()
		day_down = yesterday_work(yes_day)
	start_time = datetime.now()
	load_day(day_down)
	time_end = str(datetime.now() - start_time).split('.')[0]
	print("Все сделано за", time_end)
	
	