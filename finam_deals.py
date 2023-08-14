# Загрузка с Финама тиковых данных доступных акций и фьючерсов

from tickers import tickers, FINAM_URL
import os
from urllib.request import Request, urlopen
from urllib.parse import urlencode
from datetime import datetime
import zipfile as zf
import shutil
import pathlib
import time


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
		time.sleep(0.3)
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
	print(url)
	responce = Request(url=url, headers={'User-Agent': 'Mozilla/5.0'})
	txt = urlopen(responce).readlines()
	with open(dir_new + file_name, 'w') as f:
		for line in txt:
			f.write(line.strip().decode("utf-8") + '\n')


if __name__ == '__main__':
	day_down = input('Введите день загрузки в формате <<ДД.ММ.ГГГГ>> :\n')
	# day_down = '11.08.2023'
	load_day(day_down)
	