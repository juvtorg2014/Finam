import os
import zipfile


def zip_files(curr_dir):
	for file in os.listdir(curr_dir):
		if os.path.isfile(file):
			# f = os.path.join(curr_dir, file)
			if file.endswith('csv') and os.stat(file).st_size != 0:
				zip_name = file.replace('.csv', '.zip')
				f_zip = zipfile.ZipFile(zip_name, 'w')
				f_zip.write(file, compress_type=zipfile.ZIP_DEFLATED)
				f_zip.close()
				

def delete_files(curr_dir):
	for file in os.listdir(curr_dir):
		if os.path.isfile(curr_dir + '//' + file):
			if file.endswith('.csv'):
				f = os.path.join(curr_dir, file)
				os.remove(f)


if __name__ == '__main__':
	dir_new = 'D://PythonProjects//Course//Finam_Download//1'
	# zip_files(dir_new)
	delete_files(dir_new)
	