import codecs as c
import codecs
import gzip as gz
import multiprocessing
import optparse
import os
import re
import time
import threading
from functools import wraps


def main():
	optparser = optparse.OptionParser()
	optparser.add_option('--in', dest='in_dir', help='Path to input directory')
	optparser.add_option('--out', dest='out', help='Path to output directory')
	optparser.add_option('--merge', dest='merge', action="store_true", help='Merge multi-word named entities?')
	optparser.add_option('--log', dest='log', help='Path to logfile')
	optparser.add_option('--log_interval', dest='inter', type='int', help='Logging interval')
	(options, args) = optparser.parse_args()
	convert_decow_to_plain(options.in_dir, options.out, options.log, options.merge, options.inter)


def convert_decow_to_plain(decow_dir, out_dir, log_path, merge_nes, log_interval):
	m_proc, s_proc = divmod(log_interval, 60)
	h_proc, m_proc = divmod(m_proc, 60)
	with codecs.open(log_path, "a", "utf-8") as log_file:
		log_file.write(alt("Starting logging...\n"))
		log_file.write(alt("Corpus (parts) directory:\t%s\n" %(decow_dir)))
		log_file.write(alt("Output directory:\t\t%s\n" %(out_dir)))
		log_file.write(alt("Logging path:\t\t%s\n" %(log_path)))
		log_file.write(alt("Logging intervals:\n\t Every %2dh %2dm %2ds for metalog\n" %(h_proc, m_proc, s_proc)))

	@log_time(log_path, log_interval)
	def _convert_decow_to_plain(decow_dir, out_dir, log_path, merge_nes, log_interval):
		with codecs.open(log_path, "a", "utf-8") as log_file:
			log_file.write(alt("Preparing %i process(es)...\n" %(len(decow_dir))))
			inpaths = [path for path in os.listdir(decow_dir) if ".DS_Store" not in path and "decow" in path]
			pool = multiprocessing.Pool(processes=len(inpaths))
			log_file.write(alt("Starting process(es)!\n"))
			if merge_nes:
				pool.map(convert_part_merging, [(decow_dir + inpath, out_dir, log_path, log_interval) for inpath in inpaths])
			else:
				pool.map(convert_part, [(decow_dir + inpath, out_dir, log_path, log_interval) for inpath in inpaths])

	_convert_decow_to_plain(decow_dir, out_dir, log_path, merge_nes, log_interval)


def convert_part(argstuple):
	inpath, dir_outpath, log_path, interval = argstuple

	@log_time(log_path, interval)
	def _convert_part(inpath, dir_outpath, log_path, interval):
		file_n = get_file_number(inpath)
		outpath = dir_outpath + 'decow%s_out.txt' %(str(file_n))
		with gz.open(inpath, 'rb') as infile, c.open(outpath, 'wb', 'utf-8') as outfile:
			sentence = []
			for line in infile:
				line = line.strip().decode("utf-8")
				if line.startswith(u'<s'):
					outfile.write('%s\n' %(' '.join(sentence)))
					sentence = []
				if not line.startswith(u'<'):
					sentence.append(line.split('\t')[0])

	_convert_part(inpath, dir_outpath, log_path, interval)


def convert_part_merging(argstuple):
	inpath, dir_outpath, log_path, interval = argstuple

	@log_time(log_path, interval)
	def _convert_part_merging(inpath, dir_outpath, log_path, interval):
		with codecs.open(log_path, "a", "utf-8") as log_file:
			process_name = multiprocessing.current_process().name
			log_file.write(alt("%s: Start logging processing of\n\t%s to \n\t%s...\n" % (process_name, inpath, dir_outpath)))
			file_n = get_file_number(inpath)
			outpath = dir_outpath + 'decow%s_out.txt' %(str(file_n))
			with gz.open(inpath, 'rb') as infile, c.open(outpath, 'wb', 'utf-8') as outfile:
				sentence = []
				line, lcount = infile.readline().strip().decode("utf-8"), 1
				while line != "":

					if lcount % 100000 == 0:
						log_file.write(alt("%s: Processing line nr. %i...\n" % (process_name, lcount)))

					ne = extract_named_entity(line)

					if line.startswith(u'<s'):
						outfile.write('%s\n' %(' '.join(sentence)))
						sentence = []
					elif ne is not None:
						while True:
							next_line = infile.readline().strip().decode("utf-8")
							lcount += 1
							if not contains_tag(next_line):
								next_ne = extract_named_entity(next_line)
								if next_ne is not None and next_ne[1] == ne[1]:
									ne = ("%s_%s" %(ne[0], next_ne[0]), ne[1])
								else:
									break
							else:
								break
						sentence.append(ne[0])
						line = next_line
						continue
					elif not line.startswith(u'<'):
						sentence.append(line.split('\t')[0])
					line, lcount = infile.readline().strip().decode("utf-8"), lcount + 1

	_convert_part_merging(inpath, dir_outpath, log_path, interval)


def get_file_number(filename):
	file_n = re.findall(re.compile("\d{2}(?=[^a])"), filename)  # Retrieve file number
	if len(file_n) == 0:
		file_n = re.findall(re.compile("\d+"), filename)[len(file_n) - 1]
	else:
		file_n = file_n[len(file_n) - 1]
	return file_n if int(file_n) > 9 else "0" + file_n


def contains_tag(line):
	pattern = re.compile("<.+>")
	return pattern.search(line) is not None


def extract_sentence_id(tag):
	if "<s" not in tag:
		return ""
	pattern = re.compile('id="[a-z0-9]+?"(?=\s)')
	res = re.findall(pattern, tag)
	if len(res) == 0:
		return None
	return res[0].replace('"', "").replace("id=", "")


def extract_named_entity(line):
	try:
		line_parts = line.split("\t")
		feature = line_parts[3]
		if feature != "O":
			return line_parts[2], line_parts[3]
	except IndexError:
		return None


def log_time(logpath="log.txt", interval=5):

	def log_time_decorator(func):
		@wraps(func)
		def wrapper(*args, **kwargs):
			result = [None]

			def return_value(*args, **kwargs):
				result[0] = func(*args, **kwargs)

			t = threading.Thread(target=return_value, args=args, kwargs=kwargs)
			log_entries = 0
			with codecs.open(logpath, "a", "utf-8") as logfile:
				start_time = time.time()
				t.start()
				while t.is_alive():
					elapsed_time = (time.time() - start_time)
					if elapsed_time > interval * log_entries:
						m, s = divmod(elapsed_time, 60)
						h, m = divmod(m, 60)
						logfile.write(alt("Elapsed time for function '%s': %2dh %2dm %2ds\n"
						                  % (func.__name__, h, m, s)))
						log_entries += 1
			return result[0]

		return wrapper

	return log_time_decorator


def log_time_mp(logpath="log.txt", interval=5):

	def log_time_decorator(func):
		@wraps(func)
		def wrapper(*args, **kwargs):
			result = [None]

			def return_value(*args, **kwargs):
				result[0] = func(*args, **kwargs)

			t = threading.Thread(target=return_value, args=args, kwargs=kwargs)
			log_entries = 0
			with codecs.open(logpath, "a", "utf-8") as logfile:
				start_time = time.time()
				t.start()
				while t.is_alive():
					elapsed_time = (time.time() - start_time)
					if elapsed_time > interval * log_entries:
						m, s = divmod(elapsed_time, 60)
						h, m = divmod(m, 60)
						logfile.write(alt("Elapsed time for function '%s' of %s: %2dh %2dm %2ds\n"
						                  % (func.__name__, multiprocessing.current_process().name, h, m, s)))
						log_entries += 1
			return result[0]

		return wrapper

	return log_time_decorator


def alt(func):
	return "%s: %s" % (time.strftime("%H:%M:%S", time.gmtime()), func)


if __name__ == '__main__':
	main()