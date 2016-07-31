#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Convert the *DECOW14X* corpus into a plain text file. Is used as pre-processing step for the
`word2vec <https://code.google.com/archive/p/word2vec/>`_ training.
To make this this more feasible (decow is a **huge** corpus), python's :mod:`multiprocessing` is used, s.t. every
part of the corpus in simultaneously processed. Afterwards, a bash command like ``cat`` can be used to merge into one
single file.
"""

# STANDARD
import codecs
import gzip as gz
import multiprocessing
import optparse
import os
import re

# PROJECT
from src.misc.decorators import log_time
from src.misc.helpers import alt, contains_tag, extract_sentence_id


def main():
	"""
	Main function. Uses command lines to start corpus processing.
	"""
	optparser = optparse.OptionParser()
	optparser.add_option('--in', dest='in_dir', help='Path to input directory')
	optparser.add_option('--out', dest='out', help='Path to output directory')
	optparser.add_option('--merge', dest='merge', action="store_true", help='Merge multi-word named entities?')
	optparser.add_option('--log', dest='log', help='Path to logfile')
	optparser.add_option('--log_interval', dest='inter', type='int', help='Logging interval')
	(options, args) = optparser.parse_args()
	convert_decow_to_plain(options.in_dir, options.out, options.log, options.merge, options.inter)


def convert_decow_to_plain(decow_dir, out_dir, log_path, merge_nes, log_interval):
	"""
	Convert the whole corpus into plain text.

	Args:
		decow_dir (str): Path to directory with decow corpus paths.
		out_dir (str): Path where plain text parts should be written to.
		log_path (str): Path where the log files should be written to.
		merge_nes (bool): Flag to indicate whether multi-word expression should be merged with underscores.
		log_interval (int): Interval to log current process state in seconds.
	"""
	# Split logging interval into hourse - minutes - seconds
	m_proc, s_proc = divmod(log_interval, 60)
	h_proc, m_proc = divmod(m_proc, 60)

	# Init logfile
	with codecs.open(log_path, "a", "utf-8") as log_file:
		log_file.write(alt("Starting logging...\n"))
		log_file.write(alt("Corpus (parts) directory:\t%s\n" % decow_dir))
		log_file.write(alt("Output directory:\t\t%s\n" % out_dir))
		log_file.write(alt("Logging path:\t\t%s\n" % log_path))
		log_file.write(alt("Logging intervals:\n\t Every %2dh %2dm %2ds for metalog\n" % (h_proc, m_proc, s_proc)))

	# Start processes
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
	"""
	Convert a corpus part into plain text without merging multiple word entries.

	Args:
		argstuple: Tuple of methods arguments (``inpath`` (*str*): Path to this processes' corpus part / ``dir_outpath``
					(*str*): Path to this processes' output / ``log_path`` (*str*): Path to this processes' log / ``interval``
					(*int*): Logging interval in seconds)
	"""
	inpath, dir_outpath, log_path, interval = argstuple

	@log_time(log_path, interval)
	def _convert_part(inpath, dir_outpath):
		file_n = get_file_number(inpath)
		outpath = dir_outpath + 'decow%s_out.txt' %(str(file_n))
		with gz.open(inpath, 'rb') as infile, codecs.open(outpath, 'wb', 'utf-8') as outfile:
			sentence = []
			for line in infile:
				line = line.strip().decode("utf-8")
				if line.startswith(u'<s'):
					outfile.write('%s\n' %(' '.join(sentence)))
					sentence = []
				if not line.startswith(u'<'):
					sentence.append(line.split('\t')[0])

	_convert_part(inpath, dir_outpath)


def convert_part_merging(argstuple):
	"""
	Convert a corpus part into plain text and merging multiple word entries.

	Args:
		argstuple: Tuple of methods arguments (``inpath`` (*str*): Path to this processes' corpus part / ``dir_outpath``
					(*str*): Path to this processes' output / ``log_path`` (*str*): Path to this processes' log / ``interval``
					(*int*): Logging interval in seconds)
	"""
	inpath, dir_outpath, log_path, interval = argstuple

	@log_time(log_path, interval)
	def _convert_part_merging(inpath, dir_outpath, log_path):
		with codecs.open(log_path, "a", "utf-8") as log_file:
			process_name = multiprocessing.current_process().name
			log_file.write(alt("%s: Start logging processing of\n\t%s to \n\t%s...\n" % (process_name, inpath, dir_outpath)))
			file_n = get_file_number(inpath)
			outpath = dir_outpath + 'decow%s_out.txt' %(str(file_n))

			with gz.open(inpath, 'rb') as infile, codecs.open(outpath, 'wb', 'utf-8') as outfile:
				sentence = []
				line, lcount = infile.readline().strip().decode("utf-8"), 1

				while line != "":
					if lcount % 100000 == 0:
						log_file.write(alt("%s: Processing line nr. %i...\n" % (process_name, lcount)))

					ne = extract_named_entity(line)  # Extract possible named entity

					if line.startswith(u'<s'):
						outfile.write('%s\n' %(' '.join(sentence)))
						sentence = []
					# If there was a named entity found, try to complete it if it's a multi-word expression
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

	_convert_part_merging(inpath, dir_outpath, log_path)


def get_file_number(filename):
	"""
	Get the number of the current decow corpus part.

	Args:
		filename (str): Decow corpus part file name

	Returns:
		str: File number
	"""
	file_n = re.findall(re.compile("\d{2}(?=[^a])"), filename)  # Retrieve file number
	if len(file_n) == 0:
		file_n = re.findall(re.compile("\d+"), filename)[len(file_n) - 1]
	else:
		file_n = file_n[len(file_n) - 1]
	return file_n if int(file_n) > 9 else "0" + file_n


def extract_named_entity(line):
	"""
	Extract named entity from current line.

	Args:
		line (str): Current line

	Returns:
		str or None: Extracted named entity or None if no named entity is present.
	"""
	try:
		line_parts = line.split("\t")
		feature = line_parts[3]
		if feature != "O":
			return line_parts[2], line_parts[3]
	except IndexError:
		return None


if __name__ == '__main__':
	main()
