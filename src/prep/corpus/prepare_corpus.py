#!/usr/bin/python
# -*- coding: utf-8 -*-

import codecs as c
import multiprocessing
import optparse
import os
import random
import re
from collections import defaultdict

import yaml

from src.prep.misc.decorators import log_time, alt


def main():
	#Loader.add_constructor(u'tag:yaml.org,2002:str', construct_yaml_str)
	#SafeLoader.add_constructor(u'tag:yaml.org,2002:str', construct_yaml_str)
	optparser = optparse.OptionParser()
	optparser.add_option('--nes', dest='nes', help='Path to Named Entity Indexing file')
	optparser.add_option('--in', dest='inpath', help='Path to input corpus (or corpora)')
	optparser.add_option('--out', dest='outpath', help='Output path for processed corpus')
	optparser.add_option('--log', dest='logpath', help='Path to logging files')
	optparser.add_option('--intervall_meta', type='int', dest='inter_meta', help='Logging intervall for meta log', default= 950)
	optparser.add_option('--intervall_proc', type='int', dest='inter_proc', help='Logging intervall for process log', default= 450)
	(options, args) = optparser.parse_args()
	process_corpora(options.nes, options.inpath, options.outpath, options.logpath, options.inter_meta, options.inter_proc)


def construct_yaml_str(self, node):
	# Override the default string handling function
	# to always return unicode objects
	return self.construct_scalar(node)


def prepare(ne_inpath):
	with open(ne_inpath, "r") as ne_infile:
		#yaml.reader.Reader.NON_PRINTABLE = re.compile(ur'[\xc3\x00-\x08\x0b\x0c\x0e-\x1f\ud800-\udfff\ufffe\uffff]')
		nes_dict = yaml.load(ne_infile, Loader=yaml.CLoader)
		nes_indexing = defaultdict(list, nes_dict)
	return nes_indexing


def process_corpora(nesi, corpus_dir, out_dir, log_dir, logging_interval_meta=10, logging_interval_processing=10):
	logpath_meta = log_dir + "meta.log"
	with c.open(logpath_meta, "a", "utf-8") as logfile_meta:
		m_meta, s_meta = divmod(logging_interval_meta, 60)
		h_meta, m_meta = divmod(m_meta, 60)
		m_proc, s_proc = divmod(logging_interval_processing, 60)
		h_proc, m_proc = divmod(m_proc, 60)
		logfile_meta.write(alt("Starting logging...\n"))
		logfile_meta.write(alt("Parameters:\nMultiwords named_entities path:\t%s\n" %(nesi)))
		logfile_meta.write(alt("Corpus (parts) directory:\t%s\n" %(corpus_dir)))
		logfile_meta.write(alt("Output directory:\t\t%s\n" %(out_dir)))
		logfile_meta.write(alt("Logging directory:\t\t%s\n" %(log_dir)))
		logfile_meta.write(alt("Logging intervals:\n\t Every %2dh %2dm %2ds for metalog\n\t Every %2dh %2dm %2ds for processing logs\n"
						   %(h_meta, m_meta, s_meta, h_proc, m_proc, s_proc)))

		@log_time(logpath_meta, logging_interval_meta)
		def _process_corpora(nesi, corpus_dir, out_dir, log_dir, logpath_meta):
			with c.open(logpath_meta, "a", "utf-8") as logfile_meta:
				logfile_meta.write(alt("Loading multi-word named entities...\n"))
				nes_indexing_global = prepare(nesi)
				corpus_inpaths = [path for path in os.listdir(corpus_dir) if ".DS_Store" not in path and "decow" in path]
				logfile_meta.write(alt("Loading complete!\n"))
				logfile_meta.write(alt("Preparing %i processes...\n" %(len(corpus_inpaths))))
				pool = multiprocessing.Pool(processes=len(corpus_inpaths), initializer=init_pool, initargs=(nes_indexing_global,))
				logfile_meta.write(alt("Starting process(es)!\n"))
				pool.map(process_corpus, [(corpus_dir + inpath, out_dir, log_dir, logging_interval_processing) for inpath in corpus_inpaths])
				logfile_meta.write(alt("Processing complete!\n"))

		_process_corpora(nesi, corpus_dir, out_dir, log_dir, logpath_meta)


def process_corpus(argstuple):
	corpus_inpath, out_dir, log_dir, logging_interval_processing = argstuple
	file_number = get_file_number(corpus_inpath)
	corpus_outpath = out_dir + "decow%s_processed.txt" % (file_number)
	log_path = log_dir + "decow_processing.log"

	# TODO: Producer-Consumer-Konzept implementieren
	# TODO: Code reviewen, Bottlenecks identifizieren und beheben (?)

	@log_time(log_path, logging_interval_processing)
	def _process_corpus(corpus_inpath, corpus_outpath, log_path):
		process_name = multiprocessing.current_process().name
		with c.open(log_path, "a", "latin-1") as log_file:
			log_file.write(alt("%s: Start logging processing of\n\t%s to \n\t%s...\n" % (process_name, corpus_inpath, corpus_outpath)))
			with c.open(corpus_inpath, "rb", "latin-1") as corpus_infile, \
					c.open(corpus_outpath, "wb", "latin-1") as corpus_outfile:
				line_count = 1
				nes_keys = nes_indexing_global.keys()
				for line in corpus_infile:
					line = line.strip()
					if line_count % 100000 == 0:
						log_file.write(alt("%s: Processing line nr. %i..." % (process_name, line_count)))
					i = 0
					sentence = []
					tokens = line.split()

					while i < len(tokens):
						token = tokens[i]
						escaped_token = token.encode('unicode-escape')
						mwe_found = False
						if escaped_token in nes_keys: # TODO: Keys nicht immer neu erzeugen lassen
							# TODO: Satzzeichen überspringen
							for complement in nes_indexing_global[escaped_token]: # TODO: Indizierte schleife?
								try:
									# Check if complement follows
									len_complement = len(complement.split(" "))
									# TODO: Effizienteres String-formatting?
									possible_mwe = "%s %s" % (
									escaped_token, " ".join(tokens[i + 1:i + 1 + len_complement]))
									entry_mwe = "%s %s" % (escaped_token, complement)
									#log_file.write("%s == %s?\n" % (possible_mwe, entry_mwe))
									if possible_mwe == entry_mwe:
										# print "%s == %s !" %(" ".join(tokens[i+1:i+len_complement]), complement.decode('unicode-escape'))

										sentence.append(possible_mwe.replace(" ", "_").decode('unicode-escape'))
										i += len_complement
										mwe_found = True
										break
								except:
									# Index Error
									continue
							if not mwe_found:
								sentence.append(token)
						else:
							sentence.append(token)  # default case
						i += 1 # TODO: Zeilengeschwindigkeit einfügen
					if random.randint(0, 100) >= 99:
						log_file.write(alt("%s: Random sentence sample: %s\n" % (process_name, " ".join(sentence))))
					corpus_outfile.write("%s\n" % (" ".join(sentence)))
					line_count += 1

	_process_corpus(corpus_inpath, corpus_outpath, log_path)


def get_file_number(filename):
	file_n = re.findall(re.compile("\d{2}(?=[^a])"), filename)  # Retrieve file number
	if len(file_n) == 0:
		file_n = re.findall(re.compile("\d+"), filename)[len(file_n) - 1]
	else:
		file_n = file_n[len(file_n) - 1]
	return file_n if int(file_n) > 9 else "0" + file_n


def init_pool(args):
	global nes_indexing_global
	nes_indexing_global = args

if __name__ == "__main__":
	main()
