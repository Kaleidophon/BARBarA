#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
This script is used to find all named entities in a corpus, extract them and also store their frequencies as well as
the IDs of the sentences they occur in.
"""

# STANDARD
import codecs
import gzip
import sys

# PROJECT
from src.misc.helpers import extract_sentence_id, contains_tag


def main():
	"""
	Main function.
	"""
	# Parsing command line arguments
	if len(sys.argv) != 4:
		print "Usage: <Path to input file> <Path to output file> <Path to output logpath>"
		sys.exit(0)
	inpath, outpath, logpath = sys.argv[1:]
	process(inpath, outpath, logpath)


def process(inpath, outpath, logpath):
	"""
	Starts extracting named entities and their corresponding sentence IDs.

	Args:
		inpath (str): Path to input file. Input file is a gzipped xml file.
		outpath (str): Path to output directory.
		logpath (str): Path to log directory.
	"""
	with gzip.open(inpath, "rb") as infile:
		# Preparing paths
		outpath_dictfile = outpath + inpath[inpath.rfind("/") + 1:].replace(".xml.gz", "_freqs_out.txt")
		outpath_idfile = outpath + inpath[inpath.rfind("/") + 1:].replace(".xml.gz", "_ids_out.txt")
		outpath_logfile = logpath + inpath[inpath.rfind("/") + 1:].replace(".xml.gz", "_log.txt")

		with codecs.open(outpath_logfile, "wb", "utf-8") as logfile:
			freqs = {}  # Frequencies of named entities with specific tag
			ne_ids = {}  # all named entities with sentence id
			current_sentence_id = ""
			corrupt_id = False

			line = infile.readline().strip().decode("utf-8")
			lcount = 1
			while line != "":
				if lcount % 1000000 == 0:
					# Logging
					logfile.write("Line Nr. %ik in file %s processed.\n" % (lcount / 1000, infile))

				if contains_tag(line) and "<s" in line:
					# Get current sentence ID
					current_sentence_id = extract_sentence_id(line)
					corrupt_id = [False, True][current_sentence_id is None]
				elif not contains_tag(line) and not corrupt_id:
					ne = extract_named_entity(line)
					if ne is not None:
						while True:
							next_line = infile.readline().strip().decode("utf-8")
							lcount += 1
							if not contains_tag(next_line):
								next_ne = extract_named_entity(next_line)
								if next_ne is not None and next_ne[1] == ne[1]:
									ne = ("%s %s" % (ne[0], next_ne[0]), ne[1])
								else:
									break
							else:
								break
						line = next_line

						if ne in freqs:
							freqs[ne] += 1
						else:
							freqs[ne] = 1

						# Logging sentence ids of an occurrence
						if ne in ne_ids:
							ne_ids[ne].append(current_sentence_id)
						else:
							ne_ids[ne] = [current_sentence_id]
						continue

				# Preparation for next iteration
				line = infile.readline().strip().decode("utf-8")
				lcount += 1

			# Wrapping up processs
			logfile.write("Processing of file %s complete!\n" % (infile))

			logfile.write("Writing frequencies of named entities in %s into %s...\n" % (infile, outpath_dictfile))
			write_dict_into_file(freqs, outpath_dictfile)
			logfile.write("Writing frequencies of named entities of %s into %s complete!\n" % (infile, outpath_dictfile))

			logfile.write("Writing occurrences of named entities in %s into %s...\n" % (infile, outpath_idfile))
			write_ids_into_file(ne_ids, outpath_idfile)
			logfile.write("Writing occurrendes of named entities of %s into %s complete!\n" % (infile, outpath_idfile))


def extract_named_entity(line):
	"""
	Extracts named entity from current line if present.

	Args:
		line (str): Current line

	Returns:
		tuple: Named entity in this line and its NE tag
	"""
	line_parts = line.split("\t")
	feature = line_parts[3]
	if feature != "O":
		return line_parts[0], line_parts[3]


def write_dict_into_file(dictionary, out_path):
	"""
	Write a dictionary of named entities, their tags and their frequencies into a file.

	Args:
		dictionary (dict): Dictionary with named entities as key and their frequencies as values.
		out_path (str): Path the frequencies should written to.
	"""
	with codecs.open(out_path, "wb", "utf-8") as out_file:
		for key, value in dictionary.iteritems():
			out_file.write("%s\t%s\t%s\n" % (key[0], key[1], value))


def write_ids_into_file(dictionary, out_path):
	"""
	Write a dictionary of named entities,, their tags and IDs of the sentences they occur in into a file.

	Args:
		dictionary (dict): Dictionary with named entities as key and their occurrences as a list as values.
		out_path (str): Path the frequencies should written to.
	"""
	with codecs.open(out_path, "wb", "utf-8") as out_file:
		for key, value in dictionary.iteritems():
			out_file.write("%s\t%s\n\t%s\n\n" % (key[0], key[1], "\n\t".join(value)))


if __name__ == '__main__':
	main()
