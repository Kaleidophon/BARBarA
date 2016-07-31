#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
This script can be used to extract information out of a specific column of a file in the
`CoNLL <http://ilk.uvt.nl/conll/>`_-format.
"""

# STANDARD
import codecs
import argparse


def main():
	"""
	The main function.
	"""
	argparser = init_argparse()
	args = argparser.parse_args()
	extract_conll(args.input, args.output, args.column)


def extract_conll(inpath, outpath, column):
	"""
	Extract information out of CoNLL files.

	Args:
		inpath (str): Path to input file.
		outpath (str): Path to output file.
		column (int): The number (-1) of the column the information should be extracted from.
	"""
	current_sentence = []

	with codecs.open(inpath, 'rb', 'utf-8') as infile:
		with codecs.open(outpath, 'wb', 'utf-8') as outfile:
			line = infile.readline()
			while line:
				if line == "\n":
					outfile.write("%s\n" % " ".join(current_sentence))
					current_sentence = []
				else:
					columns = line.strip().split("\t")
					if columns[column] != "--" and columns[column] != "-":
						current_sentence.append(columns[column])
				line = infile.readline()


def init_argparse():
	"""
	Initialize all possible arguments for the argument parser.

	Returns:
		:py:mod:`argparse.ArgumentParser`: ArgumentParser object with command line arguments for this script.
	"""
	argparser = argparse.ArgumentParser()
	argparser.add_argument('--input',
							required=True,
							help='Input data path.')
	argparser.add_argument('--output',
							required=True,
							help='Output data path.')
	argparser.add_argument('--column',
							required=True,
							type=int,
							help="Columm of CONLL format to be extracted.")
	return argparser


if __name__ == "__main__":
	main()
