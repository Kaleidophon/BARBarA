#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
This script is used to add inverse relations to a *Freebase* relations dataset, e.g.
*/location/location/contains* and */location/location/containedby*.
"""

# STANDARD
import argparse
import codecs
from collections import defaultdict


def main():
	"""
	The main function. Uses command line arguments to start the script.
	"""
	argparser = init_argparse()
	args = argparser.parse_args()
	print args
	known_relations, inverse_relations = read_file_with_inverse_relations(args.inverse)
	add_inverse_relations(args.input, args.output, inverse_relations)


def add_inverse_relations(relations_inpath, relations_outpath, inverse_relations):
	inverse_keys = inverse_relations.keys()

	with codecs.open(relations_inpath, 'rb', 'utf-8') as relations_infile:
		with codecs.open(relations_outpath, 'wb', 'utf-8') as relations_outfile:
			line = relations_infile.readline()
			while line:
				triple = tuple(line.strip().split('\t'))
				new_relation = ""

				if triple[1] in inverse_keys:
					new_relation = triple[1] + "." + inverse_relations[triple[1]]
				else:
					new_relation = triple[1]

				relations_outfile.write("%s\t%s\t%s\n" % (triple[0], new_relation, triple[2]))

				line = relations_infile.readline()


def read_file_with_inverse_relations(inverse_inpath):
	"""
	Read a *Freebase* information file where inverse relations are present and separated by a simple dot.

	Args:
		inverse_inpath (str): Path to *Freebase* relation file.

	Returns:
		defaultdict: Dictionary with a relation as a key and its inverse as value.
	"""
	inverse_relations = defaultdict(str)

	with codecs.open(inverse_inpath, 'rb', 'utf-8') as inverse_infile:
		line = inverse_infile.readline()
		while line:
			triple = tuple(line.strip().split('\t'))
			relations = triple[1].split(".")
			# if there is an inverse relation
			if len(relations) == 2:
				inverse_relations[relations[0]] = relations[1]
				inverse_relations[relations[1]] = relations[0]
			line = inverse_infile.readline()
	return inverse_relations


def init_argparse():
	"""
	Initialize all possible arguments for the argument parser.

	Returns:
		:py:mod:`argparse.ArgumentParser`: ArgumentParser object with command line arguments for this script.
	"""
	argparser = argparse.ArgumentParser()
	argparser.add_argument('--input',
							required=True,
							help='Relation input data')
	argparser.add_argument('--output',
							required=True,
							help='Output data directory')
	argparser.add_argument('--inverse',
							required=True,
							help="Path to relation data file with inverse relations.")
	return argparser


if __name__ == "__main__":
	main()