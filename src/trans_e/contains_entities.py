#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
This script analyses entities in the *Freebase* ``FB14k`` relations datatset and the tql wikidata dump.
This is handy because the *Freebase* API is deprecated nowadays. Also, this scripted was used to create the ``GER14k``
dataset.
"""

# STANDARD
import argparse
import codecs
import re

# PROJECT
from src.misc.helpers import format_fbid, read_dataset


def main():
	"""
	Main function.
	"""
	argparser = init_argparse()
	args = argparser.parse_args()
	print args
	entities1 = extract_entities_from_tql_file(args.entities)
	entities2, dataset = extract_entities_from_relation_dataset(args.relations)

	contains_entities(entities1, entities2)
	decision = True if raw_input("Do you wish to create a new relation dataset? (y/n) ").strip() == "y" else False
	if decision:
		outpath = raw_input("Please enter output path for new dataset: ").strip()
		create_new_dataset(entities1, dataset, outpath)


def contains_entities(entities1, entities2):
	"""
	Prints stats about two sets of entities.

	Args:
		entities1 (set): First set of entities.
		entities2 (set): Second set of entities.
	"""
	print "Entities in set 1: %i" % len(entities1)
	print "Entities in set 2: %i" % len(entities2)
	print "The sets contains %i shared entities." % len(entities1.intersection(entities2))


def create_new_dataset(entities1, dataset, outpath):
	"""
	Write a new dataset only with relations which entities appear in a specific set.

	Args:
		entities1 (set): Set entities in relations have to appear in.
		dataset (list): Original dataset (a list of tuples).
		outpath (str): Path to new dataset.
	"""
	with codecs.open(outpath, 'wb', 'utf-8') as outfile:
		for triple in dataset:
			if triple[0] in entities1 and triple[2] in entities1:
				outfile.write("%s\t%s\t%s\n" %(triple[0], triple[1], triple[2]))


def extract_entities_from_tql_file(tql_path):
	"""
	Extract all entities from the ``tql`` *Wikidata* *Freebase* dump.

	Args:
		tql_path (str): Path to ``tql`` file.

	Returns:
		set: Set of entities in the ``tql`` dump.
	"""
	print "Read entities from wikidata dump..."
	entities = set()

	with codecs.open(tql_path, 'rb', 'utf-8') as tql_infile:
		line = tql_infile.readline().strip()
		while line:
			parts = re.findall(r"<.+?>", line)

			if len(parts) > 3:
				raw_fb_entity = re.findall(r"/(m\..+)>", parts[2])[0]
				fb_entitiy = format_fbid(raw_fb_entity)
				entities.add(fb_entitiy)

			line = tql_infile.readline().strip()

	return entities


def extract_entities_from_relation_dataset(dataset_inpath):
	"""
	Extract all entities from the *Freebase* relations file.

	Args:
		dataset_inpath (str): Path to the *Freebase* file.

	Returns:
		set: Set of entities in the *Freebase* relations file..
	"""
	print "Read entities from relation datatset..."
	entities = set()
	dataset = read_dataset(dataset_inpath)

	for triple in dataset:
		entities.add(triple[0])
		entities.add(triple[1])

	return entities, dataset


def init_argparse():
	argparser = argparse.ArgumentParser()
	argparser.add_argument('--entities',
							help='Relation input dataset 1.')
	argparser.add_argument('--relations',
							help='Relation input dataset 2.')
	return argparser


if __name__ == "__main__":
	main()
