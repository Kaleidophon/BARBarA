#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
This script partition the data of a relation dataset like ``FB15k`` into a training, validation and test set so it
can be used by `TransE <https://github.com/glorotxa/SME>`_.
To make sure that no relation appears in the validation or test set that didn't appear in the training set, data
will be partitioned relation-wise. To partition them intuitively is still an option, though.
"""

# STANDARD
import argparse
import codecs
from collections import defaultdict
import random
import sys

# PROJECT
from src.misc.helpers import read_dataset, partitions_list


def main():
	"""
	Main function.
	"""
	argparser = init_argparse()
	args = argparser.parse_args()
	print args
	if args.uniqueness_check:
		check_data_integrity(args.input, args.remove_clones, args.outdir)
	elif args.set_check:
		check_set_integrity(args.indir)
	else:
		prts = (float(args.partitions[0]), float(args.partitions[1]), float(args.partitions[2]))
		assert prts[0] + prts[1] + prts[2] == 1.0

		data = read_dataset(args.input)
		stats = get_stats(data)
		print "%i unique entities / %i unique relations / %i total in data." %(stats[0], stats[1], stats[2])
		partition_data(data, prts, args.outdir, args.whole)


def get_stats(data):
	"""
	Returns some statistics about the given data, i.e. the number of unique entities, relations and
	their sum.

	Args:
		data (list): List of relation triples as tuples.

	Returns:
		tuple: #entities, #relations, #entities + #relations.
	"""
	entities = set()
	relations = set()

	for triple in data:
		entities.add(triple[0])
		entities.add(triple[2])
		relations.add(triple[1])

	return len(entities), len(relations), len(entities) + len(relations)


def write_data_in_file(data, outfile):
	"""
	Writes relation triples into a file.

	Args:
		data (list): List of relation triples as tuples.
		outfile (str): Path the triples should be written to.
	"""
	for d in data:
		d = [part.replace(" ", "_") for part in d]
		outfile.write("%s\t%s\t%s\n" %(d[0], d[1], d[2]))


def partition_relation_wise(data, prts):
	"""
	Partition data into training, validation and test set.

	Args:
		data (list): List of relation triples as tuples.
		prts (tuple): Tuple of floats with each number corresponding to the desired percentage of data distributed to
			the corresponding set (% train set / % validation set / % tets set)

	Returns:
		tuples: Tuple of the three data sets as lists of relation triples as tuples.
	"""
	triples = defaultdict(list)

	# Aggregate relations
	for d in data:
		relation = d[1]
		if d[1] in triples.keys():
			triples[relation].append(d)
		else:
			triples[relation] = [d]

	# Partition
	train = []
	valid = []
	test = []

	for relation in triples.keys():
		rtriples = triples[relation]
		if len(rtriples) < 10:
			train.extend(rtriples)
			continue
		res = partitions_list(rtriples, prts)
		train.extend(res[0])
		valid.extend(res[1])
		test.extend(res[2])

	random.shuffle(train)
	random.shuffle(valid)
	random.shuffle(test)

	print len(train), len(valid), len(test)
	return train, valid, test


def partition_whole(data, prts):
	train, valid, test = partitions_list(data, prts)
	print len(train), len(valid), len(test)
	return train, valid, test


def partition_data(data, prts, outdir, whole=True):
	random.shuffle(data)

	train_file = codecs.open(outdir + "freebase_mtr100_mte100-train.txt", 'wb', 'utf-8')
	validation_file = codecs.open(outdir + "freebase_mtr100_mte100-valid.txt", 'wb', 'utf-8')
	test_file = codecs.open(outdir + "freebase_mtr100_mte100-test.txt", 'wb', 'utf-8')

	# Partition
	if whole:
		train, valid, test = partition_whole(data, prts)
	else:
		train, valid, test = partition_relation_wise(data, prts)

	# Write files
	write_data_in_file(train, train_file)
	write_data_in_file(valid, validation_file)
	write_data_in_file(test, test_file)

	train_file.close()
	validation_file.close()
	test_file.close()


def check_data_integrity(data_inpath, remove_clones, outpath):
	"""
	Check whether all triplets in the data are unique.
	"""
	triplets = set()

	with codecs.open(data_inpath, 'rb', 'utf-8') as infile:
		line = infile.readline().strip()
		while line:
			parts = line.split("\t")
			triplet = (parts[0], parts[1], parts[2])
			if triplet in triplets:
				sys.stderr.write("Triplet (%s, %s, %s) already in data!\n" %(triplet[0], triplet[1], triplet[2]))
				if not remove_clones:
					sys.exit(0)
			else:
				triplets.add(triplet)
			line = infile.readline().strip()

	print "Check successful!"

	if remove_clones:
		print "Writing new data..."
		with codecs.open(outpath, 'wb', 'utf-8') as outfile:
			for triplet in triplets:
				outfile.write("%s\t%s\t%s\n" %(triplet[0], triplet[1], triplet[2]))


def read_only_relations_into_set(inpath):
	"""
	Only read the relation of a given relation dataset into a set.

	Args:
		inpath (str): Path to relation dataset.

	Returns:
		set: Set of dataset relation types.
	"""
	relations = set()

	with codecs.open(inpath, 'rb', 'utf-8') as infile:
		line = infile.readline().strip()
		while line:
			parts = line.split("\t")
			relations.add(parts[1])
			line = infile.readline().strip()

	return relations


def check_set_integrity(indir):
	"""
	Checks the integrity of given training / validation / test sets (do triples with new relations appear in the
	validation or test, but not in the training set?).

	Args:
		indir (str): Directory of the datasets.
	"""
	train_set = read_only_relations_into_set(indir + "freebase_mtr100_mte100-train.txt")
	valid_set = read_only_relations_into_set(indir + "freebase_mtr100_mte100-valid.txt")
	test_set = read_only_relations_into_set(indir + "freebase_mtr100_mte100-test.txt")

	if len(valid_set.intersection(train_set)) < len(valid_set):
		sys.stderr.write("WARNING: There are unseen relations in the validation set.\n")

	if len(test_set.intersection(train_set)) < len(test_set):
		sys.stderr.write("WARNING: There are unseen relations in the test set.\n")

	print "Check complete!"


def init_argparse():
	"""
	Initialize all possible arguments for the argument parser.

	Returns:
		:py:mod:`argparse.ArgumentParser`: ArgumentParser object with command line arguments for this script.
	"""
	argparser = argparse.ArgumentParser()
	argparser.add_argument('--input',
							help='Relation input data')
	argparser.add_argument('--indir',
							help="Directory to partioned data.")
	argparser.add_argument('--outdir',
							help='Output data directory')
	argparser.add_argument('--partitions',
							nargs=3,
							help="Percentages of training / validation / testset")
	argparser.add_argument('--uniqueness_check',
							action="store_true",
							help="Checks the uniqueness of input data triplets.")
	argparser.add_argument('--set_check',
							action="store_true",
							help="Checks if relation in validation and test set appear at least once in training data.")
	argparser.add_argument('--remove_clones',
							action='store_true',
							help="Removes clones while checking for uniqueness.")
	argparser.add_argument('--whole',
							action='store_true',
							help="Partiton whole dataset or per relation..")
	return argparser

if __name__ == '__main__':
	main()
