import codecs
import sys
import os
import argparse
import random
from collections import defaultdict


def main():
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

		data = read_relations(args.input)
		stats = get_stats(data)
		print "%i unique entities / %i unique relations / %i total in data." %(stats[0], stats[1], stats[2])
		partition_data(data, prts, args.outdir, args.whole)


def read_relations(inpath):
	with codecs.open(inpath, 'rb', 'utf-8') as infile:
		return [line.strip().split("\t") for line in infile.readlines()]


def get_stats(data):
	entities = set()
	relations = set()

	for triple in data:
		#print triple[0], triple[2]
		entities.add(triple[0])
		entities.add(triple[2])
		relations.add(triple[1])

	return len(entities), len(relations), len(entities) + len(relations)


def write_data_in_file(data, outfile):
	for d in data:
		d = [part.replace(" ", "_") for part in d]
		outfile.write("%s\t%s\t%s\n" %(d[0], d[1], d[2]))


def partition_relation_wise(data, prts):
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


def partitions_list(l, prts):
	size = len(l)
	return l[:int(prts[0]*size)], l[int(prts[0]*size)+1:int((prts[0]+prts[1])*size)], l[int((prts[0]+prts[1])*size)+1:]


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
	relations = set()

	with codecs.open(inpath, 'rb', 'utf-8') as infile:
		line = infile.readline().strip()
		while line:
			parts = line.split("\t")
			relations.add(parts[1])
			line = infile.readline().strip()

	return relations


def check_set_integrity(indir):
	train_set = read_only_relations_into_set(indir + "freebase_mtr100_mte100-train.txt")
	valid_set = read_only_relations_into_set(indir + "freebase_mtr100_mte100-valid.txt")
	test_set = read_only_relations_into_set(indir + "freebase_mtr100_mte100-test.txt")

	if len(valid_set.intersection(train_set)) < len(valid_set):
		sys.stderr.write("WARNING: There are unseen relations in the validation set.\n")

	if len(test_set.intersection(train_set)) < len(test_set):
		sys.stderr.write("WARNING: There are unseen relations in the test set.\n")

	print "Check complete!"


def init_argparse():
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