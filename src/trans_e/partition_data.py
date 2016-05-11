import codecs, sys
import argparse
import random


def main():
	argparser = init_argparse()
	args = argparser.parse_args()
	print args
	if args.uniqueness_check:
		check_data_integrity(args.input, args.remove_clones, args.outdir)
	else:
		prts = (float(args.partitions[0]), float(args.partitions[1]), float(args.partitions[2]))
		assert prts[0] + prts[1] + prts[2] == 1.0

		data = read_relations(args.input)
		partition_data(data, prts, args.outdir)


def read_relations(inpath):
	with codecs.open(inpath, 'rb', 'utf-8') as infile:
		return infile.readlines()


def write_data_in_file(data, file):
	for d in data:
		file.write("%s" %(d.replace(" ", "_")))


def partition_data(data, prts, outdir):
	random.shuffle(data)
	size = len(data)

	train_file = codecs.open(outdir + "freebase_mtr100_mte100-train.txt", 'wb', 'utf-8')
	validation_file = codecs.open(outdir + "freebase_mtr100_mte100-valid.txt", 'wb', 'utf-8')
	test_file = codecs.open(outdir + "freebase_mtr100_mte100-test.txt", 'wb', 'utf-8')

	# Write files
	print len(data[:int(prts[0]*size)]),\
			len(data[int(prts[0]*size)+1:int((prts[0]+prts[1])*size)]),\
			len(data[int((prts[0]+prts[1])*size)+1:])
	write_data_in_file(data[:int(prts[0]*size)], train_file)
	write_data_in_file(data[int(prts[0]*size)+1:int((prts[0]+prts[1])*size)], validation_file)
	write_data_in_file(data[int((prts[0]+prts[1])*size)+1:], test_file)

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


def check_set_integrity(indir):
	pass
	# Check whether all relation in validation and test set appear at least once in training set


def init_argparse():
	argparser = argparse.ArgumentParser()
	argparser.add_argument('--input',
							required=True,
							help='Relation input data')
	argparser.add_argument('--outdir',
							help='Output data directory')
	argparser.add_argument('--partitions',
							nargs=3,
							help="Percentages of training / validation / testset")
	argparser.add_argument('--uniqueness_check',
							action="store_true",
							help="Checks the uniqueness of input data triplets.")
	argparser.add_argument('--remove_clones',
							action='store_true',
							help="Removes clones while checking for uniqueness.")
	return argparser

if __name__ == '__main__':
	main()
