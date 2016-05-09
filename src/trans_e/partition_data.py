import codecs, sys
import argparse
import random


def main():
	argparser = init_argparse()
	args = argparser.parse_args()
	print args
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
	write_data_in_file(data[int(prts[0]*size):], train_file)
	write_data_in_file(data[int(prts[0]*size)+1:int((prts[0]+prts[1])*size)], validation_file)
	write_data_in_file(data[int((prts[0]+prts[1])*size)+1:], test_file)

	train_file.close()
	validation_file.close()
	test_file.close()


def init_argparse():
	argparser = argparse.ArgumentParser()
	argparser.add_argument('--input',
							required=True,
							help='Relation input data')
	argparser.add_argument('--outdir',
							required=True,
							help='Output data directory')
	argparser.add_argument('--partitions',
							nargs=3,
							required=True,
							help="Percentages of training / validation / testset")
	return argparser

if __name__ == '__main__':
	main()
