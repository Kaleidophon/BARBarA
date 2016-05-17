import codecs, argparse


def main():
	argparser = init_argparse()
	args = argparser.parse_args()
	extract_conll(args.input, args.output, args.column)


def extract_conll(inpath, outpath, column):
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