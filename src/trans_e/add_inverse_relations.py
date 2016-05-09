import codecs, sys
from collections import defaultdict
import argparse


def main():
	argparser = init_argparse()
	args = argparser.parse_args()
	print args
	known_relations, inverse_relations = read_file_with_inverse_relations(args.inverse)
	add_inverse_relations(args.input, args.output, inverse_relations, known_relations)


def add_inverse_relations(relations_inpath, relations_outpath, inverse_relations, known_relations):
	known_keys = known_relations.keys()
	inverse_keys = inverse_relations.keys()

	with codecs.open(relations_inpath, 'rb', 'utf-8') as relations_infile:
		with codecs.open(relations_outpath, 'wb', 'utf-8') as relations_outfile:
			line = relations_infile.readline()
			while line:
				triple = tuple(line.strip().split('\t'))
				entikey = (triple[0], triple[2])
				rentikey = (triple[2], triple[0])
				new_relation = ""

				if entikey not in known_keys:
					if triple[1] in inverse_keys:
						new_relation = triple[1] + "." + inverse_relations[triple[1]]
					else:
						new_relation = triple[1]
				else:
					if entikey in known_keys:
						new_relation += known_relations[entikey]
					if rentikey in known_keys:
						new_relation += "." + known_relations[rentikey]

				relations_outfile.write("%s\t%s\t%s\n" %(triple[0], new_relation, triple[2]))

				line = relations_infile.readline()


def read_file_with_inverse_relations(inverse_inpath):
	known_relations = defaultdict(str)
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
				known_relations[(triple[0], triple[2])] = relations[0]
				known_relations[(triple[2], triple[0])] = relations[1]
			else:
				known_relations[(triple[0], triple[2])] = relations[0]
			line = inverse_infile.readline()
	return known_relations, inverse_relations


def init_argparse():
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