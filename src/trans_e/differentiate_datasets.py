import codecs
import argparse


def main():
	argparser = init_argparse()
	args = argparser.parse_args()
	print args
	set1, set2 = read_dataset(args.set1), read_dataset(args.set2)
	compare_entities(set1, set2)


def read_dataset(inpath):
	with codecs.open(inpath, 'rb', 'utf-8') as infile:
		return [tuple(line.strip().split('\t')) for line in infile]


def compare_entities(set1, set2):
	set1_entities = set()
	set2_entities = set()

	for d in set1:
		set1_entities.add(d[0])
		set1_entities.add(d[2])

	for d in set2:
		set2_entities.add(d[0])
		set2_entities.add(d[2])

	print "Types of entities in set 1: %i" % len(set1_entities)
	print "Types of entities in set 2: %i" % len(set2_entities)

	difference = len(set2_entities) - len(set1_entities.intersection(set2_entities)) \
					if len(set2_entities) > len(set1_entities) \
					else len(set1_entities) - len(set1_entities.intersection(set2_entities))
	print "Types of entities shared between the two sets: %i" % len(set1_entities.intersection(set2_entities))
	print "Types of entites not shared between the two sets: %i" % difference
	decision1 = True if raw_input("Show samples? (y/n) ").strip() == "y" else False
	if decision1:
		intersec = set1_entities.intersection(set2_entities)
		sorted(intersec)
		for entry in intersec:
			print entry

	decision2 = True if raw_input("Create new dataset without non-shared entities? (y/n) ").strip() == "y" else False
	if decision2:
		outpath = raw_input("Please enter path for new dataset: ")
		print outpath
		shared_entities = set1_entities.intersection(set2_entities)

		with codecs.open(outpath, 'wb', 'utf-8') as outfile:
			target_set = set1 if len(set1) > len(set2) else set2
			for entry in target_set:
				if entry[0] in shared_entities and entry[2] in shared_entities:
					outfile.write("%s\t%s\t%s\n" % (entry[0], entry[2], entry[2]))






def init_argparse():
	argparser = argparse.ArgumentParser()
	argparser.add_argument('--set1',
							help='Relation input dataset 1.')
	argparser.add_argument('--set2',
							help='Relation input dataset 2.')
	return argparser


if __name__ == "__main__":
	main()
