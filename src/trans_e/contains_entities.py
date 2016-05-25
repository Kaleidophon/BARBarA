import codecs
import argparse
import re

from differentiate_datasets import read_dataset


def main():
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
	print "Entities in set 1: %i" % len(entities1)
	print "Entities in set 2: %i" % len(entities2)
	print "The sets contains %i shared entities." % len(entities1.intersection(entities2))


def create_new_dataset(entities1, dataset, outpath):
	with codecs.open(outpath, 'wb', 'utf-8') as outfile:
		for triple in dataset:
			if triple[0] in entities1 and triple[2] in entities1:
				outfile.write("%s\t%s\t%s\n" %(triple[0], triple[1], triple[2]))


def extract_entities_from_tql_file(tql_path):
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
	print "Read entities from relation datatset..."
	entities = set()
	dataset = read_dataset(dataset_inpath)

	for triple in dataset:
		entities.add(triple[0])
		entities.add(triple[1])

	return entities, dataset


def format_fbid(id):
	return re.sub(r'm\.', '/m/', id)


def init_argparse():
	argparser = argparse.ArgumentParser()
	argparser.add_argument('--entities',
							help='Relation input dataset 1.')
	argparser.add_argument('--relations',
							help='Relation input dataset 2.')
	return argparser


if __name__ == "__main__":
	main()
