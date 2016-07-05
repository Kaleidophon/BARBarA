from sklearn.svm import SVC
import numpy as np
from gensim.models import Word2Vec as w2v
from numpy.random import uniform
from numpy.linalg import norm
import argparse
import operator
from random import choice, sample
import re

import codecs
from collections import defaultdict


def main():
	argparser = init_argparser()
	args = argparser.parse_args()
	print args
	if args.train:
		train(args.sets, args.model)
	else:
		convert_data(args.sets, args.tql, args.model)

# ---------------------------------------------------------------------------------------


class SVMGuesser(object):

	def __init__(self, input_data):
		self.input_data = input_data

	def train(self, data):
		guesser = SVC()
		X = []
		y = []
		for sample in data:
			X.append(sample[0])
			y.append(sample[1])

		X = np.array(X)
		y = np.array(y)
		guesser.fit(X, y)

	def predict_relation(self, v1, v2):
		pass


# ---------------------------------------------------------------------------------------

def train(sets_path, vector_inpath, epochs=1000, learning_rate=0.01, margin=1):
	def _read_triple_file(inpath):
		triples = []

		with codecs.open(inpath, 'rb', 'utf-8') as fb_infile:
			line = fb_infile.readline().strip()
			while line != "":
				parts = line.split()
				triples.append((parts[0], parts[1], parts[2]))
				line = fb_infile.readline().strip()

		return triples

	def _encode_relations(relation_types, triples):
		new_triples = []

		for triple in triples:
			if triple[1] not in relation_types.keys():
				relation_types[triple[1]] = len(relation_types.keys())
			new_triples.append((triple[0], relation_types[triple[1]], triple[2]))

		return new_triples, relation_types

	def lossf(args):
		v_a, v_b, v_r = args
		return norm(v_a + v_r - v_b)

	train_path = sets_path + "freebase_words-train.txt"
	valid_path = sets_path + "freebase_words-valid.txt"
	test_path = sets_path + "freebase_words-test.txt"

	# Read data from file
	print "Reading freebase datasets..."
	train_triples = _read_triple_file(train_path)
	valid_triples = _read_triple_file(valid_path)
	test_triples = _read_triple_file(test_path)

	# Replace relations with indices
	print "Replacing relations with indices..."
	relation_types = {}
	train_triples, relation_types = _encode_relations(relation_types, train_triples)
	valid_triples, relation_types = _encode_relations(relation_types, valid_triples)
	test_triples, relation_types = _encode_relations(relation_types, test_triples)

	# Load word embeddings
	print "Loading word embeddings..."
	model = load_vectors(vector_inpath)

	# Prepare for training
	# initialize relation vectors
	def _init_rel_vector():
		v_r = uniform(-0.6, 0.6, 100)
		return v_r / norm(v_r)

	relation_vectors = {index: _init_rel_vector() for index in range(len(relation_types.keys()))}

	# Transform data sets
	print "Group triples by relation type..."
	entities = set()
	grouped_train, entities = transform_triples(train_triples, relation_types, entities)
	grouped_valid, entities = transform_triples(valid_triples, relation_types, entities)
	grouped_test, entities = transform_triples(test_triples, relation_types, entities)
	print "Entities: " + str(len(entities))

	print "Create corrupted triplets..."
	grouped_corrupted = create_corrupt_triples(grouped_train, entities)

	# Training
	print "\n--------- TRAINING ---------"
	for i in range(len(grouped_train.keys())):
		print "Training relation vector... (%i/%i)" % (i+1, len(grouped_train.keys()))
		data = grouped_train[i]
		corrupted_data = grouped_corrupted[i]

		if len(data) == 0:
			continue

		print "Data points: " + str(len(data))

		first_loss = 0.0
		average_error = 0.0

		for epoch in range(epochs):
			#print "Beginning epoch %i..." % (epoch+1)
			np.random.shuffle(data)
			average_error = 0.0
			for d in data:
				v_r = relation_vectors[i]
				v_a = model[d[0]]
				v_b = model[d[1]]
				#v_a /= norm(v_a)
				#v_b /= norm(v_b)
				#v_r /= norm(v_r)

				corrupted_d = sample(corrupted_data, 1)[0]
				v_a_corrupted = model[corrupted_d[0]]
				v_b_corrupted = model[corrupted_d[1]]

				sim_correct = lossf([v_a, v_b, v_r])
				sim_corrupted = lossf([v_a_corrupted, v_b_corrupted, v_r])

				raw_loss = (margin + sim_correct - sim_corrupted)
				real_loss = raw_loss if raw_loss > 0 else 0
				average_error += real_loss

			relation_vectors[i] = v_r - (v_r * average_error * learning_rate)

			if epoch == 0:
				first_loss = average_error

			average_error /= len(data)

		print "First loss: " + str(first_loss)
		print "Last loss: " + str(average_error)
		first_loss += 0.001 if first_loss == 0 else 0  # Avoid division by zero
		print "Change in loss: " + str((average_error - first_loss) / first_loss * 100.0) + " %"

	# Evaluation
	print "\n--------- EVALUATION ---------"
	mean_rank = 0.0
	mean_rank_l = 0.0
	mean_rank_r = 0.0
	mean_hitsat10 = 0.0
	mean_hitsat10l = 0.0
	mean_hitsat10r = 0.0
	n = 0.0

	for i in range(len(grouped_test.keys())):
		data = grouped_test[i]
		if len(data) == 0:
			continue

		for d in data:
			v_r = relation_vectors[i]
			v_a = model[d[0]]
			v_b = model[d[1]]
			#v_a /= norm(v_a)
			#v_b /= norm(v_b)
			#v_r /= norm(v_r)

			# Replace left
			rank_l, hitsat10l = rank_entities(v_b - v_r, d[0], model, entities)

			# Replace right
			rank_r, hitsat10r = rank_entities(v_a + v_r, d[1], model, entities)

			# Add to average values
			mean_rank += rank_l + rank_r
			mean_rank_r += rank_r
			mean_rank_l += rank_l

			mean_hitsat10 += 1 if hitsat10l else 0
			mean_hitsat10 += 1 if hitsat10r else 0
			mean_hitsat10l += 1 if hitsat10l else 0
			mean_hitsat10r += 1 if hitsat10r else 0

			n += 2

			if n % 40 == 0:
				print "Mean rank: " + str(mean_rank / n)
				print "Mean rank left: " + str(mean_rank_l / n / 2.0)
				print "Mean rank right: " + str(mean_rank_r / n / 2.0)
				print "Mean hits@10: " + str(mean_hitsat10 * 1.0 / n * 100.0)
				print "Mean hits@10 left: " + str(mean_hitsat10l * 1.0 / n * 50.0)
				print "Mean hits@10 right: " + str(mean_hitsat10r * 1.0 / n * 50.0)

	print "\n--------- FINAL RESULTS ---------"
	print "Mean rank: " + str(mean_rank / n)
	print "Mean rank left: " + str(mean_rank_l / n / 2.0)
	print "Mean rank right: " + str(mean_rank_r / n / 2.0)
	print "Mean hits@10: " + str(mean_hitsat10 * 1.0 / n * 100.0)
	print "Mean hits@10 left: " + str(mean_hitsat10l * 1.0 / n * 50.0)
	print "Mean hits@10 right: " + str(mean_hitsat10r * 1.0 / n * 50.0)


def rank_entities(reference, solution, model, entities):
	ranks = []
	solution_score = 0.0

	for entity in entities:
		v_e = model[entity]
		score = norm(v_e - reference)
		ranks.append((entity, score))
		if entity == solution:
			solution_score = score

	ranks = sorted(ranks, key=operator.itemgetter(1))

	return get_rank(solution, ranks), solution_score <= ranks[9][1]


def get_rank(target, ranks):
	for i in range(len(ranks)):
		word, rank = ranks[i]
		if word == target:
			return i
	return -1


def create_corrupt_triples(grouped_pairs, entities):
	grouped_corrupted = {key: set() for key in grouped_pairs.keys()}

	for i in range(len(grouped_pairs.keys())):
		data = grouped_pairs[i]
		corrupted_pairs = grouped_corrupted[i]

		while len(corrupted_pairs) != len(data):
			sample_pair = sample(data, 1)[0]
			replace_entity = choice(["left", "right"])
			corrupted_pair = None

			if replace_entity == "left":
				corrupted_pair = (sample(entities, 1)[0], sample_pair[1])
			elif replace_entity == "right":
				corrupted_pair = (sample_pair[0], sample(entities, 1)[0])

			if corrupted_pair not in data:
				corrupted_pairs.add(corrupted_pair)

		grouped_corrupted[i] = corrupted_pairs

	return grouped_corrupted


def transform_triples(triples, relation_types, entities):
	grouped_triples = {key: [] for key in range(len(relation_types.keys()))}

	for triple in triples:
		entities.add(triple[0])
		entities.add(triple[2])
		grouped_triples[triple[1]].append((triple[0], triple[2]))

	return grouped_triples, entities


def convert_data(sets_path, tql_inpath, vector_inpath):
	def _convert(triples, codes2names):
		new_triples = []

		for triple in triples:
			#print codes2names[triple[0]]
			#print codes2names[triple[2]]
			new_triples.append((re.sub(r'_\(.+\)$', '', codes2names[triple[0]]), triple[1], re.sub(r'_\(.+\)$', '', codes2names[triple[2]])))

		return new_triples

	# Read data from files
	print "Reading freebase datasets..."
	train_triples, valid_triples, test_triples = read_freebase_data(sets_path)
	print "Reading tql file..."
	codes2names = read_tql_file(tql_inpath)

	new_train_path = sets_path + "freebase_words-train.txt"
	new_valid_path = sets_path + "freebase_words-valid.txt"
	new_test_path = sets_path + "freebase_words-test.txt"

	# Convert triples from freebase ids to names
	print "Converting triples..."
	new_train_triples = _convert(train_triples, codes2names)
	new_valid_triples = _convert(valid_triples, codes2names)
	new_test_triples = _convert(test_triples, codes2names)

	# Load word vectors
	print "Loading word vectors..."
	model = load_vectors(vector_inpath)

	# Calculate coverage of train / valid / test data in word vectors
	print "\nCoverage training triples:"
	found_train = test_coverage(new_train_triples, model)
	print "\nCoverage validation triples:"
	found_valid = test_coverage(new_valid_triples, model)
	print "\nCoverage testing tripels:"
	found_test = test_coverage(new_test_triples, model)

	# Write new data sets into files
	write_data(new_train_triples, found_train, new_train_path)
	write_data(new_valid_triples, found_valid, new_valid_path)
	write_data(new_test_triples, found_test, new_test_path)


def write_data(triples, found_entities, outpath):
	with codecs.open(outpath, 'wb', 'utf-8') as outfile:
		for triple in triples:
			if triple[0] in found_entities and triple[2] in found_entities:
				outfile.write("%s\t%s\t%s\n" % (triple[0], triple[1], triple[2]))


def read_freebase_data(sets_path):
	train_path = sets_path + "freebase_mtr100_mte100-train.txt"
	valid_path = sets_path + "freebase_mtr100_mte100-valid.txt"
	test_path = sets_path + "freebase_mtr100_mte100-test.txt"

	train_triples = read_freebase_file(train_path)
	valid_triples = read_freebase_file(valid_path)
	test_triples = read_freebase_file(test_path)

	return train_triples, valid_triples, test_triples


def read_freebase_file(fb_inpath):
	triples = []

	with codecs.open(fb_inpath, 'rb', 'utf-8') as fb_infile:
		line = fb_infile.readline().strip()
		while line != "":
			parts = line.split()
			triples.append((parts[0], parts[1], parts[2]))
			line = fb_infile.readline().strip()

	return triples


def read_tql_file(tql_inpath):
	codes2names = defaultdict(unicode)

	with codecs.open(tql_inpath, "rb", "utf-8") as tql_file:
		line = tql_file.readline().strip()
		while line != "":
			parts = line.split()
			if len(parts) >= 4:
				raw_name, raw_fbid = parts[1], parts[3]
				name = extract_data_from_uri(raw_name)
				fbid = "/" + extract_data_from_uri(raw_fbid).replace(".", "/")
				codes2names[fbid] = name
			line = tql_file.readline().strip()

	return codes2names


def extract_data_from_uri(uri):
	uri = uri.replace("<http://de.dbpedia.org/resource/", "")
	uri = uri.replace("<http://rdf.freebase.com/ns/", "")
	uri = uri.replace(">", "")
	return uri


def load_vectors(vector_inpath):
	model = w2v.load_word2vec_format(vector_inpath, binary=False)
	return model


def test_coverage(triples, model):
	entities = set()
	errors = 0
	found_entities = set()

	for triple in triples:
		entities.add(triple[0])
		entities.add(triple[2])

	for entity in entities:
		try:
			model[entity]
			found_entities.add(entity)
		except:
			errors += 1

	coverage = (len(entities) - errors) * 1.0 / len(entities) * 100.0
	print "Coverage is %.2f %%" % coverage
	return found_entities


def init_argparser():
	argparser = argparse.ArgumentParser()
	argparser.add_argument('--model', required=True, help='Path to word vector model')
	argparser.add_argument('--tql', help='Path to .tql file')
	argparser.add_argument('--sets', required=True, help='Path to train / valid / test sets (folder)')
	argparser.add_argument('--train', action='store_true', help="Start training!")
	return argparser

if __name__ == "__main__":
	main()
	# convert_data("/Users/dennisulmer/Desktop/fb15k_transe_german/", "../../rsc/wikidata/freebase_links_de.tql")
