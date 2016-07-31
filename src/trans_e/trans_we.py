#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
This module follows a modified approach from
`(Bordes et al., 2013) <http://papers.nips.cc/paper/5071-translating-embeddings-for-modeling-multi-relational-data.pdf>`_.
As so, noise-contrastive learning and corrupt triples are use. But whereas in this original paper,
vector representations forn entities and relations are learned in a joint manner, in this case only the continuous
representations for semantic relations will be learned and word embeddings used for the entities instead.

.. warning::
	Because we use words embedding but still the *FB15k* dataset here, we can only use data samples where we have
	trained word embeddings for both entities. Those are only a few, which is one reason why this approach performs
	badly.
"""

# STANDARD
import argparse
import codecs
from collections import defaultdict
import operator
from random import choice, sample
import re

# EXTERNAL
import numpy as np
from numpy.linalg import norm
from numpy.random import uniform

# PROJECT
from src.misc.helpers import read_dataset, load_vectors


def main():
	"""
	Main function.
	"""
	def lossf(args):
		v_a, v_b, v_r = args
		return norm(v_a + v_r - v_b)

	argparser = init_argparser()
	args = argparser.parse_args()
	print args
	relation_vectors = None
	if args.train:
		model, grouped_train, grouped_valid, grouped_test, grouped_corrupted, relation_types, entities = \
			prepare_training(args.sets, args.model)
		relation_vectors = train(model, grouped_train, grouped_corrupted, lossf, relation_types)
		if args.out:
			dump_relation_vectors(relation_vectors, args.out)
	if args.eval:
		if args.input:
			relation_vectors = load_relation_vectors(args.input)
		model, grouped_train, grouped_valid, grouped_test, grouped_corrupted, relation_types, entities = \
			prepare_training(args.sets, args.model)
		evaluate(model, grouped_test, relation_vectors, entities)

# ---------------------------------------------------------------------------------------


def prepare_training(sets_path, vector_inpath):
	"""
	Prepares the training step loading word embeddings and training sets.

	Args:
		sets_path (str): Path to training set directory.
		vector_inpath (str): Path to word embedding file.

	Returns:
		tuple: Tuple of results with **model** (*gensim.models.Word2Vec*): Word embeddings as gensim model /
		**grouped_train** (*dict*): Training samples as dictionary with relation as key and a list of tuples
		with two entities each as value / **grouped_valid** (*dict*): As **grouped_train** / **grouped_test**
		(*dict*): As **grouped_train** / **grouped_corrupted** (*dict*): As **grouped_train** /
		**relations_types** (*dict*): Dictionary with relations as key and the amount of triples with this relation
		as a key / **entities** (*set*): Set of unique entities.
	"""
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

	# Transform data sets
	print "Group triples by relation type..."
	entities = set()
	grouped_train, entities = transform_triples(train_triples, relation_types, entities)
	grouped_valid, entities = transform_triples(valid_triples, relation_types, entities)
	grouped_test, entities = transform_triples(test_triples, relation_types, entities)
	print "Entities: " + str(len(entities))

	print "Create corrupted triplets..."
	grouped_corrupted = create_corrupt_triples(grouped_train, entities)

	return model, grouped_train, grouped_valid, grouped_test, grouped_corrupted, relation_types, entities


def train(model, grouped_train, grouped_corrupted, lossf, relation_types, epochs=1000, learning_rate=0.01, margin=1.0):
	"""
	Train the relation vectors following the example of `(Bordes et al.,
	2013) <http://papers.nips.cc/paper/5071-translating-embeddings-for-modeling-multi-relational-data.pdf>`_,
	but use word embeddings for the entity vectors instead.

	Args:
		model (*gensim.models.Word2Vec*): Word embeddings as gensim model.
		grouped_train (dict): Training samples as dictionary with relation as key and a list of tuples
		with two entities each as value.
		grouped_corrupted (dict): As grouped_train.
		lossf (func): Loss function for training.
		relation_types (dict): Dictionary with relations as key and the amount of triples with this relation as a key.
		epochs (int): Number of training epochs.
		learning_rate (float): Learning rate for training.
		margin (float): Margin :math:`\gamma` for training.

	Returns:
		dict: Dictionary with index of a relation as key and the relations vector as a numpy.array as value.
	"""
	# Prepare for training
	# initialize relation vectors
	def _init_rel_vector():
		v_r = uniform(-0.6, 0.6, 100)
		return v_r / norm(v_r)

	relation_vectors = {index: _init_rel_vector() for index in range(len(relation_types.keys()))}

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
			np.random.shuffle(data)
			average_error = 0.0
			for d in sample(data, int(len(data)/5.0)+1):
				v_r = relation_vectors[i]
				v_a = model[d[0]]
				v_b = model[d[1]]

				corrupted_d = sample(corrupted_data, 1)[0]
				v_a_corrupted = model[corrupted_d[0]]
				v_b_corrupted = model[corrupted_d[1]]

				sim_correct = lossf([v_a, v_b, v_r])
				sim_corrupted = lossf([v_a_corrupted, v_b_corrupted, v_r])

				raw_loss = (margin + sim_correct - sim_corrupted)
				real_loss = raw_loss if raw_loss > 0 else 0
				average_error += real_loss

			average_error /= len(data)
			relation_vectors[i] = v_r - (v_r * average_error * learning_rate)

			if epoch == 0:
				first_loss = average_error

			if epoch % 100 == 0:
				print average_error

		print "First loss: " + str(first_loss)
		print "Last loss: " + str(average_error)
		first_loss += 0.001 if first_loss == 0 else 0  # Avoid division by zero
		print "Change in loss: " + str((average_error - first_loss) / first_loss * 100.0) + " %"

	return relation_vectors


def _stupid_train(model, grouped_train, relation_types):
	"""
	Very stupid way of training. Just used as a test.

	Args:
		model (*gensim.models.Word2Vec*): Word embeddings as gensim model.
		grouped_train (dict): Training samples as dictionary with relation as key and a list of tuples
		with two entities each as value.
		relation_types (dict): Dictionary with relations as key and the amount of triples with this relation as a key.

	Returns:
		dict: Dictionary with index of a relation as key and the relations vector as a numpy.array as value.
	"""
	relation_vectors = [None] * len(relation_types)

	# Training
	print "\n--------- STUPID TRAINING ---------"

	for i in range(len(grouped_train.keys())):
		data = grouped_train[i]
		current_relation_vector = np.array([0.0] * 100)

		for d in data:
			v_a = model[d[0]]
			v_b = model[d[1]]
			current_relation_vector += (v_b - v_a)

		relation_vectors[i] = current_relation_vector / len(data)

	return relation_vectors


def evaluate(model, grouped_test, relation_vectors, entities):
	"""
	Evaluate the relations vector the same way as in `(Bordes et al.,
	2013) <http://papers.nips.cc/paper/5071-translating-embeddings-for-modeling-multi-relational-data.pdf>`_.
	Therefore, for every relation triple in the testset, one entity will be removed and all entities will be inserted
	afterwards. Also they will be ranked by their loss (ascending) and assigned a rank. The evaluation metrics are
	the percentage of times the right entity is in the top ten highest ranked entities and mean rank of the correct
	entitiy.

	Args:
		model (gensim.models.Word2Vec): Word embeddings as gensim model.
		grouped_test (dict): Test samples as dictionary with relation as key and a list of tuples
		with two entities each as value.
		relation_vectors (dict): Dictionary with index of a relation as key and the relations vector as a numpy.array
		as value.
		entities (set): Set of unique entities.
	"""

	def _print_eval(mean_rank_r, mean_rank_l, count_l, count_r):
		print "Mean rank: " + str((mean_rank_r / count_r + mean_rank_l / count_l) / 2.0)
		print "Mean rank left: " + str(mean_rank_l / count_l)
		print "Mean rank right: " + str(mean_rank_r / count_r)
		print "Mean hits@10: " + str((mean_hitsat10l / count_l + mean_hitsat10r / count_r) / 2.0 * 100.0)
		print "Mean hits@10 left: " + str(mean_hitsat10l * 1.0 / count_l * 100.0)
		print "Mean hits@10 right: " + str(mean_hitsat10r * 1.0 / count_r * 100.0) + "\n"

	# Evaluation
	print "\n--------- EVALUATION ---------"
	mean_rank_l = 0.0
	mean_rank_r = 0.0
	mean_hitsat10l = 0.0
	mean_hitsat10r = 0.0
	count_l = 0.0
	count_r = 0.0

	for i in range(len(grouped_test.keys())):
		data = grouped_test[i]
		if len(data) == 0:
			continue

		for d in data:
			v_r = relation_vectors[i]
			v_a = model[d[0]]
			v_b = model[d[1]]

			# Replace left
			rank_l, hitsat10l = rank_entities(v_b - v_r, d[0], model, entities)

			# Replace right
			rank_r, hitsat10r = rank_entities(v_a + v_r, d[1], model, entities)

			# Add to average values
			mean_rank_r += rank_r
			mean_rank_l += rank_l

			mean_hitsat10l += 1 if hitsat10l else 0
			mean_hitsat10r += 1 if hitsat10r else 0

			count_l += 1
			count_r += 1

			if (count_l + count_r) % 40 == 0:
				_print_eval(mean_rank_r, mean_rank_l, count_r, count_l)

	print "--------- FINAL RESULTS ---------"
	_print_eval(mean_rank_r, mean_rank_l, count_r, count_l)


def rank_entities(reference, solution, model, entities):
	"""
	Ranks entities against a reference vector.

	Args:
		reference (numpy.array): Reference vector.
		solution (str): The actual solution.
		model (gensim.models.Word2Vec): Word embeddings as gensim model.
		entities (set): Set of unique entities.

	Returns:
		tuples: Rank of solution as integer, flag if a Hit@10 has occurred as boolean.
	"""
	ranks = []

	for entity in entities:
		v_e = model[entity]
		score = norm(v_e - reference)
		ranks.append((entity, score))

	sorted_ranks = sorted(ranks, key=operator.itemgetter(1))

	solution_rank = get_rank(solution, sorted_ranks)

	return get_rank(solution, sorted_ranks), solution_rank <= 9


def get_rank(target, ranks):
	"""
	Get rank of a target entity within all ranked entities.

	Args:
		target (str): Target entity which rank should be determined.
		ranks (list): List of tuples of an entity and its rank.

	Returns:
		int: Rank of entity or -1 if entity is not present in ranks.
	"""
	for i in range(len(ranks)):
		word, rank = ranks[i]
		if word == target:
			return i
	return -1


def create_corrupt_triples(grouped_pairs, entities):
	"""
	Creates a set of corrupted training triplets group by their shared relation.

	Args:
		grouped_pairs (dict): Test samples as dictionary with relation as key and a list of tuples
		with two entities each as value.
		entities (set): Set of unique entities.

	Returns:
		grouped_train (dict): Corrupted training samples as dictionary with a relation as key and a list of tuples
		with two entities each as value.
	"""
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
	"""
	Groups a list of relations triples by their relations and returns a suitable data structure.

	Args:
		triples (list): List of relation triples as tuples.
		relation_types (dict): Dictionary with relations as key and the amount of triples with this relation as a key.
		entities (set): Set of unique entities.

	Returns:
		tuple: Dictionary with relation as key and a list of entity tuples as value and an augmented set of unique
			entities.
	"""
	grouped_triples = {key: [] for key in range(len(relation_types.keys()))}

	for triple in triples:
		entities.add(triple[0])
		entities.add(triple[2])
		grouped_triples[triple[1]].append((triple[0], triple[2]))

	return grouped_triples, entities


def convert_data(sets_path, tql_inpath, vector_inpath):
	"""
	Re-formats relation data sets to fit the training routine in this module.
	Also tests the coverage of word embedding model on all entities in the datasets.

	Args:
		sets_path (str): Directory of the datasets.
		tql_inpath (str): Path to *Wikidata* *Freebase* dump in `tql` format.
		vector_inpath (str): Path to word embedding file.
	"""
	def _convert(triples, codes2names):
		new_triples = []

		for triple in triples:
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
	"""
	Writes relation triples into a file, but only those triples where both entities are also found in a designated
	set.

	Args:
		triples (list): List of relation triples as tuples.
		found_entities (set): Set of unique entities.
		outpath (str): Path the data should be written to.
	"""
	with codecs.open(outpath, 'wb', 'utf-8') as outfile:
		for triple in triples:
			if triple[0] in found_entities and triple[2] in found_entities:
				outfile.write("%s\t%s\t%s\n" % (triple[0], triple[1], triple[2]))


def read_freebase_data(sets_path):
	"""
	Reads all different datasets in a directory at once.

	Args:
		sets_path (str): Directory of the datasets.

	Returns:
		tuple: Tuple of datasets as lists.
	"""
	train_path = sets_path + "freebase_mtr100_mte100-train.txt"
	valid_path = sets_path + "freebase_mtr100_mte100-valid.txt"
	test_path = sets_path + "freebase_mtr100_mte100-test.txt"

	train_triples = read_dataset(train_path)
	valid_triples = read_dataset(valid_path)
	test_triples = read_dataset(test_path)

	return train_triples, valid_triples, test_triples


def read_tql_file(tql_inpath):
	"""
	Reads a *Freebase* dump by wikidata. Must be in `tql` format. Available online
	`here <https://developers.google.com/freebase/>`_ (July 2016).

	Args:
		tql_inpath (str): Path to *Wikidata* *Freebase* dump in `tql` format.

	Returns:
		defaultdict: Dictionary with *Freebase* code as key and the corresponding real name of an entity as value.
	"""
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


def dump_relation_vectors(relation_vectors, outpath):
	"""
	Saves relation numpy vectors.

	Args:
		relation_vectors (dict): Dictionary with index of a relation as key and the relations vector as a
		numpy.array as value.
		outpath (str): Path the vectors should be saved to.

	"""
	print "Saving relation vectors to " + outpath
	np.save(outpath, relation_vectors)


def load_relation_vectors(inpath):
	"""
	Loads relation numpy vectors.

	Args:
		inpath (str): Path the numpy vectors should be loaded from.

	Returns:
		dict: Dictionary with index of a relation as key and the relations vector as a numpy.array as value.
	"""
	print "Loading relation vectors from " + inpath
	return np.load(inpath).tolist()


def extract_data_from_uri(uri):
	"""
	Extracts data from an URI.

	Args:
		uri (str): URI the data should be extracted from.

	Returns:
		str: Extracted data.
	"""
	uri = uri.replace("<http://de.dbpedia.org/resource/", "")
	uri = uri.replace("<http://rdf.freebase.com/ns/", "")
	uri = uri.replace(">", "")
	return uri


def test_coverage(triples, model):
	"""
	Test the coverage of a dataset consisting of freebase triples on word2vec word embeddings.
	For every triple (h, l, t), the entities h and t are taken and used for look up in the word2vec
	model.

	Args:
		triples (list): List of relation triples as tuples.
		model (gensim.models.Word2Vec): Word embeddings as gensim model.

	Return:
		set: Set of entities in the model.
	"""
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
	"""
	Initialize all possible arguments for the argument parser.

	Returns:
		:py:mod:`argparse.ArgumentParser`: ArgumentParser object with command line arguments for this script.
	"""
	argparser = argparse.ArgumentParser()
	argparser.add_argument('--model', required=True, help='Path to word vector model')
	argparser.add_argument('--tql', help='Path to .tql file')
	argparser.add_argument('--sets', required=True, help='Path to train / valid / test sets (folder)')
	argparser.add_argument('--train', action='store_true', help="Start training!")
	argparser.add_argument('--eval', action='store_true', help="Start evaluation!")
	argparser.add_argument('--out', type=str, help="Output path for relation vectors.")
	argparser.add_argument('--input', type=str, help="Input path for relation vectors.")
	return argparser

if __name__ == "__main__":
	main()
