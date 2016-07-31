#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Module used to conduct the word similarity evaluation. The system assign word pairs a similarity score based on
the cosine similarity of their word embeddings. Then, to correlation between those and human ratings is measured with
Pearson's rho.

The whole module is used in :py:mod:`src.eval.eval_vectors.py`.
"""

# STANDARD
import codecs
import math

# EXTERNAL
from scipy.stats import pearsonr
from scipy.spatial.distance import cosine

# PROJECT
from src.misc.helpers import capitalize, alt, load_vectors_from_model


def word_sim_eval(vector_inpath, wordpair_path, format="google"):
	"""
	Function that let's the system assign word pairs a similarity score based on the cosine similarity of their word
	embeddings. Then, to correlation between those and human ratings is measured with Pearson's rho.

	Args:
		vector_inpath (str): Path to vector file. File has to have the following format (separated by spaces):
			<index of original vector #1> <index of original vector #2> <Dimension 1> ... <Dimension n>
		wordpair_path (str): Path to word pair file.
		format (str): Format of word pair file {google|semrel}
	"""
	alt("Loading model...\n")
	model = load_vectors_from_model(vector_inpath)[1]

	# Read word pairs with values
	alt("Loading word pairs...\n")
	raw_pairs, x = read_wordpairs(wordpair_path, format)
	y = [None] * len(raw_pairs)
	pairs = []
	for i in range(len(raw_pairs)):
		pairs.append((i, raw_pairs[i]))

	# Calculate similarity values for pairs
	alt("Calculating word pair similarities...\n")
	error_counter = 0
	for pair_id, pair in pairs:
		try:
			a = model[capitalize(pair[0])]
			b = model[capitalize(pair[1])]
			sim = cosine(a, b)
			y[pair_id] = sim
		except TypeError:
			error_counter += 1

	# Remove pairs where no word embedding was found
	x, y = remove_unknowns(x, y)

	# Calculate results
	rho, t, z = evaluate_wordpair_sims(x, y, len(pairs))

	successful_pairs = len(pairs) - error_counter
	successful_percentage = (len(pairs) - error_counter * 1.0) / len(pairs) * 100.0
	alt("Calculated Pearman's rho for %i pairs (%.2f %%).\n\tr = %.4f\n\tt = %.4f\n\tz = %.4f\n"
		% (successful_pairs, successful_percentage, rho, t, z))


def remove_unknowns(x, y):
	"""
	Remove word pairs from the results where one or two word embedding weren't found.

	Args:
		x (list): List of similarity scores assigned by humans.
		y (list): List of similarity scores assigned by the system.

	Returns:
		x (list): Purged list of similarity scores assigned by humans.
		y (list): Purged list of similarity scores assigned by the system.
	"""
	to_pop = []
	for i in range(len(y)):
		if not y[i]:
			to_pop.append(i)

	# Avoid popping elements while iterating through list
	to_pop.reverse()
	for i in to_pop:
		x.pop(i)
		y.pop(i)

	return x, y


def evaluate_wordpair_sims(x, y, number_of_pairs):
	"""
	Evaluate results of the similarity score assignments, i.e. calculate pearson's rho and its significance.

	Args:
		x (list): List of similarity scores assigned by humans.
		y (list): List of similarity scores assigned by the system.
		number_of_pairs (int): Number of word pairs evaluated.

	Returns:
		rho (float): Pearson's correlation coefficient.
		t (float): Student's t value.
		z (float): z value.
	"""
	# Calculate Pearman's rho
	rho = pearsonr(x, y)[0]

	rho_ = math.fabs(rho)
	# Calculate rho's significance
	t = ((number_of_pairs - 2) / (1 - rho_ ** 2)) ** (1 / 2) * rho_  # Student t
	# test
	z = ((number_of_pairs - 3) / 1.06) ** (1 / 2) * 1 / 2 * math.log((1 + rho_) / (1 - rho_))
	return rho, t, z


def read_wordpairs(wordpair_path, format="google"):
	"""
	Read wordpair file with wordpairs and their similarity scores assigned by humans.

	Args:
		wordpair_path (str): Path to word pair file.
		format (str): Format of wor pair file {google|semrel}

	Returns:
		tuple: Tuple of a list of word pairs and a list of similarity scores for those same pair assigned by humans.
	"""
	pairs, x = [], []
	with codecs.open(wordpair_path, "rb", "utf-8") as wordpair_file:
		if format == "google":
			for line in wordpair_file:
				line_parts = line.strip().split()
				word_pair = line_parts[0].split("-")
				pairs.append((word_pair[0], word_pair[1]))
				x.append(float(line_parts[1]))
		elif format == "semrel":
			for line in wordpair_file:
				if line.startswith('#'):
					continue
				line_parts = line.strip().split(":")
				pairs.append((line_parts[0], line_parts[1]))
				x.append(float(line_parts[2]))
	return pairs, x
