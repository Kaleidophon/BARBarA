#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Module to evaluate word embeddings by the means of analogies like "W is to X like Y is to Z". Usually,
the system uses the word embeddings of word W, X, Y and tries to find the vector of word Z that is most similar to
X and Y and most dissimilar to W.
Therefore, the CosMul method (Levy et al., 2015) is used.

The whole module is used in :py:mod:`src.eval.eval_vectors.py`.
"""

# STANDARD
import codecs
import sys

# EXTERNAL
from gensim.models import Word2Vec as w2v


def analogy_eval(vector_inpath, analogy_path, per_section=False):
	"""
	Perform analogy evaluation. Usually,
	the system uses the word embeddings of word W, X, Y and tries to find the vector of word Z that is most similar to
	X and Y and most dissimilar to W for an analogy like "W is to X like Y is to Z."
	Therefore, the CosMul method (Levy et al., 2015) is used.

	Args:
		vector_inpath (str): Path to `word2vec` vector file.
		analogy_path (str): Path to analogy file.
		per_section (bool): Flag to indicate whether analogies test should be conducted section-wise or just all in
			one run.
	"""
	# Prepare data
	sys.stdout = codecs.getwriter('utf8')(sys.stdout)  # Some words in the analogy contain umlauts
	print "Loading model..."
	model = w2v.load_word2vec_format(vector_inpath, binary=False)
	print "Reading analogies...\n"
	sections = read_analogies(analogy_path, per_section)

	# Start evaluations
	result_outputs = []

	for header in sections.keys():
		section = sections[header]
		total = len(section)
		n = 0
		fail = 0
		errors = 0
		print "Evaluating model..."

		for analogy in section:
			n += 1
			print "Analogy %i of %i (%.2f%%)" %(n, total, n * 1.0 / total * 100.0)
			print "n: %i | fail: %i | success: %i | error: %i" % (n, fail, n-fail, errors)
			try:
				print u"%s is to %s like %s is to... (%s)?" %(analogy[0], analogy[1], analogy[2], analogy[3])
				most_similar = model.most_similar_cosmul(positive=[analogy[2], analogy[1]], negative=[analogy[0]])

				# Remove W, X, Y from results
				for element in analogy:
					if element in most_similar:
						most_similar.remove(element)

				# Evaluate result
				print most_similar
				if most_similar[0][0] != analogy[3]:
					fail += 1
			except:
				errors += 1
				fail += 1
		result_outputs.append("Accuracy in section %s with vectors from %s: %.2f %%. %i KeyErrors."
								% (header, vector_inpath, (n - fail) * 1.0 / n * 100.0, errors))

	# Print results for all sections
	for result_output in result_outputs:
		print result_output


def read_analogies(analogy_path, per_section=False):
	"""
	Reads a file with analogies.

	Args:
		analogy_path (str): Path to analogy file.
		per_section (bool): Flag to indicate whether analogies test should be conducted section-wise or just all in
			one run. In this function, the section will be put into a data structure accordingly.

	Returns:
		dict: Dictionary with section header as key, list of analogy as 4-tuples as value.
	"""
	current_section = None
	sections = {}

	with codecs.open(analogy_path, "rb", "utf-8") as analogy_file:
		for line in analogy_file.readlines():
			line = line.strip()
			if line.startswith(":"):
				current_section = line[1:].strip()
				sections[current_section] = []
				continue
			parts = line.split()
			sections[current_section].append((parts[0], parts[1], parts[2], parts[3]))

	if per_section:
		return sections
	else:
		return {"Total": [analogy for section in sections.keys() for analogy in sections[section]]}
