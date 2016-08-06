#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
This module contains decorators that wrap aroud functions used in other modules.
"""

# STANDARD
import codecs
from collections import defaultdict
import numpy as np
import re
import time

# EXTERNAL
#from gensim.models import Word2Vec as w2v


def alt(func):
	"""
	Prepends the local time to the output of a function.

	Args:
		func (function): Function the local time should be prepended to.
	"""
	return "%s: %s" % (time.strftime("%H:%M:%S", time.gmtime()), func)


def format_fbid(fbid):
	"""
	Transform the format of the *Freebase* IDs from the format used in the dataset to the format used in requests.

	Args:
		fbid (str): *Freebase* ID to be formatted.

	Returns:
		str: Formatted *Freebase* ID.
	"""
	return re.sub(r'm\.', '/m/', fbid)


def read_dataset(inpath):
	"""
	Reads a generic dataset with rows separated by tabs into a list.

	Args:
		inpath (str): Path to dataset.

	Returns:
		list: List of line contents as tuples.
	"""
	with codecs.open(inpath, 'rb', 'utf-8') as infile:
		return [tuple(line.strip().split('\t')) for line in infile]


def partitions_list(l, prts):
	"""
	Partitions a list into three parts according to their percentages in regard to the length of the original list given
	in a tuple as floats.

	Args:
		l (list): List to be partitioned.
		prts (tuple): Tuple of float with new list sizes.

	Returns:
		tuple: Tuple of the three new lists.
	"""
	size = len(l)
	return l[:int(prts[0] * size)], l[int(prts[0] * size) + 1:int((prts[0] + prts[1]) * size)], l[int(
		(prts[0] + prts[1]) * size) + 1:]


def capitalize(word):
	"""
	Capitalizes a string.

	Args:
		word (str): Word to be capitalized.

	Returns:
		str: Capitalized word.
	"""
	return word[0].upper() + word[1:]


def load_vectors(vector_inpath):
	"""
	Load word embeddings, gensim style.

	Args:
		vector_inpath (str): Path to vector file.

	Returns:
		gensim.models.Word2Vec: Word2Vec gensim model.
	"""
	model = w2v.load_word2vec_format(vector_inpath, binary=False)
	return model


def load_vectors_from_model(vector_inpath, max_n=None, indices=False):
	"""
	Load word embeddings (or mapping vectors), my style.

	Args:
		vector_inpath (str): Path to vector file.
		max_n (int): Maximum number of vectors to load.
		indices (bool): Flag to indicate the loading of mapping vectors.

	Returns:
		Tuple: A list of words as well as a dictionary with the vectors as numpy.arrays as value and their
		corresponding words or index pairs as keys.
	"""
	if max_n:
		alt("Start loading %i vectors in %s....\n" % (max_n, vector_inpath))
	elif not max_n:
		alt("Start loading all vectors in %s....\n" % vector_inpath)
	loading_starttime = time.time()
	word_list = []
	vector_dict = defaultdict(np.array)
	with codecs.open(vector_inpath, "rb", "utf-8") as vector_infile:
		n = 1
		line = vector_infile.readline().strip()
		while line != "":
			if n == 1:
				n += 1
				line = vector_infile.readline().strip()
				continue
			parts = line.strip().split(" ")
			if indices:
				word = (int(parts[0]), int(parts[1]))
				vector = np.array([float(dimension) for dimension in parts[2:]])
			else:
				word = parts[0]
				vector = np.array([float(dimension) for dimension in parts[1:]])
			word_list.append(word)
			vector_dict[word] = vector
			if max_n:
				if n == max_n:
					break
			n += 1
			line = vector_infile.readline().strip()
	loading_endtime = time.time()
	m, s = divmod(loading_endtime - loading_starttime, 60)
	alt("Loading of %s complete! Loading took %2dm %2ds.\n" % (vector_inpath, m, s))
	return word_list, vector_dict


def contains_tag(line):
	"""
	Checks whether the current line contains an xml tag.

	Args:
		line (str): Current line

	Returns:
		bool: Whether the current line contains an xml tag.
	"""
	pattern = re.compile("<.+>")
	return pattern.search(line) is not None


def extract_sentence_id(tag):
	"""
	Extract the sentence ID of current sentence.

	Args:
		tag (str): Sentence tag

	Returns:
		str: sentence ID
	"""
	if "<s" not in tag:
		return ""
	pattern = re.compile('id="[a-z0-9]+?"(?=\s)')
	res = re.findall(pattern, tag)
	if len(res) == 0:
		return None
	return res[0].replace('"', "").replace("id=", "")
