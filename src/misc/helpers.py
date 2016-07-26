import time
import codecs
from collections import defaultdict
import numpy as np


def alt(func):
	"""
	Prepends the local time to the output of a function.

	Args:
		func (function): Function the local time should be prepended to.
	"""
	return "%s: %s" % (time.strftime("%H:%M:%S", time.gmtime()), func)


def capitalize(word):
	return word[0].upper() + word[1:]


def load_vectors_from_model(vector_inpath, max_n=None, logpath=None, indices=False):
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
