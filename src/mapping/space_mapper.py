#!/usr/bin/env python
import sys, codecs, numpy
from collections import defaultdict
import numpy
from scipy.spatial.distance import euclidean, cityblock, cosine


def distance(v1, v2):
	return v1 - v2


def euclidian_distance1(v1, v2):
	return euclidean(v1, v2)


def euclidian_distance2(v1, v2):
	return euclidean(v1, v2)**2


def manhattan_distance(v1, v2):
	return cityblock(v1, v2)


def cosine_similarity(v1, v2):
	return cosine(v1, v2)


def soft_cosine_similarity(v1, v2):

	def similarity_sum(v1, v2):
		sim = 0
		for d1 in v1:
			for d2 in v2:
				sim += euclidean(d1, d2)
		return sim

	return similarity_sum(v1, v2) / ((similarity_sum(v1, v1))**(1/2) * (
			similarity_sum(v2, v2))**(1/2))


def load_settings(settings_inpath):
	with codecs.open(settings_inpath, "rb", "utf-8") as settings_file:
		parts = [line.strip().split(" ") for line in settings_file.readlines()]
		settings = {part[0]: part[1] for part in parts}
		return settings


def load_vectors_from_model(vector_inpath):
	word_list = []
	vector_dict = defaultdict(numpy.array)
	with codecs.open(vector_inpath, "rb", "utf-8") as vector_infile:
		n = 1
		line = None
		while line != "":
			line = vector_infile.readline().strip()
			if n == 1:
				n += 1
				continue
			parts = line.strip().split(" ")
			word = parts[0]
			vector = numpy.array([float(dimension) for dimension in parts[1:]])
			word_list.append(word)
			vector_dict[word] = vector
			n += 1
	return word_list, vector_dict

if __name__ == "__main__":
	# Preparations
	vector_operations = {'distvec': distance,
						'eukldist1': euclidian_distance1,
						'eukldist2': euclidian_distance2,
						'mandist': manhattan_distance,
						'cossim': cosine_similarity,
						'softcossim': soft_cosine_similarity}
	sys.stdout = codecs.getwriter('utf8')(sys.stdout)
	settings = load_settings("settings.txt")
	vectors = load_vectors_from_model(settings["vectors"])[1]
	firstline = True
	for line in sys.stdin:
		if firstline:
			firstline = False
			continue
		# Process vectors from stdin linewise
		vector_parts = line.strip().split(" ")
		current_index = vector_parts[0]
		# Parse to numpy vector
		current_vector = numpy.array([float(dimension) for dimension in vector_parts[1:]])
		for index in vectors.keys():
			# Calculate other vectors
			if index == current_index or not index:
				continue
			new_vector = numpy.ndarray([])
			for operation in vector_operations:
				if operation in settings["operations"].split(" "):
					oresult = vector_operations[operation](current_vector, vectors[index])
					oresult = current_vector - vectors[index]
					new_vector = numpy.append(new_vector, oresult)
			# Redirect vectors to stdout
			sys.stdout.write(u'%s %s %s\n' % (current_index, index, ' '.join(
				[str(d) for d in new_vector.tolist()])))
		del vectors[current_index]
