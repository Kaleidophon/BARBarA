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
