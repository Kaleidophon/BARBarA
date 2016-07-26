#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Cool docstring.
"""

import argparse
from sklearn.cluster import DBSCAN
from sklearn.decomposition import PCA
from sklearn.metrics import silhouette_score
from scipy.optimize import minimize
import codecs
import numpy
from collections import defaultdict
import time
from random import randrange


def main():
	"""
	This is the main method. Duh.
	"""
	argparser = init_argparser()
	args = argparser.parse_args()
	if args.train:
		train_clustering_parameters(args.input[0])
	else:
		if args.pca:
			if args.resolve:
				cluster_mappings(args.input[0], True, args.pca, indices_inpath=args.input[1])
			else:
				cluster_mappings(args.input[0], True, args.pca)
		else:
			if args.resolve:
				cluster_mappings(args.input[0], indices_inpath=args.input[1])
			else:
				cluster_mappings(args.input[0])


def train_clustering_parameters(vector_inpath, iterations=10):
	x0 = numpy.array([2.5, 20, 2])
	print alt("Load mappings...")
	indices, model = load_mappings_from_model(vector_inpath)
	X = numpy.array([model[key] for key in indices])
	print alt("Start training...")

	def cluster_amount_constraint(x, expected_n):
		n = expected_n * 2.0
		factor = (n / 2)**2
		return -(1.0/factor) * x**2 + (1.0/(factor/10.0)) * x

	def simple_clustering(x):
		print alt("Current parameters: %s" %(str(x)))
		dbscan = DBSCAN(eps=x[0], min_samples=x[1], p=x[2])
		dbscan.fit(X)
		cluster_sizes = get_cluster_size(dbscan.labels_)
		print alt("Current cluster sizes: %s" %(cluster_sizes))
		sscore = silhouette_score(X, dbscan.labels_)
		tscore = (sscore / (len(cluster_sizes.keys()) - 1))
		print alt("Current value of objective function: %.5f" %(tscore))
		print "-" * 50
		return cluster_amount_constraint(-1.0 * tscore, 16)

	result = minimize(simple_clustering, x0, method="Nelder-Mead", options={'maxiter': 1})
	print result.x


def cluster_mappings(vector_inpath, do_pca=False, target_dim=100, indices_inpath=None, epsilon=2.625, min_s=20):
	# TODO: CLustering parameters
	# TODO: Metric cosine similarity or euclidian distance
	print alt("Load mappings...")
	indices, model = load_mappings_from_model(vector_inpath)
	X = numpy.array([model[key] for key in indices])
	# del model
	if do_pca:
		print alt("Truncate vectors with PCA to %i dimensions..." %(target_dim))
		pca = PCA(n_components=target_dim)
		pca.fit(X)
		X = pca.transform(X)
	print alt("Cluster points...")
	# k = 2 * X[0].shape[0] - 1
	# min_pts = k + 1
	#dbscan = DBSCAN(eps=0.1, min_samples=20, metric='cosine',algorithm='brute')
	dbscan = DBSCAN(eps=epsilon, min_samples=min_s, p=2)
	dbscan.fit(X)
	labels = dbscan.labels_
	print get_cluster_size(labels)
	print alt("Finished clustering!")
	sscore = silhouette_score(X, labels)
	print("Silhouette Coefficient: %0.3f" %(sscore))
	if indices_inpath:
		resolve_indices(indices, labels, indices_inpath, model)


def resolve_indices(points, labels, indices_inpath, model):
	indices = load_indices(indices_inpath)
	clusters = aggregate_cluster(points, labels)
	for cluster in clusters.keys():
		cluster_points = clusters[cluster]
		print "\nCluster %i:\n%s\n" %(cluster, 12*"-")
		for cluster_point in cluster_points:
			words = (indices[cluster_point[0]], indices[cluster_point[1]])
			print "%s - %s" %(words[0], words[1])


def aggregate_cluster(points, labels):
	"""
	Arranges all clusters in a list, where a sublist with all points at index i corresponds with the
	custer with label i.

	Args:
		points (list): List of datapoints
		labels (list): List of unique cluster labels

	Returns:
		list: list of lists of datapoints belonging to the i-th cluster
	"""
	print alt("Aggregate clusters...")
	clusters = defaultdict(tuple)
	for i in range(len(labels)):
		label = labels[i]
		if label == -1:
			continue
		if label in clusters.keys():
			clusters[label].append(points[i])
		else:
			clusters[label] = [points[i]]
	return clusters


def load_indices(indices_inpath):
	print alt("Load indices...")
	indices = defaultdict(str)
	with codecs.open(indices_inpath, "rb", "utf-8") as indices_inpath:
		line = indices_inpath.readline().strip()
		while line:
			parts = line.split("\t")
			index = int(parts[0])
			word = parts[1].replace(" ", "_")
			indices[index] = word
			line = indices_inpath.readline().strip()
	return indices


def get_cluster_size(labels):
	sizes = defaultdict(int)
	for label in labels:
		if label in sizes.keys():
			sizes[label] += 1
		else:
			sizes[label] = 0
	return sizes


def load_mappings_from_model(mapping_inpath):
	indices_list = []
	mappings_dict = defaultdict(numpy.array)
	with codecs.open(mapping_inpath, "rb", "utf-8") as mapping_infile:
		line = mapping_infile.readline().strip()
		while line:
			parts = line.strip().split(" ")
			indices = (int(parts[0]), int(parts[1]))
			vector = numpy.array([float(dimension.strip()) for dimension in parts[2:]])
			indices_list.append(indices)
			mappings_dict[indices] = vector
			line = mapping_infile.readline().strip()
	return indices_list, mappings_dict


def alt(func):
	return "%s: %s" % (time.strftime("%H:%M:%S", time.gmtime()), func)


def init_argparser():
	argparser = argparse.ArgumentParser()
	argparser.add_argument('--input',
							required=True,
							nargs='+',
							help='Evaluation inputs (Path to vector file / additional data')
	argparser.add_argument('--pca',
							type=int,
							help='Shorten number of dimension of mapping vectors')
	argparser.add_argument('--train', action='store_true')
	argparser.add_argument('--resolve', action='store_true')
	argparser.add_argument('--epsilon', type=float, help='Epsilon parameter for DBSCAN.')
	argparser.add_argument('--min', type=int, help='Minimum number of neighbouring objects of a point to become a core '
													'object in DBSCAN')
	return argparser


if __name__ == "__main__":
	main()