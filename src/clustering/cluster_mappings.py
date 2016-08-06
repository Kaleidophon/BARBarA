#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Script to cluster mapping vectors created with :py:mod:`src.mapping.mapthreading`.
"""
# Ugly import hack
import sys
import os
sys.path.insert(0, os.path.abspath('../../'))

# STANDARD
import argparse
import codecs
from collections import defaultdict

# EXTERNAL
import numpy
from scipy.optimize import minimize
from sklearn.cluster import DBSCAN
from sklearn.decomposition import PCA
from sklearn.metrics import silhouette_score

# PROJECT
from src.misc.helpers import alt


def main():
	"""
	This is the main function. It uses the parsed command line arguments to pick the right function to execute.
	"""
	argparser = init_argparser()
	args = argparser.parse_args()
	if args.train:
		train_clustering_parameters(args.input[0])
	else:
		if args.resolve:
			cluster_mappings(args.input[0], True, args.pca, indices_inpath=args.input[1], epsilon=args.epsilon, min_s=args.min)
		else:
			cluster_mappings(args.input[0], True, args.pca, epsilon=args.epsilon, min_s=args.min)


def train_clustering_parameters(vector_inpath):
	"""
	Functions that tries to figure out the optimal clustering parameters in regard to DBSCAN's epsilon,
	min_samples and p.

	Args:
		vector_inpath (str): Path to vector file. File has to have the following format (separated by spaces):
			<index of original vector #1> <index of original vector #2> <Dimension 1> ... <Dimension n>
	"""
	x0 = numpy.array([2.5, 20, 2])  # First parameters
	print alt("Load mappings...")
	indices, model = load_mappings_from_model(vector_inpath)
	X = numpy.array([model[key] for key in indices])  # Arrange data for optimization
	print alt("Start training...")

	# Cluster data and calculate loss as silhouette coefficient per cluster
	def simple_clustering(x):
		# Start clustering
		print alt("Current parameters: %s" % (str(x)))
		dbscan = DBSCAN(eps=x[0], min_samples=x[1], p=x[2])
		dbscan.fit(X)

		# Evaluate results
		cluster_sizes = get_cluster_size(dbscan.labels_)
		print alt("Current cluster sizes: %s" % (cluster_sizes))
		sscore = silhouette_score(X, dbscan.labels_)
		tscore = (sscore / (len(cluster_sizes.keys()) - 1))
		print alt("Current value of objective function: %.5f" % (tscore))
		print "-" * 50
		return tscore

	# Start minimizing
	result = minimize(simple_clustering, x0, method="Nelder-Mead")
	print result.x  # Print resulting parameter configuration


def cluster_mappings(vector_inpath, do_pca=False, target_dim=100, indices_inpath=None, epsilon=2.625, min_s=20):
	"""
	Cluster mapping vectors created with :py:mod:`src.mapping.mapthreading` or :py:mod:`rc.mapping.map_vectors.py`.
	Because just reading about the number of clusters and their sizes, there's an option to resolve the indices of
	the vectors in the cluster to their original word pairs.

	Args:
		vector_inpath (str): Path to vector file. File should have the following format (separated by spaces):
			<index of original vector #1> <index of original vector #2> <Dimension 1> ... <Dimension n>
		do_pca (bool): Flag to indicate whether PCA should be executed before clustering to reduce amount of
		computation.
		target_dim (int): Number of dimensions vectors should be shrunk to in case PCA is performed.
		indices_inpath (str): Path to file with the indices given to words. The file should have the following format:
			<index of word>	<word> (separated by tab)
		epsilon (float): Radius of circle DBSCAN uses to look for other data points.
		min_s (int): Minimum number of points in radius epsilon DBSCAN needs to declare a point a core object.
	"""
	print alt("Load mappings...")
	indices, model = load_mappings_from_model(vector_inpath)
	X = numpy.array([model[key] for key in indices])
	del model  # free up memory

	# do PCA if wanted
	if do_pca:
		print alt("Truncate vectors with PCA to %i dimensions..." % target_dim)
		pca = PCA(n_components=target_dim)
		pca.fit(X)
		X = pca.transform(X)

	# Start clustering
	print alt("Cluster points...")
	dbscan = DBSCAN(eps=epsilon, min_samples=min_s, p=2)
	dbscan.fit(X)

	# Get results
	labels = dbscan.labels_
	print alt("Cluster sizes:")
	print get_cluster_size(labels)
	print alt("Finished clustering!")
	sscore = silhouette_score(X, labels)
	print("Silhouette Coefficient: %0.3f" % (sscore))

	# Resolve indices and print all word-pairs in clusters if wanted
	if indices_inpath:
		resolve_indices(indices, labels, indices_inpath, model)


def resolve_indices(points, labels, indices_inpath):
	"""
	Resolves the indices of word pairs found in a cluster to their real names.

	Args:
		points (list): List of datapoints
		labels (list): List of unique cluster labels
		indices_inpath (str): Path to file with the indices given to words. The file should have the following format:
			<index of word>	<word> (separated by tab)
	"""
	indices = load_indices(indices_inpath)
	clusters = aggregate_cluster(points, labels)
	for cluster in clusters.keys():
		cluster_points = clusters[cluster]
		print "\nCluster %i:\n%s\n" % (cluster, 12 * "-")
		for cluster_point in cluster_points:
			words = (indices[cluster_point[0]], indices[cluster_point[1]])
			print "%s - %s" % (words[0], words[1])


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
	"""
	Load word indices from a file. The file should have the following format: <index of word>	<word> (separated by
	tab)

	Args:
		indices_inpath (str): Path to index file.
	"""
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
	"""
	Calculate the size of every cluster found by DBSCAN.

	Args:
		labels (list): List of cluster IDs assigned to every data point.

	Returns:
		defaultdict: Dictionary of cluster sizes with cluster id as key and cluster size as value.
	"""
	sizes = defaultdict(int)
	for label in labels:
		if label in sizes.keys():
			sizes[label] += 1
		else:
			sizes[label] = 0
	return sizes


def load_mappings_from_model(mapping_inpath):
	"""
	Load mapping vectors from file.

	Args:
		mapping_inpath: Path mapping vector file.

	Returns:
		tuple: A tuple of a list of word index pairs and a dictionary (defaultdict) with index pair tuple as key
		and mapping vector (as numpy.array) as value.
	"""
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


def init_argparser():
	"""
	Initialize all possible arguments for the argument parser.

	Returns:
		:py:mod:`argparse.ArgumentParser`: ArgumentParser object with command line arguments for this script.
	"""
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
	argparser.add_argument('--min', type=int, help='Minimum number of neighbouring objects of a point to become a core object in DBSCAN')
	return argparser


if __name__ == "__main__":
	main()
