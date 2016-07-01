#!/usr/bin/python
# -*- coding: utf-8 -*-

import numpy as np
import argparse
import os
import codecs

from sklearn.decomposition import PCA
import matplotlib.pyplot as plt
import matplotlib.font_manager
from mpl_toolkits.mplot3d import axes3d, Axes3D
from gensim.models import Word2Vec as w2v

from concentration import calculate_concentration, load_vectors_from_model, calculate_loss_of_precision
from analogy import analogy_eval, analogy_eval_parallel
from word_similarity import word_sim_eval, parallel_word_sim_eval


def main():
	argparser = init_argparser()
	args = argparser.parse_args()

	# Evaluates word embedding with analogy task
	if args.mode == "analogy":
		if args.procs > 1:
			apply_on_input(analogy_eval_parallel,
			               args.sets,
			               args.input[0],
			               args.input[1],
			               args.sectionwise,
			               args.log,
			               args.procs)
		else:
			apply_on_input(analogy_eval,
							args.sets,
							args.input[0],
							args.input[1],
							args.sectionwise,
							args.log)

	elif args.mode == "wordsim":
		if args.procs > 1:
			apply_on_input(parallel_word_sim_eval,
							args.sets,
							args.input[0],
							args.input[1],
                            args.log,
							args.format,
							args.procs)
		else:
			apply_on_input(word_sim_eval,
							args.sets,
							args.input[0],
							args.input[1],
                            args.log,
							args.format)

	# Calculates 'concentration' value for a set of word embeddings
	elif args.mode == "concentration":
		apply_on_input(calculate_concentration,
						args.sets,
						load_vectors_from_model(args.input,
												args.max,
												args.log),
						args.procs,
						args.log,
						args.input[0])

	# Visualizes a set of word embeddings in a 2D or 3D projection
	elif args.mode == "visualize":
		apply_on_input(plot,
						args.sets,
						args.input[0],
						args.max,
						args.dimensions,
						args.show,
						args.display)

	# Plots the distribution of distances between word embeddings
	elif args.mode == "distance_distr":
		apply_on_input(plot_distance_distribution,
						args.input[0],
						args.max,
						args.show)

	# Calculate loss of precision for different model sizes
	elif args.mode == "loss":
		calculate_loss_of_precision(args.input[0],
									args.procs,
									args.sizes,
									args.log)

	# Find the n nearest neighbors of a list of words given a dataset
	elif args.mode == "neighbors":
		find_nearest_neighbors(args.input[0],
								args.max,
								args.words)


def find_nearest_neighbors(vector_inpath, max, wordlist):
	print "Loading vectors...."
	model = w2v.load_word2vec_format(vector_inpath, binary=False)
	print wordlist
	for word in wordlist:
		most_similar_with_score = model.most_similar(positive=[word], topn=max)
		for v in most_similar_with_score:
			print v
		most_similar_words = [pair[0] for pair in most_similar_with_score]

		print u"%i most similar words of %s in dataset %s" %(max, word, vector_inpath)
		for i in range(len(most_similar_words)):
			print u"%i: %s" %(i+1, most_similar_words[i])


def plot(data, max, dimensions, show_plot=False, display_names=False):
	font = matplotlib.font_manager.FontProperties(fname='./ipag.ttc')
	FONT_SIZE = 8
	TEXT_KW = dict(fontsize=FONT_SIZE, fontweight='bold', fontproperties=font)
	dimensions = int(dimensions)
	words, model = load_vectors_from_model(data, max, indices=True)
	words = words[50:]

	# do PCA
	X = [model[key] for key in words]
	vector_length = X[0].shape[0]
	if dimensions == 2:
		xs = ys = None
		if dimensions < vector_length:
			pca = PCA(n_components=2)
			pca.fit(X)
			print pca.explained_variance_ratio_
			X = pca.transform(X)
			xs = X[:, 0]
			ys = X[:, 1]
		else:
			xs = [x[0] for x in X]
			ys = [x[1] for x in X]
		plt.figure()
		plt.scatter(xs, ys)
		if display_names:
			for i, w in enumerate(words):
				plt.annotate(w,
							 xy=(xs[i], ys[i]), xytext=(3, 3),
							 textcoords='offset points', ha='left', va='top',
							 **TEXT_KW)
	elif dimensions == 3:
		if dimensions < vector_length:
			pca = PCA(n_components=3)
			pca.fit(X)
			print pca.explained_variance_ratio_
			X = pca.transform(X)
		xs = X[:, 0]
		ys = X[:, 1]
		zs = X[:, 2]
		fig = plt.figure()
		ax = Axes3D(fig)
		ax.scatter(xs, ys, zs)

	plt.savefig("./fig.jpg", format="jpg")
	if show_plot:
		plt.show()


def plot_distance_distribution(data, max, show_plot=False):
	word_list, vector_dict = load_vectors_from_model(data, max)
	distance_list = []

	breaking = False
	while True:
		current_word = word_list.pop(0)
		if not current_word: break
		current_vector = vector_dict[current_word]
		for word in vector_dict.keys():
			if word == current_word: continue
			current_distance = np.linalg.norm(vector_dict[word] -
											  current_vector)
			distance_list.append(current_distance)
			if len(word_list) == 0:
				breaking = True
				break
		if breaking: break

	# print distance_list

	n, bins, patches = plt.hist(distance_list, alpha=0.5, bins=int(max ** (1.0 / 2)))
	# add a 'best fit' line
	plt.xlabel('Length of distances')
	plt.ylabel('# of distances')
	plt.title("Distribution of distance lengths between %i points\n(%i "
			  "connections)" %
			  (max, max * (max - 1) / 2))

	# Tweak spacing to prevent clipping of ylabel
	plt.subplots_adjust(left=0.15)

	plt.savefig("./hist.jpg", format="jpg")
	if show_plot:
		plt.show()


def opt_callback(option, opt, value, parser):
	setattr(parser.values, option.dest, [int(v) for v in value.split(',')])


def apply_on_input(func, sets, inpath, *args):
	if sets:
		inpaths = [inpath + path for path in os.listdir(inpath)]
		print inpaths
		for infile in inpaths:
			func(infile, *args)
	else:
		func(inpath, *args)


def output(message, logpath=None):
	if not logpath:
		print rreplace(message, "\n", "", 1)
	else:
		with codecs.open(logpath, "a", "utf-8") as logfile:
			logfile.write(message)


def rreplace(s, old, new, occurrence):
	li = s.rsplit(old, occurrence)
	return new.join(li)


def init_argparser():
	argparser = argparse.ArgumentParser()
	argparser.add_argument('--input',
							nargs='+',
							required=True,
							help='Evaluation inputs (Path to vector file / '
								 'additional data')
	argparser.add_argument('--mode',
							required=True,
							help='Mode. Different depending on evaluation '
								 'method.')
	argparser.add_argument('--procs',
							default=1,
							type=int,
							help='Number of processes. WARNING: Broken.')
	argparser.add_argument('--log',
							default=None,
							help='Path to log file.')
	argparser.add_argument('--max',
							default=None,
							type=int,
							help='Maximum numbers of vectors to load / number of most similar neigbors to find.')
	argparser.add_argument('--sizes',
							help='Model sizes to calculate loss of precision.')
	argparser.add_argument('--dimensions',
							type=int,
							choices=[2, 3],
							help='Number of dimensions for visualization.')
	argparser.add_argument('--show',
							action='store_true',
							help='Show (interactive) version of plot')
	argparser.add_argument('--display',
							action='store_true',
							help='Display words belonging to points in figure.')
	argparser.add_argument('--sets',
							action='store_true',
							help='Perform an action on multiple data sets.')
	argparser.add_argument('--format',
							choices=['google', 'semrel'],
							default='google',
							help='File format for word similarity evaluation '
							     'file.')
	argparser.add_argument('--words',
							nargs='+',
							help="List words the nearest neigbors should be found for.")
	argparser.add_argument('--sectionwise',
							action='store_true',
							default=False,
							help='Evaluate with analogies sectionswise.')
	return argparser


if __name__ == "__main__":
	main()
