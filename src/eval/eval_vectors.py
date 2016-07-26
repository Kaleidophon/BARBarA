#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Main module used to evaluate word embeddings. It offers the following options:
	| 1.) Analogy: The system tries to complete an analogy like "W is to X like Y is to...?" The percentage
		of correct answers is measured.
	| 2.) Word similarity: The system assign word pairs a similarity score based on the cosine similarity of their
		word embeddings. Then, to correlation between those and human ratings is measured with Pearson's rho.
	| 3.) Nearest neighbors: Find the nearest neighbors for a list of words based on their word embeddings. Good for
		a first look on the data, but not quantifiable.
	| 4.) Visualize: Plot word embeddings in 2D or 3D. Fancy plots. Yay!
"""
# STANDARD
import argparse

# EXTERNAL
import matplotlib.font_manager
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
from gensim.models import Word2Vec as w2v
from sklearn.decomposition import PCA

# PROJECT
from src.eval.analogy import analogy_eval
from src.eval.word_similarity import word_sim_eval
from src.misc.helpers import load_vectors_from_model


def main():
	"""
	This is the main function. It uses the parsed command line arguments, especially `--mode`, to pick the right
	function to execute.
	"""
	argparser = init_argparser()
	args = argparser.parse_args()

	# Evaluates word embeddings with analogy task
	if args.mode == "analogy":
		analogy_eval(args.input[0], args.input[1], args.sectionwise)

	# Evaluate word embeddings with word similarity task
	elif args.mode == "wordsim":
		word_sim_eval(args.input[0], args.input[1], args.log, args.format)

	# Visualizes a set of word embeddings in a 2D or 3D projection
	elif args.mode == "visualize":
		plot(args.input[0], args.max, args.dimensions, args.show, args.display)

	# Find the n nearest neighbors of a list of words given a dataset
	elif args.mode == "neighbors":
		find_nearest_neighbors(args.input[0], args.max, args.words)


def find_nearest_neighbors(vector_inpath, max_n, wordlist):
	"""
	Find the nearest neighbors for a list of words based on their word embeddings.

	Args:
		vector_inpath (str): Path to vector file. File has to have the following format (separated by spaces):
			<index of original vector #1> <index of original vector #2> <Dimension 1> ... <Dimension n>
		max_n (int): Number of nearest neighbors that should be determined.
		wordlist (list): List of words nearest neighbors should be found for.
	"""
	print "Loading vectors...."
	model = w2v.load_word2vec_format(vector_inpath, binary=False)
	print wordlist

	# Find nearest neighbors
	for word in wordlist:
		most_similar_with_score = model.most_similar(positive=[word], topn=max_n)
		for v in most_similar_with_score:
			print v
		most_similar_words = [pair[0] for pair in most_similar_with_score]  # Only use words, not scores

		# Print results
		print u"%i most similar words of %s in dataset %s" %(max_n, word, vector_inpath)
		for i in range(len(most_similar_words)):
			print u"%i: %s" %(i+1, most_similar_words[i])


def plot(vector_inpath, max_n, target_dim, show_plot=False, display_names=False):
	"""
	Plot word embeddings in 2D or 3D. As a heuristic, word will only be plotted after the 50th most frequent words to
	avoid plotting boring stop words.

	Args:
		vector_inpath (str): Path to vector file. File has to have the following format (separated by spaces):
			<index of original vector #1> <index of original vector #2> <Dimension 1> ... <Dimension n>
		max_n (int): Maximum number of vectors to be plotted.
		show_plot (bool): Flag to indicate whether a window with the (interactive) plot should pop up after executing
			the script.
		display_names (bool): Flag to indicate whether the words should acutally be shown next to the data point in
			the plot. Can get very messy with higher `max_n`.
	"""
	# Configure plot properties
	font = matplotlib.font_manager.FontProperties(fname='./ipag.ttc')
	FONT_SIZE = 8
	TEXT_KW = dict(fontsize=FONT_SIZE, fontweight='bold', fontproperties=font)
	dimensions = int(target_dim)

	# Load vectors
	words, model = load_vectors_from_model(vector_inpath, max, indices=True)
	words = words[50:50+max_n]

	# do PCA
	X = [model[key] for key in words]
	vector_length = X[0].shape[0]

	# Plot in two dimensions
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

	# Plot in three dimensions
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


def init_argparser():
	"""
	Initialize all possible arguments for the argument parser.

	Returns:
		:py:mod:`argparse.ArgumentParser`: ArgumentParser object with command line arguments for this script.
	"""
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
