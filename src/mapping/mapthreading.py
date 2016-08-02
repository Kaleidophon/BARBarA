#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Module used to map a pair of vectors into a new combined vector space. Those mappings will be created by multiple
threads in a master-slave-pattern. To do so, the user can choose between different vector operations as offset, cosine
similarity, euclidean distance and many more.

.. warning::
	Because of :math:`\\Omega=\\frac{n(n-1)}{2}`, it is recommended to use the co-occurrence constraint
	:math:`\\Lambda`, which limits the calculations to word embedding pairs which words occurred together in a corpus in
	at least *n* sentences (but it will still take quite a while).
"""

# STANDARD
import argparse
import codecs
from collections import defaultdict
import os
import threading
import time
from Queue import Queue

# EXTERNAL
import numpy
from scipy.spatial.distance import euclidean, cityblock, cosine


def main():
	"""
	Main function that initializes the master thread with command line arguments and starts it.
	"""
	argparser = init_argparse()
	args = argparser.parse_args()
	print args
	master = MappingMasterThread(args.procs, args.input, args.output, args.features, args.cooc, args.ids, args.indices)
	master.start_threads()


class MappingMasterThread(threading.Thread):
	"""
	Master thread class. The master thread loads all necessary data into suitable data structures and distributes
	them among all worker threads.
	"""
	def __init__(self, n, vector_inpath, vector_outpath, features, lambda_, ids_inpath, indices_inpath):
		"""
		Master thread constructor.

		Args:
			n (int): Number of worker threads.
			vector_inpath (str): Path to word embeddings file.
			vector_outpath (str): Path mapping vectors should be written to.
			features (list): List of wanted features for the mapping vector.
			lambda_ (int): Value of lambda co-occurrence constraint.
			ids_inpath (str): Path to sentence IDs file. The file should be in the following `YAML`-format:
				- <word>:
					- <sentence id>
					- <sentence id>
					...
			indices_inpath (str): Path to file with the indices given to words. The file should have the following format:
			<index of word>	<word> (separated by tab)
		"""
		threading.Thread.__init__(self)

		# Paths
		self.vector_inpath = vector_inpath
		self.vector_outpath = vector_outpath
		self.ids_inpath = ids_inpath  # optional
		self.indices_inpath = indices_inpath  # optional

		# Initialize data structures
		self.vector_queue = Queue()
		self.vector_dict = VectorDict()
		self.occurrences = None  # optional
		self.indices = None  # optional

		# Set remaining variables
		self.features = features
		self.coc = True if ids_inpath else False
		self.ind = True if indices_inpath else False
		self.threads = []
		self.lambda_ = lambda_ if lambda_ else 100

		# Prepare dictionary, queue and occurrences
		print alt("Preparing vectors...")
		self.prepare()

		# Initialize workers
		print alt("Initializing %i worker threads..." % n)
		for i in range(int(n)):
			self.threads.append(MappingWorkerThread(i+1, self.vector_dict, self.vector_queue, self.vector_outpath,
													self.features, self.occurrences, self.indices, self.lambda_))

	def start_threads(self):
		"""
		Starts all the threads (and ends them if they're all finished).
		"""
		print alt("Starting threads...")
		for thread in self.threads:
			thread.start()

		for thread in self.threads:
			thread.join()
		print alt("Mapping finished!")

	def prepare(self):
		"""
		Loads The master thread loads all necessary data into suitable data structures. To be more specific,
		word embeddings, sentence IDs and word indices are processed.
		"""
		# Remove previous output file
		try:
			os.remove(self.vector_outpath)
		except OSError:
			pass

		# Load sentence IDs of words (if needed)
		if self.coc:
			print alt("Loading word occurrences...")
			self.occurrences = self.read_ids_file(self.ids_inpath)

		# Load a dictionary that maps indices to their corresponding words
		if self.ind:
			print alt("Loading word indices...")
			self.indices = defaultdict(unicode)
			with codecs.open(self.indices_inpath, "rb", "utf-8") as indices_infile:
				line = indices_infile.readline().strip()
				while line:
					line = indices_infile.readline().strip()
					while line:
						parts = line.split("\t")
						index = int(parts[0])
						word = parts[1]
						self.indices[index] = word
						line = indices_infile.readline().strip()

		# Load vectors
		print alt("Loading vectors...")
		with codecs.open(self.vector_inpath, "rb", "utf-8") as vector_infile:
			n = 1
			line = vector_infile.readline().strip()
			while line:
				if n == 1:
					line = vector_infile.readline().strip()
					n += 1
					continue
				parts = line.strip().split(" ")
				index = int(parts[0].strip())
				vector = numpy.array([float(dimension.strip()) for dimension in parts[1:]])

				# Only add those words which can possibly fulfill the lambda constraint.
				if len(self.occurrences[self.indices[index]]) >= self.lambda_:
					self.vector_queue.put((index, vector))
					self.vector_dict.add_vector(index, vector)
				n += 1
				line = vector_infile.readline().strip()

	def read_ids_file(self, ids_inpath):
		"""
		Read the sentence ID file.

		Args:
			ids_inpath (str): Path to sentence IDs file. The file should be in the following `YAML`-format:
				- <word>:
					- <sentence id>
					- <sentence id>
					...

		Returns:
			defaultdict: Dictionary with words as keys and the IDs of the sentences they occur in in a set as value.
		"""
		occurrences = defaultdict(set)
		with codecs.open(ids_inpath, "rb", "utf-8") as ids_infile:
			current_entity = None
			line = ids_infile.readline()
			while line:
				if line.startswith("\t-"):
					# Sentence id
					id = int(line.replace("-", "", 1).strip())
					occurrences[current_entity].add(id)
				else:
					# Named entitiy
					current_entity = line.split("\t")[0].strip()
					occurrences[current_entity] = set()
				line = ids_infile.readline()
		return occurrences


class MappingWorkerThread(threading.Thread):
	"""
	Worker thread class. The worker threads do all the dirty work after they receive all necessary data from the master
	thread an try to calculate every possible combinations of two word embeddings in a dataset.

	All the word embeddings will be stores in a dictionary (:py:class:`VectorDict` as well as a :py:class:`Queue`).
	An idle thread picks a new vector from the queue and then starts to iterate over all the vectors in the
	:py:class:`VectorDict` (this way, the queue gets shorter over time while the size of the dictionary stays fixed).

	Before it starts calculations, it checks a) if the co-occurrence constraint is satisfied and b) if this combination
	of word embeddings has already been processed.
	"""
	def __init__(self, worker_id, vector_dict, vector_queue, vector_outpath, features, occurrences, indices, lambda_):
		"""
		Constructor for worker thread.

		Args:
			worker_id (int): Unique thread ID.
			vector_dict (VectorDict): VectorDict object.
			vector_queue (Queue): Queue shared among all threads.
			vector_outpath (str): Path mapping vectors should be written to.
			features (list): List of wanted features for the mapping vector. The right function will be picked
				accordingly.
			occurrences (defaultdict): Dictionary with words as keys and the IDs of the sentences they occur in in a
				set as value.
			indices (defaultdict): Dictionary mapping word indices to actual words.
			lambda_ (int): Value of lambda co-occurrence constraint.
		"""
		threading.Thread.__init__(self)
		self.worker_id = worker_id
		self.vector_dict = vector_dict
		self.occurrences = occurrences  # optional
		self.indices = indices  # optional
		self.vector_queue = vector_queue
		self.vector_outpath = vector_outpath
		self.features = features
		self.lambda_ = lambda_  # Lambda co-occurrence constraint
		self.voperations = {'distvec': self.distance, 'eucldist1': self.euclidean_distance1,
							'eucldist2': self.euclidean_distance2, 'mandist': self.manhattan_distance,
							'cossim': self.cosine_similarity}

	def run(self):
		"""
		Starts a worker thread.
		"""
		print "Worker %i start!" % self.worker_id
		keys = self.vector_dict.get_keys()
		with codecs.open(self.vector_outpath, "a", "utf-8") as vector_outfile:
			while not self.vector_queue.empty():
				# print "Worker %i getting a new vector..." % self.worker_id
				current_index, current_vector = self.vector_queue.get()
				qsize = self.vector_queue.qsize()
				print alt("Queue size: %i (%.4f %%)" % (qsize, (qsize * 100.0 / len(keys))))

				current_word = self.indices[current_index]
				current_occ = self.occurrences[current_word]

				for index in keys:
					# Check whether this mapping has already been computed by
					# another thread
					if not self.vector_dict.skippable(index) and not current_index == index:
						# print "Worker %i processing %i - %i" %(self.worker_id, current_index, index)
						self.vector_dict.add_skippable(self.hash_indices(current_index, index))
						# Get the other vector
						comp_vector = self.vector_dict.get_vector(index)
						new_vector = numpy.array([])

						# Check whether this mapping would satisfy the
						# co-occurrence criterion (optional)
						word = self.indices[index]
						occ = self.occurrences[word]
						joint_occ = current_occ & occ
						if len(joint_occ) >= self.lambda_:
							# print "Worker %i calculating %i - %i" % (self.worker_id, current_index, index)
							# Compute new vector
							for operation in self.features:
								oresult = self.voperations[operation](current_vector, comp_vector)
								new_vector = numpy.append(new_vector, oresult)
								# Write result and mark combination as done
								vector_str = ' '.join([str(d) for d in new_vector.tolist()])
								vector_outfile.write(u'%i %i %s\n' % (current_index, index, vector_str))

	def distance(self, v1, v2):
		"""
		Return the vector offset of two vectors:

		Args:
			v1 (numpy.array): First vector
			v2 (numpy.array): Second vector

		Returns
			numpy.array: Vector offset.
		"""
		return v1 - v2

	def euclidean_distance1(self, v1, v2):
		"""
		Return the euclidean distance between two vectors.

		.. math::
			eucl(\\vec{a}, \\vec{b}) = \\sqrt{\\sum_{i=1}^n (\\vec{b}_i - \\vec{a}_i)^2}

		Args:
			v1 (numpy.array): First vector
			v2 (numpy.array): Second vector

		Returns:
			float: Euclidean distance between the two vectors.
		"""
		return euclidean(v1, v2)

	def euclidean_distance2(self, v1, v2):
		"""
		Returns the squared euclidean distance between two vectors.

		.. math::
			eucl2(\\vec{a}, \\vec{b}) = \\sum_{i=1}^n (\\vec{b}_i - \\vec{a}_i)^2

		Args:
			v1 (numpy.array): First vector
			v2 (numpy.array): Second vector

		Returns:
			float: Squared euclidean distance between the two vectors.
		"""
		return euclidean(v1, v2) ** 2

	def manhattan_distance(self, v1, v2):
		"""
		Returns the manhattan distance between two vectors.

		.. math::
			manhattan(\\vec{a}, \\vec{b}) = \\sum_{i=1}^n |\\vec{b}_i - \\vec{a}_i |

		Args:
			v1 (numpy.array): First vector
			v2 (numpy.array): Second vector

		Returns:
			float: Manhattan distance between the two vectors.
		"""
		return cityblock(v1, v2)

	def cosine_similarity(self, v1, v2):
		"""
		Calculates the cosine similarity (:math:`cos(\\vec{v}_1, \\vec{v}_2) \\in [-1,-1]`) between two vectors.

		Args:
			v1 (numpy.array): First vector
			v2 (numpy.array): Second vector

		Returns:
			float: Cosine similarity between the two vectors.
		"""
		return cosine(self, v1, v2)

	def soft_cosine_similarity(self, v1, v2):
		"""
		Calculates the soft cosine similarity between two vectors.

		.. math::
			S = \\begin{bmatrix}
				eucl(\\vec{a}_1, \\vec{b}_1) & \\ldots & eucl(\\vec{a}_1, \\vec{b}_n) \\\\
				\\vdots & \\ddots & \\vdots \\\\
				eucl(\\vec{a}_n, \\vec{b}_1) & \\ldots & eucl(\\vec{a}_n, \\vec{b}_n) \\\\
			\\end{bmatrix}

			softcos(\\vec{a}, \\vec{b}) = \\frac{\\sum_{i,j}^N S_{ij}\\vec{a}_i\\vec{b}_j}{\\sqrt{\\sum_{i,
			j}^N S_{ij}\\vec{a}_i\\vec{a}_j}\\sqrt{\\sum_{i,j}^N S_{ij}\\vec{b}_i\\vec{b}_j}}

		(It considers the similarity between pairs of features.)

		Args:
			v1 (numpy.array): First vector
			v2 (numpy.array): Second vector

		Returns:
			float: Soft cosine similarity between the two vectors.
		"""
		def similarity_sum(v1, v2):
			sim = 0
			for d1 in v1:
				for d2 in v2:
					sim += euclidean(d1, d2)
			return sim

		return similarity_sum(v1, v2) / ((similarity_sum(v1, v1)) ** (1 / 2) * (similarity_sum(v2, v2)) ** (1 / 2))

	def hash_indices(self, i1, i2):
		"""
		Combines two vector indices (the indices of the words' embeddings used in vector operations) into a hash
		s.t. threads can do an easy lookup if a mapping vector has already been calculated.
		To guarantee this, :math:`h(i_1, i_2) = h(i_2, i_1)` has to be the case.

		Args:
			i1 (int): Index of first word's embedding
			i2 (int): Index of second word's embedding

		Returns:
			int: Unique hash for index pair.
		"""
		return hash((i1, i2)) + hash((i2, i1))


class VectorDict(object):
	"""
	VectorDict class that serves two functions:
		| 1.) Storing word embeddings so they don't allocate memory for every worker thread
		| 2.) Providing a set, where are processed vector pairs are stored so no redundant computations are made.

	Locks are used for synchronization purposes.
	"""
	def __init__(self):
		"""
		VectorDict constructor.
		"""
		self.core_dict = defaultdict(numpy.array)
		self.lock = threading.Lock()
		self.skippables = set()

	def get_vector(self, index):
		"""
		Get a word embedding given its word's index.

		Args:
			index (int): Index of the word the embedding belongs to.

		Returns:
			numpy.array: Word embedding corresponding to given index.
		"""
		return self.core_dict[index]

	def add_vector(self, index, vector):
		"""
		Add a new word embedding.

		Args:
			index (int): Index of the word the embedding belongs to.
			vector (numpy.array): Word embedding corresponding to given index.
		"""
		self.core_dict[index] = vector

	def add_skippable(self, index_hash):
		"""
		Add the hash of an index pair to a set of already processed vector pairs.

		Args:
			index_hash (int): Hash value of index pair. Produced with :func:`hash_indices`.
		"""
		with self.lock:
			self.skippables.add(index_hash)

	def get_keys(self):
		"""
		Get all the keys (word embedding IDs) of this dictionary.

		Returns:
			list: List of word embedding IDs.
		"""
		return self.core_dict.keys()

	def skippable(self, index_hash):
		"""
		Checks whether a pair of vectors has already been processed.

		Args:
			index_hash (int): Hash value of index pair. Produced with :func:`hash_indices`.

		Returns:
			bool: Whether a pair of vectors has already been processed.
		"""

		if index_hash in self.skippables:
			return True
		return False


def init_argparse():
	"""
	Initialize all possible arguments for the argument parser.

	Returns:
		:py:mod:`argparse.ArgumentParser`: ArgumentParser object with command line arguments for this script.
	"""
	argparser = argparse.ArgumentParser()
	argparser.add_argument('--procs', type=int, required=True, help="Number of threads.")
	argparser.add_argument('--features', nargs='+', type=str, required=True,
							choices=['distvec', 'eucldist1', 'eucldist2', 'mandist', 'cossim', 'softcossim'],
							help='Mapping features. Included are:\n'
									'- distvec: Distance vector between a and b.\n'
									'- eukldist1: 1-norm euklidian distance.\n'
									'- eukldist2: 2-norm euklidian distance.\n'
									'- mandist: Manhatten distance.\n'
									'- cossim: Cosine similarity.\n'
									'- softcossim: "Soft" cosine similarity.\n')
	argparser.add_argument('--norm', action='store_true', help='Shorten mapping vector length to 1.')
	argparser.add_argument('--input', required=True, help='Input path to vector file.')
	argparser.add_argument('--output', required=True, type=str, help='Output path.')
	argparser.add_argument('--ids', type=str, help='(Optional) path to sentence ID file.')
	argparser.add_argument('--indices', type=str, help='(Optional) path to word indices file.')
	argparser.add_argument('--cooc', type=int, help='Co-occurrence constraint lambda.')
	return argparser


def alt(func):
	"""
	Prepends the local time to the output of a function.

	Args:
		func (function): Function the local time should be prepended to.
	"""
	return "%s: %s" % (time.strftime("%H:%M:%S", time.gmtime()), func)


if __name__ == "__main__":
	main()
