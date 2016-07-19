import threading
import Queue
import codecs
from collections import defaultdict
import numpy
from scipy.spatial.distance import euclidean, cityblock, cosine
import argparse
import time
import os
from math import log


def main():
	argparser = init_argparse()
	args = argparser.parse_args()
	master = MappingMasterThread(args.procs, args.input, args.output, args.features, args.ids, args.indices)
	master.start_threads()


class MappingMasterThread(threading.Thread):
	def __init__(self, n, vector_inpath, vector_outpath, features, ids_inpath, indices_inpath):
		threading.Thread.__init__(self)

		# Paths
		self.vector_inpath = vector_inpath
		self.vector_outpath = vector_outpath
		self.ids_inpath = ids_inpath  # optional
		self.indices_inpath = indices_inpath  # optional

		# Initialize data structures
		self.vector_queue = Queue.Queue()
		self.vector_dict = VectorDict()
		self.occurrences = None  # optional
		self.indices = None  # optional

		# Set remaining variables
		self.features = features
		self.coc = True if ids_inpath else False
		self.ind = True if indices_inpath else False
		self.threads = []

		# Prepare dictionary, queue and occurrences
		print alt("Preparing vectors...")
		self.prepare()

		# Initialize workers
		print alt("Initializing %i worker threads..." % (n))
		for i in range(n):
			self.threads.append(MappingWorkerThread(n, self.vector_dict, self.vector_queue, self.vector_outpath,
													self.features, self.occurrences, self.indices))

	def start_threads(self):
		print alt("Starting threads...")
		for thread in self.threads:
			thread.start()

		for thread in self.threads:
			thread.join()
		print alt("Mapping finished!")

	def prepare(self):
		# Remove previous output file
		try:
			os.remove(self.vector_outpath)
		except OSError:
			pass

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
				self.vector_queue.put((index, vector))
				self.vector_dict.add_vector(index, vector)
				n += 1
				line = vector_infile.readline().strip()

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

	def read_ids_file(self, ids_inpath):
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
	def __init__(self, worker_id, vector_dict, vector_queue, vector_outpath, features, occurrences, indices):
		threading.Thread.__init__(self)
		self.worker_id = worker_id
		self.vector_dict = vector_dict
		self.occurrences = occurrences  # optional
		self.indices = indices  # optional
		self.vector_queue = vector_queue
		self.vector_outpath = vector_outpath
		self.features = features
		self.voperations = {'distvec': self.distance, 'eucldist1': self.euclidian_distance1,
							'eucldist2': self.euclidian_distance2, 'mandist': self.manhattan_distance,
							'cossim': self.cosine_similarity, 'concat': self.concat, 'spray': self.spray}

	def run(self):
		keys = self.vector_dict.get_keys()
		with codecs.open(self.vector_outpath, "a", "utf-8") as vector_outfile:
			while not self.vector_queue.empty():
				current_index, current_vector = self.vector_queue.get()
				for index in keys:
					# Check whether this mapping has already been computed by
					# another thread
					if not self.vector_dict.skippable(index) and not current_index == index:
						# Check whether this mapping would satisfy the
						# co-occurrence
						# criterion (optional)
						#if self.occurrences:
						#	current_word = self.indices[current_index]
						#	word = self.indices[index]
						#	current_occ = self.occurrences[current_word]
						#	occ = self.occurrences[word]
						#	joint_occ = current_occ & occ
							#try:
							#	if log(len(joint_occ) * 1.0 / (len(occ) * 1.0 * len(current_occ))) < -11:
							#		self.vector_dict.add_skippable(self.hash_indices(current_index, index))
							#		continue
							#except:
							#	self.vector_dict.add_skippable(self.hash_indices(current_index, index))
							#	continue

						# Get the other vector
						comp_vector = self.vector_dict.get_vector(index)
						new_vector = numpy.array([])

						# Compute new vector
						for operation in self.features:
							current_word = self.indices[current_index]
							word = self.indices[index]
							current_occ = self.occurrences[current_word]
							occ = self.occurrences[word]
							joint_occ = current_occ & occ
							if len(joint_occ) >= 100:
								oresult = self.voperations[operation](current_vector, comp_vector)
								new_vector = numpy.append(new_vector, oresult)

						# Write result and mark combination as done
						if not self.vector_dict.skippable(index):
							vector_outfile.write(u'%i %i %s\n' % (current_index, index,
																' '.join([str(d) for d in new_vector.tolist()])))
						self.vector_dict.add_skippable(self.hash_indices(current_index, index))

	def distance(self, v1, v2):
		return v1 - v2

	def euclidian_distance1(self, v1, v2):
		return euclidean(v1, v2)

	def euclidian_distance2(self, v1, v2):
		return euclidean(v1, v2) ** 2

	def manhattan_distance(self, v1, v2):
		return cityblock(v1, v2)

	def cosine_similarity(self, v1, v2):
		return cosine(v1, v2)

	def concat(self, v1, v2):
		return numpy.concatenate((v1, v2))

	def spray(self, v1, v2, cooc):
		return (v1 - v2) * cooc ** (1.0/2)

	def hash_indices(self, i1, i2):
		return hash((i1, i2)) + hash((i2, i1))


class VectorDict(object):
	def __init__(self):
		self.core_dict = defaultdict(numpy.array)
		self.lock = threading.Lock()
		self.skippables = set()

	def get_vector(self, index):
		return self.core_dict[index]

	def add_vector(self, index, vector):
		with self.lock:
			self.core_dict[index] = vector

	def add_skippable(self, index):
		with self.lock:
			self.skippables.add(index)

	def get_keys(self):
		return self.core_dict.keys()

	def skippable(self, index_hash):
		if index_hash in self.skippables:
			return True
		return False


def init_argparse():
	argparser = argparse.ArgumentParser()
	argparser.add_argument('--procs', type=int, required=True, help="Number of threads.")
	argparser.add_argument('--features', nargs='+', type=str, required=True,
							choices=['distvec', 'eucldist1', 'eucldist2', 'mandist', 'cossim', 'softcossim', 'co-occ', 'concat', 'spray'],
							help='Mapping features. Included are:\n'
									'- distvec: Distance vector between a and b.\n'
									'- eukldist1: 1-norm euklidian distance.\n'
									'- eukldist2: 2-norm euklidian distance.\n'
									'- mandist: Manhatten distance.\n'
									'- cossim: Cosine similarity.\n'
									'- softcossim: "Soft" cosine similarity.\n'
									'- concat: vector concatenation\n')
	argparser.add_argument('--norm', action='store_true', help='Shorten mapping vector length to 1.')
	argparser.add_argument('--input', required=True, help='Input path to vector file.')
	argparser.add_argument('--output', required=True, type=str, help='Output path.')
	argparser.add_argument('--ids', type=str, help='(Optional) path to sentence ID file.')
	argparser.add_argument('--indices', type=str, help='(Optional) path to word indices file.')
	return argparser


def alt(func):
	return "%s: %s" % (time.strftime("%H:%M:%S", time.gmtime()), func)


if __name__ == "__main__":
	main()
