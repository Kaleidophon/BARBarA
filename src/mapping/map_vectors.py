import argparse
import pysash
from gensim.models import Word2Vec as w2v
import numpy
import time
import codecs
from collections import defaultdict, Set
import cPickle as pickle
import sys, os
from multiprocessing import Pool


def main():
	argparser = init_argparse()
	args = argparser.parse_args()
	print args
	if args.mode == "map":
		pass
	elif args.mode == "index":
		if len(args.input) == 1:
			index_vectors(args.input[0], args.output[0], args.output[1])
		elif len(args.input) == 2:
			index_vectors(args.input[0], args.output[0], args.output[1], args.input[1])
	elif args.mode == "filter":
		filter_duplicate_vectors(args.input[0], args.output[0])


def init_argparse():
	argparser = argparse.ArgumentParser()
	argparser.add_argument('--mode',
							type=str,
							choices=['map', 'index', 'filter'],
							help='Mode to execute script in.')
	argparser.add_argument('--features',
							nargs='+',
							type=str,
							choices=['distvec', 'eukldist1',
									'eukldist2', 'mandist',
									'cossim', 'softcossim'],
							help='Mapping features. Included are:\n'
								'- distvec: Distance vector between a and b.\n'
								'- eukldist1: 1-norm euklidian distance.\n'
								'- eukldist2: 2-norm euklidian distance.\n'
								'- mandist: Manhatten distance.\n'
								'- cossim: Cosine similarity.\n'
								'- softcossim: "Soft" cosine similarity.')
	argparser.add_argument('--input',
							nargs='+',
							required=True,
							help='Evaluation inputs (Path to vector file / additional data')
	argparser.add_argument('--output',
							type=str,
							nargs='+',
							help='Output path(s).')
	return argparser


def filter_duplicate_vectors(vectors_indir, vector_outpath):
	# TODO: Testen?
	all_indices = Set([])
	vector_inpaths = os.listdir(vectors_indir)
	with codecs.open(vector_outpath, "wb", "utf-8") as vector_outfile:
		for vector_inpath in vector_inpaths:
			with codecs.open(vector_inpath, "rb", "utf-8") as infile:
				line = infile.readline().strip()
				while line:
					hashed_indices = hash_tuple(current_indices)
					if hashed_indices not in all_indices:
						vector_outfile.write("%s\n" %(line))
						all_indices.add(hashed_indices)
					line = infile.readline().strip()


def filter_duplicate_vectors_parallelized(vectors_indir, vector_outpath, procs=1):
	# TODO: Testen?

	def _fdvp(vector_inpath, vector_outpath):
		with codecs.open(vector_outpath, "a", "utf-8") as vector_outfile:
			with codecs.open(vector_inpath, "rb", "utf-8") as vector_infile:
				line = vector_infile.readline().strip()
				while line:
					current_indices = tuple([int(index) for index in line.split(" ")[:2]])
					r_current_indices = (current_indices[1], current_indices[0])
					if current_indices not in all_indices_global and \
						r_current_indices not in all_indices_global:
						vector_outfile.write("%s\n" %(line))
						all_indices_global.add(current_indices)
					line = vector_infile.readline().strip()

	vector_inpaths = os.listdir(vectors_indir)

	pool = Pool(processes=len(procs), initializer=init_pool, initargs=Set([]))
	pool.map(_fdvp, vector_inpaths)


def hash_tuple(t):
	return hash(t) + hash((t[1], t[0]))


def init_pool(args):
	global all_indices_global
	all_indices_global = args


def construct_nearest_neighbour_graph(vector_inpath):
	def f(l1, l2):
		s1 = set(l1)
		s2 = set(l2)
		r = 1 - (float(len(s1.intersection(s2)))) / len(s1.union(s2))
		return r

	print "Load model..."
	model = load_vectors_from_model(vector_inpath)
	print "Extract vectors..."
	vectors = model[1]
	base = numpy.zeros((len(vectors.keys()),), dtype=object)
	i = 0
	for key in vectors.keys():
		base[i] = vectors[key].astype(numpy.float32)
		i += 1
	sash = pysash.GenericSash(f)
	print "Build SASH..."
	# sash.build(model.astype(numpy.float32))
	sash.build(base)
	print "Save SASH..."
	sash.save(vector_inpath)


def index_vectors(vector_inpath, vector_outpath, indexing_outpath, subset):
	print alt("Read vocabulary subset...")
	subset = read_subset(subset)
	print len(subset)

	index = 0
	vector_dict = defaultdict(numpy.array)
	indexing_dict = defaultdict(str)

	print alt("Read vectors...")
	model = load_vectors_from_model(vector_inpath)
	print type(model)
	vectors = model[1]
	print type(vectors)
	del model

	print alt("Index vectors and create subset...")
	for word in subset:
		try:
			current_vector = vectors[word]
			vector_dict[index] = current_vector
			indexing_dict[index] = word
			index += 1
		except:
			continue

	print alt("Dump vectors...")
	dump_vector_defaultdict(vector_dict, vector_outpath, False)
	print alt("Dump indices...")
	dump_defaultdict(indexing_dict, indexing_outpath, False)
	sys.exit(0)


def load_vectors_from_model(vector_inpath, max_n=None, logpath=None):
	if max_n:
		output(alt(
			"Start loading %i vectors in %s....\n" % (max_n, vector_inpath)),
		       logpath)
	elif not max_n:
		output(alt("Start loading all vectors in %s....\n" % (vector_inpath)),
		       logpath)
	loading_starttime = time.time()
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
			if max_n:
				if n == max_n:
					break
			n += 1
	loading_endtime = time.time()
	m, s = divmod(loading_endtime - loading_starttime, 60)
	output(alt("Loading of %s complete! Loading took %2dm %2ds.\n" % (
	vector_inpath, m, s)), logpath)
	return word_list, vector_dict


def output(message, logpath=None):
	if not logpath:
		print rreplace(message, "\n", "", 1)
	else:
		with codecs.open(logpath, "a", "utf-8") as logfile:
			logfile.write(message)


def read_subset(subset_inpath):
	subset = []
	with codecs.open(subset_inpath, "rb", "utf-8") as subset_infile:
		line = subset_infile.readline().strip()
		while line != "":
			subset.append(line)
			line = subset_infile.readline().strip()
	return subset


def alt(func):
	return "%s: %s" % (time.strftime("%H:%M:%S", time.gmtime()), func)


def rreplace(s, old, new, occurrence):
	li = s.rsplit(old, occurrence)
	return new.join(li)


def dump_vector_defaultdict(ddict, outpath, pickled=True):
	if pickled:
		with open(outpath, "wb") as pickle_file:
			pickle.dump(ddict, pickle_file)
	else:
		with codecs.open(outpath, "wb", "utf-8") as outfile:
			outfile.write(u"%i %i\n" %(len(ddict.keys()), 100))
			for key in ddict.keys():
				vectorstring = " ".join([str(d) for d in ddict[key].tolist()])
				outfile.write(u"{key} {value}\n".format(key=key, value=vectorstring))


def dump_defaultdict(ddict, outpath, pickled=True):
	if pickled:
		with open(outpath, "wb") as pickle_file:
			pickle.dump(ddict, pickle_file)
	else:
		with codecs.open(outpath, "wb", "utf-8") as outfile:
			for key in ddict.keys():
				outfile.write(u"{key}\t{value}\n".format(key=key, value=ddict[key]))


if __name__ == '__main__':
	main()
