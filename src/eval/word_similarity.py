import threading
import codecs
import math
from scipy.stats import pearsonr
from scipy.spatial.distance import cosine
from concentration import load_vectors_from_model, load_vectors_from_model_parallel, alt
import Queue


class WordSimMasterThread(threading.Thread):
	def __init__(self, n, vector_inpath, wordpair_inpath, logpath, format):
		threading.Thread.__init__(self)
		self.n = n
		self.vector_inpath = vector_inpath
		self.wordpair_inpath = wordpair_inpath
		self.logpath = logpath
		self.format = format
		self.model = None
		self.pairs = None
		self.x = None
		self.y = None
		self.global_error_count = 0
		self.threads = []
		self.pair_queue = Queue.Queue()

		self.prepare()
		for i in range(self.n):
			self.threads.append(WordSimWorkerThread(i, self.pair_queue, self.model, self.y))

	def start_threads(self):
		output(alt("Starting %i wordsim threads...\n" % (self.n)), self.logpath)
		for thread in self.threads:
			thread.start()

		for thread in self.threads:
			thread.join()
			self.global_error_count += thread.error_count

		output(alt("Removing errors...\n"), self.logpath)
		self.remove_unknowns()

		output(alt("Calculating results...\n"), self.logpath)
		rho, t, z = evaluate_wordpair_sims(self.x, self.y, len(self.pairs))

		successful_pairs = len(self.pairs) - self.global_error_count
		successful_percentage = (len(self.pairs) - self.global_error_count * 1.0) / len(self.pairs) * 100.0
		# Write on screen or into logfile
		output(alt("Calculated Pearman's rho for %i pairs (%.2f %%).\n\tr = %.4f\n\tt = %.4f\n\tz = %.4f\n")
					% (successful_pairs, successful_percentage, rho, t, z), self.logpath)

	def prepare(self):
		output(alt("Loading model...\n"), self.logpath)
		self.model = load_vectors_from_model_parallel(self.vector_inpath, self.n, self.logpath)[1]
		output(alt("Loading word pairs...\n"), self.logpath)
		self.pairs, self.x = read_wordpairs(self.wordpair_inpath, self.format)
		output(alt("Preparing queue...\n"), self.logpath)
		for i in range(len(self.pairs)):
			self.pair_queue.put((i, self.pairs[i]))
		self.y = [None] * len(self.pairs)

	def remove_unknowns(self):
		to_pop = []
		for i in range(len(self.y)):
			if not self.y[i]:
				to_pop.append(i)

		# Avoid popping elements while iterating through list
		to_pop.reverse()
		for i in to_pop:
			self.x.pop(i)
			self.y.pop(i)


class WordSimWorkerThread(threading.Thread):
	def __init__(self, worker_id, pair_queue, model, y):
		threading.Thread.__init__(self)
		self.worker_id = worker_id
		self.pair_queue = pair_queue
		self.model = model
		self.y = y
		self.error_count = 0

	def run(self):
		while not self.pair_queue.empty():
			pair_id, pair = self.pair_queue.get()
			try:
				a = self.model[capitalize(pair[0])]
				b = self.model[capitalize(pair[1])]
				sim = cosine(a, b)
				self.y[pair_id] = sim
			except TypeError:
				self.error_count += 1


def parallel_word_sim_eval(vector_inpath, wordpair_path, logpath, format="google", threads=1):
	master = WordSimMasterThread(threads, vector_inpath, wordpair_path, logpath, format)
	master.start_threads()


def word_sim_eval(vector_inpath, wordpair_path, logpath, format="google"):
	output(alt("Loading model...\n"), logpath)
	model = load_vectors_from_model(vector_inpath)[1]
	y = []
	error_counter = 0

	# Read word pairs with values
	output(alt("Loading word pairs...\n"), logpath)
	pairs, x = read_wordpairs(wordpair_path, format)

	# Calculate similarity values for pairs
	output(alt("Calculating word pair similarities...\n"), logpath)
	for pair in pairs:
		try:
			a = model[capitalize(pair[0])]
			b = model[capitalize(pair[1])]
			sim = cosine(a, b)
			y.append(sim)
		except TypeError:
			x.pop(pairs.index(pair))
			error_counter += 1

	rho, t, z = evaluate_wordpair_sims(x, y, len(pairs))

	successful_pairs = len(pairs) - error_counter
	successful_percentage = (len(pairs) - error_counter * 1.0) / len(pairs) * 100.0
	# Write on screen or into logfile
	output(alt("Calculated Pearman's rho for %i pairs (%.2f %%).\n\tr = %.4f\n\tt = %.4f\n\tz = %.4f\n")
			% (successful_pairs, successful_percentage, rho, t, z), logpath)


def capitalize(word):
	return word[0].upper() + word[1:]


def evaluate_wordpair_sims(x, y, number_of_pairs):
	# Calculate Pearman's rho
	rho = pearsonr(x, y)[0]

	rho_ = math.fabs(rho)
	# Calculate rho's significance
	t = ((number_of_pairs - 2) / (1 - rho_ ** 2)) ** (1 / 2) * rho_  # Student t
	# test
	z = ((number_of_pairs - 3) / 1.06) ** (1 / 2) * 1 / 2 * math.log((1 + rho_) / (1 - rho_))
	return rho, t, z


def read_wordpairs(wordpair_path, format="google"):
	pairs, x = [], []
	with codecs.open(wordpair_path, "rb", "utf-8") as wordpair_file:
		if format == "google":
			for line in wordpair_file:
				line_parts = line.strip().split()
				word_pair = line_parts[0].split("-")
				pairs.append((word_pair[0], word_pair[1]))
				x.append(float(line_parts[1]))
		elif format == "semrel":
			for line in wordpair_file:
				if line.startswith('#'):
					continue
				line_parts = line.strip().split(":")
				pairs.append((line_parts[0], line_parts[1]))
				x.append(float(line_parts[2]))
	return pairs, x


def output(message, logpath=None):
	if not logpath:
		print rreplace(message, "\n", "", 1)
	else:
		with codecs.open(logpath, "a", "utf-8") as logfile:
			logfile.write(message)


def rreplace(s, old, new, occurrence):
	li = s.rsplit(old, occurrence)
	return new.join(li)
