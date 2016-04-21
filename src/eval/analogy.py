from gensim.models import Word2Vec as w2v
import codecs, sys
import threading
import Queue
from concentration import alt, load_vectors_from_model_parallel
from numpy import cos


def analogy_eval_parallel(vector_inpath, analogy_path, per_section=False, logpath=None, threads=1):
	master = AnalogyMasterThread(vector_inpath, analogy_path, per_section, logpath, threads)
	# master.start_threads()


class AnalogyMasterThread(threading.Thread):
	def __init__(self, vector_inpath, analogy_path, per_section, logpath, n):
		threading.Thread.__init__(self)
		self.vector_inpath = vector_inpath
		self.analogy_path = analogy_path
		self.per_section = per_section
		self.logpath = logpath
		self.n = n
		self.threads = []
		self.queues = None
		self.model = None

		self.prepare()
		output(alt("Initialize %i threads...\n" %(n)), self.logpath)
		for i in range(self.n):
			self.threads.append(AnalogyWorkerThread(i, self.model))

	def start_threads(self):
		total_n = 0
		total_fails = 0
		total_errors = 0
		current_n = 0
		current_fails = 0
		current_errors = 0
		result_outputs = []
		output(alt("Starting threads...\n"), self.logpath)

		first_queue_pair = self.queues.pop()
		first_queue_title = first_queue_pair.keys()[0]
		first_queue = first_queue_pair[first_queue_title]

		for thread in self.threads:
			thread.current_queue = first_queue
			thread.start()

		distribute_queues_lock = threading.Lock()

		while len(self.queues) > 0:
			current_queue_pair = self.queues.pop()
			current_title = current_queue_pair.keys()[0]
			current_queue = current_queue_pair[current_title]
			with distribute_queues_lock:
				for thread in self.threads:
					thread.current_queue = current_queue
					total_n += thread.n
					total_fails += thread.fail
					total_errors += thread.errors
					current_n = thread.n
					current_fails = thread.fail
					current_errors = thread.errors
					thread.n = 0
					thread.fails = 0
					thread.errors = 0
				accuracy = (current_n - current_fails) * 1.0 / current_n * 100.0
				result_outputs.append("Accuracy in section %s: %.2f %%. %i KeyErrors." % (current_title, accuracy, current_errors))

		for thread in self.threads:
			thread.finished = True

		for thread in self.threads:
			thread.join()

		for result_output in result_outputs:
			print result_output

	def prepare(self):
		output(alt("Read analogies...\n"), self.logpath)
		self.queues = read_analogies_for_parallel(self.analogy_path, self.per_section)
		self.queues.reverse()
		output(alt("Load model...\n"), self.logpath)
		# self.model = w2v.load_word2vec_format(self.vector_inpath, binary=False)
		self.model = load_vectors_from_model_parallel(self.vector_inpath, self.n)


class AnalogyWorkerThread(threading.Thread):
	def __init__(self, worker_id, model):
		threading.Thread.__init__(self)
		self.worker_id = worker_id
		self.model = model
		self.finished = False
		self.current_queue = None
		self.n = 0
		self.fail = 0
		self.errors = 0

	def run(self):
		while not self.finished:
			analogy = self.current_queue.get()
			self.n += 1
			try:
				print u"%s is to %s like %s is to...?" % (analogy[0], analogy[1], analogy[2])
				most_similar = self.find_most_similar_cosmul(a=analogy[0], a_=analogy[1], b=analogy[2])
				print most_similar
				if most_similar[0] != analogy[3]:
					self.fail += 1
			except KeyError:
				self.errors += 1

	def find_most_similar_cosmul(self, a, a_, b):
		vec_a = self.model[a]
		vec_a_ = self.model[a_]
		vec_b = self.model[b]

		if not vec_a or not vec_a_ or not vec_b:
			raise KeyError("cannot compete similarity without input.")

		best_word = None
		best_sim = 0

		for word in self.model.keys():
			if word == a:
				continue
			current_vec = self.model[word]
			sim = (cos(current_vec, vec_b) * cos(current_vec, vec_a_)) / (cos(current_vec, vec_a) + 0.000001)
			if sim > best_sim:
				best_sim = sim
				best_word = word

		return best_word, best_sim


def analogy_eval(vector_inpath, analogy_path, per_section=False, logpath=None):
	sys.stdout = codecs.getwriter('utf8')(sys.stdout)
	output("Loading model...\n", logpath)
	model = w2v.load_word2vec_format(vector_inpath, binary=False)
	#model = load_vectors_from_model(vector_inpath)[1]

	#accuracy =  model.accuracy(questions=analogy_path)
	#print len(accuracy[len(accuracy)-1]["correct"])
	#print len(accuracy[len(accuracy)-1]["incorrect"])
	#return

	output("Reading analogies...\n", logpath)
	sections = read_analogies(analogy_path, per_section)

	result_outputs = []
	for header in sections.keys():
		section = sections[header]
		total = len(section)
		n = 0
		fail = 0
		errors = 0
		output("Evaluating model...\n", logpath)
		for analogy in section:
			n += 1
			print "Analogy %i of %i (%.2f%%)" %(n, total, n * 1.0 / total * 100.0)
			print "n: %i | fail: %i | success: %i | error: %i" % (n, fail, n-fail, errors)
			try:
				print u"%s is to %s like %s is to... (%s)?" %(analogy[0], analogy[1],
														analogy[2], analogy[3])
				most_similar = model.most_similar_cosmul(positive=[analogy[2], analogy[1]],
														 negative=[analogy[0]])
				for element in analogy:
					if element in most_similar:
						most_similar.remove(element)
				print most_similar
				if most_similar[0][0] != analogy[3]:
					fail += 1
			except:
				errors += 1
				fail += 1
		result_outputs.append("Accuracy in section %s with vectors from %s: %.2f %%. %i KeyErrors."
								% (header, vector_inpath, (n - fail) * 1.0 / n * 100.0, errors))

	for result_output in result_outputs:
		print result_output


def read_analogies(analogy_path, per_section=False):
	current_section = None
	sections = {}

	with codecs.open(analogy_path, "rb", "utf-8") as analogy_file:
		for line in analogy_file.readlines():
			line = line.strip()
			if line.startswith(":"):
				current_section = line[1:].strip()
				sections[current_section] = []
				continue
			parts = line.split()
			sections[current_section].append((parts[0], parts[1], parts[2], parts[3]))

	if per_section:
		return sections
	else:
		return {"Total": [analogy for section in sections.keys() for analogy in sections[section]]}


def read_analogies_for_parallel(analogy_path, per_section=False):
	current_section = Queue.Queue()
	sections = []
	current_section_title = None
	if not per_section:
		sections.append({"Total": Queue.Queue()})

	with codecs.open(analogy_path, "rb", "utf-8") as analogy_file:
		for line in analogy_file.readlines():
			line = line.strip()
			if line.startswith(":") and per_section:
				current_section_title = line[1:].strip()
				sections.append({current_section_title: Queue.Queue()})
				continue
			elif line.startswith(":"):
				continue

			parts = line.split()
			analogy = (parts[0], parts[1], parts[2], parts[3])
			if per_section:
				sections[-1][current_section_title].put(analogy)
			else:
				sections[0]["Total"].put(analogy)

	return sections


def output(message, logpath=None):
	if not logpath:
		print rreplace(message, "\n", "", 1)
	else:
		with codecs.open(logpath, "a", "utf-8") as logfile:
			logfile.write(message)


def rreplace(s, old, new, occurrence):
	li = s.rsplit(old, occurrence)
	return new.join(li)