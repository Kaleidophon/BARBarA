import numpy as np
import time
import codecs
import os
from collections import defaultdict
import multiprocessing
import threading
import Queue


def calculate_loss_of_precision(vector_inpath, procs, sizes, logpath=None):
	sizes.sort(key=lambda x: x, reverse=True)
	print sizes
	model = load_vectors_from_model(vector_inpath, sizes[0], logpath)
	concentrations = []
	distances = []
	for size in sizes:
		model = load_vectors_from_model(vector_inpath, size, logpath)
		results = calculate_concentration(model, procs, logpath, vector_inpath)
		distances.append(results[0])
		concentrations.append(results[1])
	print distances
	print concentrations


def calculate_concentrations(vectors_inpath, procs, max_n_vectors,
							 logpath=None):
	vector_inpaths = os.listdir(vectors_inpath)
	for vector_inpath in vector_inpaths:
		calculate_concentration(vector_inpath, procs, max_n_vectors, logpath)


def calculate_concentration(model, procs, logpath=None,
							vector_inpath=""):
	total_starttime = time.time()
	word_list, vector_dict = model
	distance_queue = multiprocessing.Queue()
	distance_pool_args = (vector_dict, 0.0, distance_queue)
	distance_pool = multiprocessing.Pool(processes=procs,
										 initializer=init_pool_for_distances,
										 initargs=(distance_pool_args,))
	output(alt("Initializing distance pool for %s complete. Starting %i "
			   "processes...\n" % (vector_inpath, procs)), logpath)
	calc_starttime = time.time()
	distance_args = [(word_list, logpath)]
	if procs > 1:
		lol = chunks2(word_list, procs)
		distance_args = [(chunk, logpath) for chunk in lol]
	#print distance_args, len(distance_args)
	distance_results = distance_pool.map(_calculate_all_distances,
										 distance_args)
	distance_pool.close()
	calc_endtime = time.time()
	m, s = divmod(calc_endtime - calc_starttime, 60)
	time.sleep(2)
	output(alt("Calculating distances for %s complete. Calculations took %2dm"
			   "%2ds.\n" % (vector_inpath, m, s)), logpath)
	output(alt("Looking for longest distance in %s...\n" % (vector_inpath)),
		   logpath)
	total_n, total_distance, longest_distance = 0.0, 0.0, 0.0
	for result in distance_results:
		total_n += result[1]
		total_distance += result[0]
		if result[2] > longest_distance:
			longest_distance = result[2]
	average_distance = total_distance / total_n
	deviance_pool_args = (distance_queue)
	output(alt("Initializing deviance pool for %s complete. Starting %i "
			   "processes...\n" % (vector_inpath, procs)), logpath)
	deviance_args = [(average_distance, logpath)]
	if procs > 1:
		deviance_args = [(average_distance, logpath) for i in range(procs)]
	deviance_pool = multiprocessing.Pool(processes=procs,
										 initializer=init_pool_for_deviances,
										 initargs=(deviance_pool_args,))
	deviance_results = deviance_pool.map(_calculate_deviance, deviance_args)
	deviance_pool.close()
	total_deviance = 0.0
	for result in deviance_results:
		total_deviance += result
	average_deviance = total_deviance / total_n
	total_endtime = time.time()
	time.sleep(2)
	output(alt("Looked at %i data points in %s: \n\tAverage distance is %.2f"
			   "\n\tLongest distance is %.2f \n\tAverage deviance is %.2f\n"
			   % (len(vector_dict.keys()), vector_inpath, average_distance,
				  longest_distance,
				  average_deviance)), logpath)
	m, s = divmod(total_endtime - total_starttime, 60)
	output(alt("Process took %2dm %2ds.\n" % (m, s)), logpath)
	return average_distance, average_deviance


def _calculate_all_distances(cargs):
	vector_queue, logpath = cargs
	longest_distance = 0.0
	average_distance = 0.0
	n = 0
	# process_name = multiprocessing.current_process().name
	while True:
		# print process_name, n, len(vector_queue)
		current_word = vector_queue.pop(0)
		if not current_word: break
		current_vector = global_vector_dict[current_word]
		for word in global_vector_dict.keys():
			if word == current_word: continue
			current_distance = np.linalg.norm(global_vector_dict[word]
											  - current_vector)
			global_distance_queue.put(current_distance)
			if current_distance > longest_distance:
				longest_distance = current_distance
			average_distance += current_distance
			n += 1
		if len(vector_queue) == 0:
			break
	global_distance_queue.close()
	return average_distance, n, longest_distance


def _calculate_deviance(dargs):
	average_distance, logpath = dargs
	deviance = 0.0
	while not global_distance_queue.empty():
		current_distance = global_distance_queue.get()
		deviance += abs(current_distance - average_distance)
	return deviance


def chunks(l, n):
	chunklist = []
	for i in xrange(0, len(l), n):
		chunklist.append(l[i:i + n])
	return chunklist


def chunks2(lst, n):
	return [lst[i::n] for i in xrange(n)]


def init_pool_for_distances(pool_args):
	global global_vector_dict
	global global_longest_distance
	global global_distance_queue
	global_vector_dict, \
	global_longest_distance, \
	global_distance_queue = pool_args


def init_pool_for_deviances(pool_args):
	global global_distance_queue
	global_distance_queue = pool_args


def output(message, logpath=None):
	if not logpath:
		print rreplace(message, "\n", "", 1)
	else:
		with codecs.open(logpath, "a", "utf-8") as logfile:
			logfile.write(message)


def alt(func):
	return "%s: %s" % (time.strftime("%H:%M:%S", time.gmtime()), func)


def load_vectors_from_model(vector_inpath, max_n=None, logpath=None, indices=False):
	if max_n:
		output(alt("Start loading %i vectors in %s....\n" % (max_n, vector_inpath)), logpath)
	elif not max_n:
		output(alt("Start loading all vectors in %s....\n" % (vector_inpath)), logpath)
	loading_starttime = time.time()
	word_list = []
	vector_dict = defaultdict(np.array)
	with codecs.open(vector_inpath, "rb", "utf-8") as vector_infile:
		n = 1
		line = vector_infile.readline().strip()
		while line != "":
			if n == 1:
				n += 1
				line = vector_infile.readline().strip()
				continue
			parts = line.strip().split(" ")
			if indices:
				word = (int(parts[0]), int(parts[1]))
				vector = np.array([float(dimension) for dimension in parts[2:]])
			else:
				word = parts[0]
				vector = np.array([float(dimension) for dimension in parts[1:]])
			word_list.append(word)
			vector_dict[word] = vector
			if max_n:
				if n == max_n:
					break
			n += 1
			line = vector_infile.readline().strip()
	loading_endtime = time.time()
	m, s = divmod(loading_endtime - loading_starttime, 60)
	output(alt("Loading of %s complete! Loading took %2dm %2ds.\n" % (vector_inpath, m, s)), logpath)
	return word_list, vector_dict


def load_vectors_from_model_parallel(vector_inpath, procs, logpath=None):
	output(alt("Start loading all vectors in %s....\n" % (vector_inpath)), logpath)

	class ProducerThread(threading.Thread):
		def __init__(self):
			threading.Thread.__init__(self)

		def run(self):
			for lock in locks:
				lock.acquire()
			lock_released = False
			with codecs.open(vector_inpath, "rb", "utf-8") as infile:
				first = infile.readline()  # Skip first lone
				while True:
					next_lines = infile.readlines(10000)
					print "Still producing..."
					if not next_lines:
						reading_finished = True
						break
					for line in next_lines:
						line_queue.put(line)
						if not lock_released:
							for lock in locks:
								lock.release()
							lock_released = True
				print "Producing finished!"

	class ConsumerThread(threading.Thread):
		def __init__(self, n):
			threading.Thread.__init__(self)
			self.n = n

		def run(self):
			with locks[self.n]:
				print "Consumer %i starts consuming!" %(self.n)
				while not (line_queue.empty() and reading_finished):
					print "Consumer %i still consuming!" %(self.n)
					line = line_queue.get()
					parts = line.strip().split(" ")
					word = parts[0]
					vector = np.array([float(dimension) for dimension in parts[1:]])
					word_list.append(word)
					vector_dict[word] = vector
			print "Consumer %i stopped consuming." %(self.n)

	loading_starttime = time.time()
	word_list = []
	vector_dict = defaultdict(np.array)
	line_queue = Queue.Queue()
	reading_finished = False
	locks = [threading.Lock() for i in range(procs-1)]

	output(alt("Initializing 1 producer and %i consumer thread(s)...\n" %(procs-1)), logpath)
	threads = [ProducerThread()]
	for i in range(procs-1):
		threads.append(ConsumerThread(i))

	output(alt("Starting reading threads...\n"), logpath)
	for thread in threads:
		thread.start()

	for thread in threads:
		thread.join()

	loading_endtime = time.time()
	m, s = divmod(loading_endtime - loading_starttime, 60)
	output(alt("Loading of %s complete! Loading took %2dm %2ds.\n" % (vector_inpath, m, s)), logpath)
	return word_list, vector_dict


def rreplace(s, old, new, occurrence):
	li = s.rsplit(old, occurrence)
	return new.join(li)
