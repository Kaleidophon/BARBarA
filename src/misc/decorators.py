import codecs, threading, time, multiprocessing
from functools import wraps


def log_time(logpath="log.txt", interval=5):

	def log_time_decorator(func):
		@wraps(func)
		def wrapper(*args, **kwargs):
			result = [None]

			def return_value(*args, **kwargs):
				result[0] = func(*args, **kwargs)

			t = threading.Thread(target=return_value, args=args, kwargs=kwargs)
			log_entries = 0
			with codecs.open(logpath, "a", "utf-8") as logfile:
				start_time = time.time()
				t.start()
				while t.is_alive():
					elapsed_time = (time.time() - start_time)
					if elapsed_time > interval * log_entries:
						m, s = divmod(elapsed_time, 60)
						h, m = divmod(m, 60)
						logfile.write(alt("Elapsed time for function '%s': %2dh %2dm %2ds\n"
									  %(func.__name__, h, m, s)))
						log_entries += 1
			return result[0]
		return wrapper

	return log_time_decorator


def log_time_mp(logpath="log.txt", interval=5):

	def log_time_decorator(func):
		@wraps(func)
		def wrapper(*args, **kwargs):
			result = [None]

			def return_value(*args, **kwargs):
				result[0] = func(*args, **kwargs)

			t = threading.Thread(target=return_value, args=args, kwargs=kwargs)
			log_entries = 0
			with codecs.open(logpath, "a", "utf-8") as logfile:
				start_time = time.time()
				t.start()
				while t.is_alive():
					elapsed_time = (time.time() - start_time)
					if elapsed_time > interval * log_entries:
						m, s = divmod(elapsed_time, 60)
						h, m = divmod(m, 60)
						logfile.write(alt("Elapsed time for function '%s' of %s: %2dh %2dm %2ds\n"
									  %(func.__name__, multiprocessing.current_process().name, h, m, s)))
						log_entries += 1
			return result[0]
		return wrapper

	return log_time_decorator


def alt(func):
	return "%s: %s" %(time.strftime("%H:%M:%S", time.gmtime()), func)
