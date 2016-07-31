#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
This module contains decorators that wrap around functions used in other modules.
"""

# STANDARD
import codecs
import threading
import time
from functools import wraps

# PROJECT
from src.misc.helpers import alt


def log_time(logpath="log.txt", interval=5):
	"""
	This decorator is used to log the execution time of a function into a given logfile, following a constant interval.

	Args:
		logpath (str): Path to logfile.
		interval (int): Logging interval in seconds.

	Returns:
		function: Decorator with function.
	"""

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

