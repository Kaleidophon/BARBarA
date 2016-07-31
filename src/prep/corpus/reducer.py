#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Reducer classed used to count frequencies of words in a corpus. Corpus has to be in plain text format. This class is
used in a `Map-Reduce <https://en.wikipedia.org/wiki/MapReduce>`_-pattern, so you also need the :mod:`mapper.py` class.

Then, you can open your terminal and pipe them together:

.. code-block:: console

	> cat corpus.txt | ./mapper.py | sort | ./reducer.py

Also, you probably have to remove the ``if __name__ == \"__main__\":`` line and unindent the remaining code,
this is only
due to sphinx being picky and not documenting plain python scripts at all.
"""

# STANDARD
import codecs
from collections import defaultdict
import sys

if __name__ == "__main__":
	sys.stdout = codecs.getwriter('utf8')(sys.stdout)
	sys.stdin = codecs.getreader('utf-8')(sys.stdin)

	# maps words to their counts
	mwes = defaultdict(str)

	# input comes from STDIN
	for line in sys.stdin:
		line = line.strip()
		parts = line.split('\t')
		start, mwe = (parts[0].strip(), parts[1].strip())

		try:
			mwes[start].append(mwe)
		except:
			mwes[start] = [mwe]

	for key in mwes.keys():
		print u'"%s": \n  - "%s"\n' %(key, u'"\n  - "'.join(mwes[key]))
