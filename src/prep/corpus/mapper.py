#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Mapper classed used to count frequencies of words in a corpus. Corpus has to be in plain text format. This class is
used in a `Map-Reduce <https://en.wikipedia.org/wiki/MapReduce>`_-pattern, so you also need the :mod:`reducer.py` class.

Then, you can open your terminal and pipe them together:

.. code-block:: console

	> cat corpus.txt | ./mapper.py | sort | ./reducer.py

Also, you probably have to remove the ``if __name__ == \"__main__\":`` line and unindent the remaining code,
this is only
due to sphinx being picky and not documenting plain python scripts at all.
"""

# STANDARD
import codecs
import sys

if __name__ == "__main__":
	sys.stdout = codecs.getwriter('utf8')(sys.stdout)

	for line in sys.stdin:
		line = line.strip().decode('utf-8', 'replace')
		parts = line.split(' ')

	if ' ' in line:
		sys.stdout.write(u'%s\t%s\n' % (parts[0], ' '.join(parts[1:])))
