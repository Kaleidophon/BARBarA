#!/usr/bin/env python
from collections import defaultdict
import sys, codecs

sys.stdout = codecs.getwriter('utf8')(sys.stdout)
sys.stdin = codecs.getreader('utf8')(sys.stdin)

# maps words to their counts
counts = defaultdict(int)

# input comes from STDIN
for line in sys.stdin:
	# remove leading and trailing whitespace
	line = line.strip()

	# parse the input we got from count_mapper.py
	word, count = line.split('\t')
	# convert count (currently a string) to int
	count = int(count)

	try:
		counts[word] += count
	except KeyError:
		counts[word] = count

# write the tuples to stdout
# Note: they are unsorted
for word in counts.keys():
	sys.stdout.write(u'%s\t%i' % (word, counts[word]))
