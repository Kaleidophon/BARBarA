#!/usr/bin/env python
import sys, cPickle as cp, codecs, yaml, re, unicodedata
from collections import defaultdict

sys.stdout = codecs.getwriter('utf8')(sys.stdout)
sys.stdin = codecs.getreader('utf-8')(sys.stdin)

# maps words to their counts
mwes = defaultdict(str)

# input comes from STDIN

for line in sys.stdin:
	# remove leading and trailing whitespace
	#line = line.decode('iso8859-1')
	line = line.strip()
	parts = line.split('\t')
	start, mwe = (parts[0].strip(), parts[1].strip())

	try:
		mwes[start].append(mwe)
	except:
		mwes[start] = [mwe]

#for word in mwes.keys():
#	print '%s\t%s'% (word, str(mwes[word]))

for key in mwes.keys():
	print u'"%s": \n  - "%s"\n' %(key, u'"\n  - "'.join(mwes[key]))

#for key in mwes.keys():
#	print '"%s": \n  - "%s"\n' %(key, '"\n  - "'.join(mwes[key]))
