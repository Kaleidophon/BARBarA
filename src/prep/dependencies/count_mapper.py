#!/usr/bin/env python
import sys, codecs

sys.stdout = codecs.getwriter('utf8')(sys.stdout)

for line in sys.stdin:
	line = line.strip().decode('utf-8', 'replace')

	for word in line.split():
		sys.stdout.write(u'%s\t%i\n' % (word, 1))
