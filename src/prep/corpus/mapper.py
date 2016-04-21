#!/usr/bin/env python
import sys, codecs

sys.stdout = codecs.getwriter('utf8')(sys.stdout)

for line in sys.stdin:
	line = line.strip().decode('utf-8', 'replace')
	parts = line.split(' ')

	if ' ' in line:
		sys.stdout.write(u'%s\t%s\n' % (parts[0], ' '.join(parts[1:])))