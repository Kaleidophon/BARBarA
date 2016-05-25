import codecs
import re
import sys

entity_exp = r"\/m\/[0-9a-z_]+"

with codecs.open(sys.argv[1], 'rb', 'utf-8') as infile:
	with codecs.open(sys.argv[2], 'wb', 'utf-8') as outfile:
		line = infile.readline().strip()
		while line:
			parts = line.split("\t")
			if re.match(entity_exp, parts[0]) and re.match(entity_exp, parts[2]):
				outfile.write(line + "\n")
			else:
				print line
			line = infile.readline().strip()
