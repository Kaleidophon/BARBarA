import codecs
import sys

with codecs.open(sys.argv[1], "rb", "utf-8") as infile:
	with codecs.open(sys.argv[2], "wb", "utf-8") as outfile:
		line = infile.readline()
		while line != "":
			if line.startswith("<"):
				line = infile.readline()
				continue
			outfile.write(line)
			line = infile.readline()
