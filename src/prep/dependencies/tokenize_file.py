import codecs
import sys
import nltk

with codecs.open(sys.argv[1], "rb", "utf-8") as infile:
	with codecs.open(sys.argv[2], "wb", "utf-8") as outfile:
		line = infile.readline()
		while line != "":
			outfile.write(" ".join(nltk.word_tokenize(line.strip())) + "\n")
			line = infile.readline()