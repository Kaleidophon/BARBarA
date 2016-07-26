import codecs
import sys
import yaml

if __name__ == "__main__":
	with codecs.open(sys.argv[1], "wb", "utf-8") as infile:
		with codecs.open(sys.argv[2], "wb", "utf-8") as outfile:
			line = infile.readline()
			while line != "":
				relation = yaml.load(line)[0]
				outfile.write("%s\t%s\t%s\n" %(relation[1].strip(), relation[2].strip(), relation[4].strip()))
				line = infile.readline()
