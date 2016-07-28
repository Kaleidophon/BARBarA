#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
This script collects a few statistics about named entities extracted from the corpus and the percentage of their
occurrence in the *Freebase* relation dataset.
Requires a relation file in yaml format and a merged named entity frequency file, see :py:mod:`extract_nes.py`,
:py:mod:`merge.py` and  :py:mod:`relations.py`.
"""

# STANDARD
import codecs
import sys

# EXTERNAL
import yaml


def main():
	"""
	Main function
	"""
	if len(sys.argv) != 3:
		print "Usage: <Path to merged frequency file> <Path to relation yaml file>"
	calculate_occurrences(sys.argv[1], sys.argv[2])


def calculate_occurrences(freqpath, relations_path):
	"""
	Calculate statistics about named entities extracted from the corpus and the percentage of their
	occurrence in the *Freebase* relation dataset.

	Args:
		freqpath (str): Path to merged frequencies file.
		relations_path 8str): Path to relation yaml file.
	"""
	rel_entities = set()
	with codecs.open(relations_path, "rb", "utf-8") as relfile:
		line = relfile.readline().strip()
		while line != "":
			try:
				yaml_map = yaml.safe_load(line)
				rel_entities.add(yaml_map[0][0])
				rel_entities.add(yaml_map[0][3])
			except yaml.scanner.ScannerError:
				parts = line.replace(u"- [", u"").replace(u"]", u"").split(u", ")
				rel_entities.add(parts[0])
				rel_entities.add(parts[3])
			line = relfile.readline().strip()
	total = len(rel_entities)
	with codecs.open(freqpath, "rb", "utf-8") as freqfile:
		lcount = 0
		line = freqfile.readline().strip()
		while line != "":
			parts = line.split("\t")
			if parts[0] in rel_entities:
				rel_entities.remove(parts[0])
			line = freqfile.readline().strip()
			lcount += 1
		print "Total # of entities in decow data: %i" %(lcount)
		print "Total # of entities in relations data: %i" %(total)
		print "Percentage of relation entities in decow data: %.3f %%" %((total - len(rel_entities)*1.0) / total * 100)
		print "Percentage of decow entities in relation data: %.3f %%" %((total - len(rel_entities)*1.0) / lcount * 100)


if __name__ == '__main__':
	main()
