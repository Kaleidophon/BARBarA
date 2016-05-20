#!/usr/bin/python
# -*- coding: utf-8 -*-

import codecs, yaml


def main():
	#calculateOccurrences(sys.argv[1:])
	calculate_occurrences("../rsc/decow/freq_merge.txt", "../rsc/rel/relations.yaml")


def calculate_occurrences(freqpath, relations_path):
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
