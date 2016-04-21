#!/usr/bin/python
# -*- coding: utf-8 -*-
import gzip, re, codecs, sys


def main():
	# Parsing command line arguments
	inpath, outpath, logpath = sys.argv[1:]
	process(inpath, outpath, logpath)


def process(inpath, outpath, logpath):
	with gzip.open(inpath, "rb") as infile:
		# Preparing paths
		outpath_dictfile = outpath + inpath[inpath.rfind("/") + 1:].replace(".xml.gz", "_freqs_out.txt")
		outpath_idfile = outpath + inpath[inpath.rfind("/") + 1:].replace(".xml.gz", "_ids_out.txt")
		outpath_logfile = logpath + inpath[inpath.rfind("/") + 1:].replace(".xml.gz", "_log.txt")

		with codecs.open(outpath_logfile, "wb", "utf-8") as logfile:
			freqs = {}  # Frequencies of named entities with specific tag
			ne_ids = {}  # all named entities with sentence id
			current_sentence_id = ""
			corrupt_id = False

			line = infile.readline().strip().decode("utf-8")
			lcount = 1
			while line != "":
				if lcount % 1000000 == 0:
					# Logging
					logfile.write("Line Nr. %ik in file %s processed.\n" % (lcount / 1000, infile))

				if contains_tag(line) and "<s" in line:
					# Get current sentence ID
					current_sentence_id = extract_sentence_id(line)
					corrupt_id = [False, True][current_sentence_id is None]
				elif not contains_tag(line) and not corrupt_id:
					ne = extract_named_entity(line)
					if ne is not None:
						while True:
							next_line = infile.readline().strip().decode("utf-8")
							lcount += 1
							if not contains_tag(next_line):
								next_ne = extract_named_entity(next_line)
								if next_ne is not None and next_ne[1] == ne[1]:
									ne = ("%s %s" %(ne[0], next_ne[0]), ne[1])
								else:
									break
							else:
								break
						line = next_line

						if ne in freqs:
							freqs[ne] += 1
						else:
							freqs[ne] = 1

						# Logging sentence ids of an occurrence
						if ne in ne_ids:
							ne_ids[ne].append(current_sentence_id)
						else:
							ne_ids[ne] = [current_sentence_id]
						continue

				# Preparation for next iteration
				line = infile.readline().strip().decode("utf-8")
				lcount += 1

			# Wrapping up processs
			logfile.write("Processing of file %s complete!\n" % (infile))

			logfile.write("Writing frequencies of named entities in %s into %s...\n" % (infile, outpath_dictfile))
			print_dict_in_file(freqs, outpath_dictfile)
			logfile.write("Writing frequencies of named entities of %s into %s complete!\n" % (infile, outpath_dictfile))

			logfile.write("Writing occurrences of named entities in %s into %s...\n" % (infile, outpath_idfile))
			print_ids_in_file(ne_ids, outpath_idfile)
			logfile.write("Writing occurrendes of named entities of %s into %s complete!\n" % (infile, outpath_idfile))


def contains_tag(line):
	pattern = re.compile("<.+>")
	return pattern.search(line) is not None


def extract_sentence_id(tag):
	if "<s" not in tag:
		return ""
	pattern = re.compile('id="[a-z0-9]+?"(?=\s)')
	res = re.findall(pattern, tag)
	if len(res) == 0:
		return None
	return res[0].replace('"', "").replace("id=", "")


def extract_named_entity(line):
	line_parts = line.split("\t")
	feature = line_parts[3]
	if feature != "O":
		return line_parts[0], line_parts[3]


def print_dict_in_file(dictionary, out_path):
	with codecs.open(out_path, "wb", "utf-8") as out_file:
		for key, value in dictionary.iteritems():
			out_file.write("%s\t%s\t%s\n" % (key[0], key[1], value))


def print_ids_in_file(dictionary, out_path):
	with codecs.open(out_path, "wb", "utf-8") as out_file:
		for key, value in dictionary.iteritems():
			out_file.write("%s\t%s\n\t%s\n\n" % (key[0], key[1], "\n\t".join(value)))


def print_list_in_file(ne_list, out_path):
	with codecs.open(out_path, "wb", "utf-8") as out_file:
		for ne_tuple in ne_list:
			out_file.write("%s\t%s\t%s\n" % (ne_tuple[0], ne_tuple[1], ne_tuple[2]))


if __name__ == '__main__':
	main()
