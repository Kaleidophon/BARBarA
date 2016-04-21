import codecs, os, sys, random, re, copy, cPickle
from collections import defaultdict, Counter
from multiprocessing import Pool


def main():
	# Parsing command line arguments
	if sys.argv[1] == "-f":
		# Merge frequency files
		merge_frequency_files(sys.argv[2], sys.argv[3], sys.argv[4])
	elif sys.argv[1] == "-id":
		# Merge ID files
		merge_id_files(sys.argv[2], sys.argv[3], sys.argv[4])
	elif sys.argv[1] == "-yid":
		# Merge ID files into YAML format
		merge_id_files(sys.argv[2], sys.argv[3], sys.argv[4], True)


# ----------------------------------------------------------------------------------------------------------------------

# Functions associated with the merging of frequency files

def merge_frequency_files(infiles_path, outpath, logpath):
	with codecs.open(logpath, "wb", "utf-8") as logfile:
		inpaths = [infiles_path + path for path in os.listdir(infiles_path)]
		logfile.write("Inpaths: %s\n" % (unicode(inpaths)))
		logfile.write("Outpath: %s\n" % outpath)

		# Assign files to workers
		logfile.write("Assigning tasks...\n")
		pool = Pool(processes=len(inpaths))
		logfile.write("Starting %i threads...\n" % (len(inpaths)))
		freqdicts = pool.map(freqWorker, inpaths)
		logfile.write("Reading of frequency files complete. Start merging...\n")

		# Merging dictionaries
		while len(freqdicts) != 1:
			logfile.write("Merging %i dictionaries...\n" % len(freqdicts))
			# Assigning dictionary pair to be merged in parallel
			freqdict_tuples = [(freqdicts[i], freqdicts[i - 1]) for i in range(1, len(freqdicts), 2)]
			new_freqdicts = map(mergeDicts, freqdict_tuples)

			# Dealing with dictionary that might have been left over
			if len(freqdicts) % 2 == 1:
				new_freqdicts.append(freqdicts[len(freqdicts) - 1])
			freqdicts = new_freqdicts

		freqdict = freqdicts[0]
		logfile.write("Merging complete!\n")

		# Writing results
		logfile.write("Writing results into file...\n")
		with codecs.open(outpath, "wb", "utf-8") as outfile:
			for fkey in freqdict:
				outfile.write("%s\t%s\t%i\n" % (unicode(fkey[0]), unicode(fkey[1]), freqdict[fkey]))
		logfile.write("Writing files complete!\n")


def freqWorker(inpath):
	freqdict = defaultdict(int)
	with codecs.open(inpath, "rb", "utf-8") as infile:
		line = rl(infile)
		while line != "":
			parts = line.split("\t")
			key_tuple = (parts[0], parts[1])
			freqdict.setdefault(key_tuple, int(parts[2]))
			line = rl(infile)
		return freqdict


def mergeDicts(dicttuple):
	c1, c2 = Counter(dicttuple[0]), Counter(dicttuple[1])
	return c1 + c2


def rl(infile):
	return infile.readline().strip()


# ----------------------------------------------------------------------------------------------------------------------

# Functions associated with the merging of ID files

def merge_id_files(infiles_path, outpath, logpath, yaml=False):
	with codecs.open(logpath, "wb", "utf-8") as logfile:
		inpaths = [infiles_path + path for path in os.listdir(infiles_path) if ".DS_Store" not in path]
		logfile.write("Inpaths: %s\n" % (unicode(inpaths)))
		logfile.write("Outpath: %s\n" % (outpath))

		# Assigning files to workers
		logfile.write("Assigning tasks...\n")
		pool = Pool(processes=len(inpaths))
		logfile.write("Starting %i threads...\n" % (len(inpaths)))
		idsdicts = pool.map(idWorker, inpaths)
		logfile.write("Reading of ID files complete. Start merging...\n")

		# Merging dictionaries
		while len(idsdicts) != 1:
			logfile.write("Merging %i ID dictionaries...\n" % (len(idsdicts)))
			# Assigning dictionary pair to be merged in parallel
			idsdict_tuples = [(idsdicts[i], idsdicts[i - 1]) for i in range(1, len(idsdicts), 2)]
			new_idsdicts = map(merge_id_dicts, idsdict_tuples)

			# Dealing with dictionary that might have been left over
			if len(idsdicts) % 2 == 1:
				new_idsdicts.append(idsdicts[len(idsdicts) - 1])
			idsdicts = new_idsdicts

		idsdict = idsdicts[0]
		del idsdicts  # Free memory. Even in parallel this task is damn memory intensive
		logfile.write("Merging complete!\n")

		# Writing results
		logfile.write("Writing results into file...\n")
		with codecs.open(outpath, "wb", "utf-8") as outfile:
			for idkey in idsdict:
				if yaml:
					outfile.write("%s\t%s\n\t- %s\n"
								  %(unicode(idkey[0]), unicode(idkey[1]), "\n\t- ".join([unicode(e) for e in idsdict[idkey]])))
				elif not yaml:
					outfile.write("%s\t%s\n\t%s\n\n"
								  %(unicode(idkey[0]), unicode(idkey[1]), "\n\t".join([unicode(e) for e in idsdict[idkey]])))
		logfile.write("Writing files complete!\n")


def idWorker(inpath):
	idsdict = defaultdict(list)
	ids_lookup = defaultdict(str)
	file_n = re.findall(re.compile("\d{2}(?=[^a])"), inpath) # Retrieve file number
	if len(file_n) == 0:
		file_n = int(re.findall(re.compile("\d+"), inpath)[0])
	else:
		file_n = int(file_n[0])

	with codecs.open(inpath, "rb", "utf-8") as infile:
		line = infile.readline()
		key = ("", "")
		while line != "":
			if line == "\n":
				# If block for single ID is over
				line = infile.readline()
				key = ("", "")
				continue
			elif line.startswith("\t"):
				# parse ID
				line = line.strip()
				# Check if key is existing
				if line in ids_lookup:
					line = ids_lookup[line]
				else:
					random_id = random.randint((file_n - 1) * 100000000, file_n * 100000000) # Assign new, shorter ID
					while random_id in ids_lookup:
						random_id = random.randint(0, 100000000)
					ids_lookup[line] = random_id
					line = random_id

				if key not in idsdict:
					idsdict[key] = [line]
				else:
					idsdict[key].append(line)
			else:
				# Key found
				parts = line.replace("\n", "").split("\t")
				key = (parts[0], parts[1])

			line = infile.readline()

		return idsdict


def merge_id_dicts(dicttuple):
	dict1, dict2 = dicttuple
	new = copy.deepcopy(dict1)
	for key, value in dict2.items():
		new.setdefault(key, []).extend(value)
	return new


def print_key_lengths(dictionary):
	for key in dictionary:
		print "%s\t%i" % (key, len(dictionary[key]))
	print ""


def dump_ids_dict(idsdict, outpath):
	with codecs.open(outpath, "wb", "utf-8") as outfile:
		idsdict = {(key[0].encode('latin-1'), key[0].encode('latin-1')): idsdict[key] for key in idsdict}
		cPickle.dump(idsdict, outfile)


def load_ids_dict(inpath):
	with codecs.open(inpath, "rb") as infile:
		idsdict = cPickle.load(infile)
		idsdict = {(key[0].decode('latin-1'), key[1].decode('latin-1')): idsdict[key] for key in idsdict}
		return idsdict


if __name__ == "__main__":
	main()
