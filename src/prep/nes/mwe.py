import cPickle as pickle, sys, codecs as c, base64 as b64
from collections import defaultdict


def main():
	if (len(sys.argv) == 4) and sys.argv[1] == '-create':
		create_mwe_pickle2(sys.argv[2], sys.argv[3])
	elif (len(sys.argv) == 5) and sys.argv[1] == '-create':
		create_mwe_pickle2(sys.argv[2], sys.argv[3], sys.argv[4])
	elif len(sys.argv) == 3 and sys.argv[1] == '-show':
		d = load_dict_pickle2(sys.argv[2])
		for key in d.keys():
			print u"%s\t%s" %(key, unicode(d[key]))
	elif len(sys.argv) == 5 and sys.argv[1] == '-replace':
		replace_mwes(sys.argv[2], sys.argv[3], sys.argv[4])


def replace_mwes(mwe_path, corpus_path, out_path):
	mwes = load_dict_pickle(mwe_path)
	with c.open(corpus_path, 'rb', 'utf-8') as corpus_file, c.open(out_path, 'wb', 'utf-8') as out_file:
		line = corpus_file.readline().strip()
		while line != '':
			print line
			tokens = line.split(' ')
			tokens_new = []
			token_iterator = iter(range(len(tokens)))
			for i in token_iterator:
				token = tokens[i]
				if token in mwes.keys():
					try:
						entries = mwes[token]
						print entries
						len_e = 0
						found = False
						for entry in entries:
							print "%s = %s?" %(' '.join(tokens[i:len(entry.split(' '))]), entry)
							if ' '.join(tokens[i:len(entry.split(' '))]) == entry:
								tokens_new.append(entry.replace(' ', '_'))
								len_e = len(entry.split(" "))
								found = True
								break;
						if found:
							for i in range(len_e):
								next(token_iterator)
					except IndexError:
						continue
				else:
					tokens_new.append(token)
			out_file.write('%s\n' %(' '.join(tokens_new)))
			print ' '.join(tokens_new)
			line = corpus_file.readline().strip()


def create_mwe_pickle(inpath, outpath, logpath='./mwes.log'):
	mwes = defaultdict(str)
	with c.open(inpath, 'rb', 'utf-8') as infile, c.open(logpath, 'wb', 'utf-8') as logfile:
		logfile.write('Starting to create mwe dictionary...\n')
		line = infile.readline().strip()
		count = 0
		while line != "":
			if count % 100000 == 0:
				logfile.write('%i lines processed...\n' %(count))
			parts = line.split(" ")
			if len(parts) > 1:
				if parts[0] not in mwes.keys():
					mwes.setdefault(parts[0], [parts[1:]])
				else:
					mwes[parts[0]].append(line)
			line = infile.readline().strip()
			count += 1
		logfile.write('Dumping dictionary...\n')
		dump_dict_pickle2(mwes, outpath)


def create_mwe_pickle2(inpath, outpath, logpath='./mwes.log'):
	mwes = defaultdict(str)
	with c.open(inpath, 'rb', 'utf-8') as infile, c.open(logpath, 'wb', 'utf-8') as logfile:
		logfile.write('Starting to create mwe dictionary...\n')
		line = infile.readline().strip()
		count = 0
		while line != "":
			if count % 100000 == 0:
				logfile.write('%i lines processed...\n' %(count))
			if ' ' in line:
				parts = line.split(" ")
				if parts[0] not in mwes.keys():
					mwes.setdefault(parts[0].encode('latin-1', 'replace'), [' '.join(parts[1:]).encode('latin-1', 'replace')])
				else:
					mwes[parts[0]].append(' '.join(parts[1:]).encode('latin-1', 'replace'))
			line = infile.readline().strip()
			count += 1
		logfile.write('Dumping dictionary...\n')
		pickle.dump(mwes)


def dump_dict_pickle(d, outpath):
	with open(outpath, 'w') as outfile:
		d_encoded = {key.encode('latin-1', 'replace') : d[key].encode('latin-1', 'replace') for key in d.keys()}
		pickle.dump(d_encoded, outfile)


def dump_dict_pickle2(d, outpath):
	with open(outpath, 'w') as outfile:
		print 'Encoding entries for dumping...'
		for key in d.keys():
			k, v = (key, d[key])
			del d[key]
			k, v = (k.encode('latin-1', 'replace'), [v_.encode('latin-1', 'replace') for v_ in v])
			d.setdefault(k, v)
		print 'Dumping...'
		pickle.dump(d, outfile)


def load_dict_pickle(inpath):
	with open(inpath, 'rb') as infile:
		d = pickle.load(infile)
		return {key.decode('latin-1', 'replace') : d[key].decode('latin-1', 'replace') for key in d.keys()}


def load_dict_pickle2(inpath):
	with open(inpath, 'rb') as infile:
		d = pickle.load(infile)
		for key in d.keys():
			k, v = (key, d[key])
			del d[key]
			k, v = (k.decode('latin-1', 'replace'), [v_.decode('latin-1', 'replace') for v_ in v])
			d.setdefault(k, v)
		return d


if __name__ == '__main__':
	main()