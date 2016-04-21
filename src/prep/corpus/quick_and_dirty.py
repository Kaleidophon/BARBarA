import codecs, re, unicodedata, sys


def clean_file(inpath, outpath):

	all_chars = (unichr(i) for i in xrange(0x110000))
	control_chars = ''.join(map(unichr, range(0,32) + range(127,160)))
	control_char_re = re.compile('[%s]' % re.escape(control_chars))

	with codecs.open(inpath, "rb", "latin-1") as infile:
		with codecs.open(outpath, "wb", "utf-8") as outfile:
			for line in infile:
				#print line
				#print type(line)
				#print type(line)
				line = line.strip().replace('\n', '').encode('unicode-escape')
				if not line:
					continue
				if line.startswith("-"):
					outfile.write(u"    %s\n" %(line))
				else:
					outfile.write(u"\n%s\n" %(line))


if __name__ == "__main__":
	clean_file(sys.argv[1], sys.argv[2])

#perl -pi -e 's/\n//g' nes_indexed_cleaned.yaml
#perl -pi -e 's/"    -/"\n    -/g' nes_indexed_cleaned.yaml
#perl -pi -e 's/":    -/":\n    -/g' nes_indexed_cleaned.yaml