#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
This modules is about retrieving the names of entities and relations in the
`FB15k dataset <https://everest.hds.utc.fr/doku.php?id=en:smemlj12>`_. Because the entities are
used with their (quite cryptic) *Freebase* ids, those have to be resolved.

.. warning::
	Unfortunately, it isn't possible anymore to use this code (July 2016), because the *Freebase API* is now
	deprecated; the whole *Freebase* project has been integrated into *Wikidata*. However, this code is still
	included to show the process of thow freebase where transformed into real names using the API and the MQL query
	language.
"""

# STANDARD
import codecs
from collections import defaultdict
import time
import optparse
import re

# EXTERNAL
import json
import urllib


def main():
	"""
	Main function. Start translation of relation triplets based on command line arguments.
	"""
	optparser = init_optparser()
	(options, args) = optparser.parse_args()

	fetch_relation_triples_of_file(options.input, options.output, options.log, options.lang)


def fetch_relation_triples_of_file(inpath, outpath, logpath, lang="de"):
	"""
	Start the translation of the *Freebase* IDs into real names.

	Args:
		inpath (str): Path to *Freebase* relation file.
		outpath (str): Path the translated triplets should be written to.
		logpath (str): Path to log file.
		lang (str): Target language of the translation process (default is \"de\" for german).
	"""
	starttime = time.time()
	id_dict = defaultdict(str)
	failed_id_requests = set()

	with codecs.open(inpath, "rb", "utf-8") as infile, codecs.open(logpath, "wb", "utf-8") as logfile:
		line = rl(infile)
		exception_count = success_count = line_count = query_count = 0
		with codecs.open(outpath, "wb", "utf-8") as outfile:
			while line != "":
				# Write information into log file
				if line_count % 100 == 0:
					logfile.write(u"Line nr. %i\n" % line_count)
					logfile.write(u"Stored %i IDs so far...\n" % len(id_dict.keys()))
				logfile.write(u"Processing '%s'...\n" % line)

				# Start translation
				parts = line.split("\t")
				try:
					id1, id2 = format_fbid(parts[0]), format_fbid(parts[1])

					# If name for id not available in language
					if id1 in failed_id_requests:
						raise MissingTranslationException("No associated alias for id %s in language %s found in "
														  "Freebase." % (id1, lang))
					elif id2 in failed_id_requests:
						raise MissingTranslationException("No associated alias for id %s in language %s found in "
														  "Freebase." % (id2, lang))

					entity1 = entity2 = ""

					# If name for entity was already requested
					if id1 in id_dict.keys():
						entity1 = id_dict[id1]
					# Get name of entity in language
					else:
						entity1 = fetch_name(id1, lang)
						id_dict.setdefault(id1, entity1)
						query_count += 1

					# If name for entity was already requested
					if id2 in id_dict.keys():
						entity2 = id_dict[id2]
					# Get name of entity in language
					else:
						entity2 = fetch_name(id2, lang)
						id_dict.setdefault(id2, entity2)
						query_count += 1

					outfile.write('- [%s, %s, %s, %s, %s]\n' % (entity1, id1, parts[2], entity2, id2))
					success_count += 1
				except MissingTranslationException as mte:
					failed_id_requests.add(mte.get_id())
					logfile.write(u"%s\n" % mte)
					exception_count += 1

				line = rl(infile)
				line_count += 1
				time.sleep(0.11)  # Work around request quotas

		endtime = time.time()
		elapsed_time = endtime - starttime
		success_rate = success_count * 1.0 / (success_count + exception_count)
		logfile.write(u"%.2f %% of triples successfully extracted.\n" % success_rate * 100)
		logfile.write(u"%i queries sent.\n" % query_count)
		logfile.write(u"Process took %.3f minutes (%.2f line per second).\n" % (elapsed_time / 60.0, line_count * 1.0 /
																				elapsed_time))


def fetch_name(fbid, lang='de'):
	"""
	Looks for the translation of a *Freebase* id in a target language.

	Args:
		fbid (str): Freebase ID to be translated
		lang (str): Target language of the translation process (default is \"de\" for german).

	Raises:
		MissingTranslationException: If no translation is found.

	Returns:
		str: Translation of *Freebase* ID
	"""
	api_key, service_url = read_credentials()

	query = [{
		'id': '%s' % fbid,
		'name': [{
			'lang': '/lang/%s' % lang,
			'value': None
		}]
	}]
	response = freebase_request(query, api_key, service_url)
	if not response[u'result']:
		raise MissingTranslationException("No associated alias for id %s in language %s found in Freebase." %(fbid, lang))
	return response[u'result'][0][u'name'][0][u'value']


def freebase_request(query, api_key, service_url):
	"""
	Sends a request to the *Freebase* API.

	Args:
		query (list): ``MQL`` query as a dictionary wrapped inside a list
		api_key (str): API key
		service_url (str): URI to API

	Returns:
		dict: Response as a dictionary
	"""
	params = {'query': json.dumps(query), 'key': api_key}
	url = service_url + '?' + urllib.urlencode(params)
	response = json.loads(urllib.urlopen(url).read())
	return response


def read_credentials():
	"""
	Reads API credentials from a file.

	Returns:
		tuple: API key and API URI as strings
	"""
	api_key = open("../../../rsc/rel/freebase_api_key").read()
	service_url = 'https://www.googleapis.com/freebase/v1/mqlread'
	return api_key, service_url


def format_fbid(fbid):
	"""
	Transform the format of the *Freebase* IDs from the format used in the dataset to the format used in requests.

	Args:
		fbid (str): *Freebase* ID to be formatted.

	Returns:
		str: Formatted *Freebase* ID.
	"""
	return re.sub(r'm\.', '/m/', fbid)


def rl(infile):
	"""
	Lazy function to read a line from a while and remove redundant whitespaces.

	Args:
		infile (str): Path to input file.

	Returns:
		str: Stripped line
	"""
	return infile.readline().strip()


def init_optparser():
	"""
	Initialize the option parser for this script.

	Returns:
		OptionParser: OptionParser object
	"""
	optparser = optparse.OptionParser()
	optparser.add_option('--in', dest='input', help="Path to input file.")
	optparser.add_option('--out', dest='output', help="Path to output file.")
	optparser.add_option('--log', dest='log', help="Path to logging file.")
	optparser.add_option('--lang', dest='lang', help="Target language.", default="de")
	return optparser


class MissingTranslationException(Exception):
	"""
	Exception class to be thrown in cases where the API cannot find a translation for a *Freebase* API given the
	target language.

	"""
	def get_id(self):
		"""
		Return the *Freebase* ID that triggered this exception.

		Returns:
			str: *Freebase* ID that triggered this exception.
		"""
		return re.findall(r'/m/\w+', self.message)[0]

if __name__ == '__main__':
	main()
