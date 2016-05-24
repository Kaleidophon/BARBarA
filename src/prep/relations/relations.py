#!/usr/bin/python
# -*- coding: utf-8 -*-

import json, urllib, re, codecs, sys, time
from collections import defaultdict
import optparse


def main():
	optparser = optparse.OptionParser()
	optparser.add_option('--in', dest='input', help="Path to input file.")
	optparser.add_option('--out', dest='output', help="Path to output file.")
	optparser.add_option('--log', dest='log', help="Path to logging file.")
	optparser.add_option('--task', dest='task', help="Task? (fetch_triplets / translate_qa)")
	optparser.add_option('--lang', dest='lang', help="Target language.", default="de")
	(options, args) = optparser.parse_args()
	if options.task == "fetch_triplets":
		fetch_relation_triples_of_file(options.input, options.output, options.log, options.lang)
	elif options.task == "translate_qa":
		translate_word2vec_question_phrases(options.input, options.output, options.lang)


def translate_word2vec_question_phrases(inpath, outpath, lang="en"):
	with codecs.open(inpath, "rb", "utf-8") as question_phrases_file, codecs.open(outpath, "wb", "utf-8") as outfile:
		requests = defaultdict(str)
		n, failed = 0, 0
		for line in question_phrases_file:
			n += 1
			if line.startswith(":"): continue
			line = line.strip()
			line_parts = line.split(" ")
			print line
			found_none = False
			new_parts = []
			for part in line_parts:
				if part not in requests.keys():
					translation = translate_name(part.replace("_", " "), lang)
					translation = translation.replace(" ", "_") if translation is not None else None
					requests[part] = translation
				else:
					translation = requests[part]
				if not translation:
					failed += 1
					break
				new_parts.append(translation)
			else:
				outfile.write("%s\n" %(" ".join(new_parts)))
		print "%i of %i translations successful (%.2f %%)" %(n-failed, n, (n-failed)*100.0/n)


def fetch_relation_triples_of_file(inpath, outpath, logpath, lang="en"):
	starttime = time.time()
	id_dict = defaultdict(str)
	failed_id_requests = set()
	with codecs.open(inpath, "rb", "utf-8") as infile, codecs.open(logpath, "wb", "utf-8") as logfile:
		line = rl(infile)
		exception_count = success_count = line_count = query_count = 0
		with codecs.open(outpath, "wb", "utf-8") as outfile:
			while line != "":
				if line_count % 100 == 0:
					logfile.write(u"Line nr. %i\n" %(line_count))
					logfile.write(u"Stored %i IDs so far...\n" %(len(id_dict.keys())))
				logfile.write(u"Processing '%s'...\n" %(line))
				parts = line.split("\t")
				try:
					id1, id2 = format_fbid(parts[0]), format_fbid(parts[1])

					# If name for id not available in language
					if id1 in failed_id_requests:
						raise MissingTranslationException("No associated alias for id %s in language %s found in Freebase." %(id1, lang))
					elif id2 in failed_id_requests:
						raise MissingTranslationException("No associated alias for id %s in language %s found in Freebase." %(id2, lang))

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

					outfile.write('- [%s, %s, %s, %s, %s]\n' %(entity1, id1, parts[2], entity2, id2))
					success_count += 1
				except MissingTranslationException as mte:
					failed_id_requests.add(mte.get_id())
					logfile.write(u"%s\n" %(mte))
					exception_count += 1

				line = rl(infile)
				line_count += 1
				time.sleep(0.11)

		endtime = time.time()
		elapsed_time = endtime - starttime
		success_rate = success_count * 1.0 / (success_count + exception_count)
		logfile.write(u"%.2f %% of triples successfully extracted.\n" %(success_rate * 100))
		logfile.write(u"%i queries sent.\n" %(query_count))
		logfile.write(u"Process took %.3f minutes (%.2f line per second).\n" %(elapsed_time / 60.0, line_count * 1.0 / elapsed_time))


def fetch_relation_triples(relation):
	api_key, service_url = read_credentials()
	# query = [{'id': None, 'name': None, 'type': '/astronomy/planet'}]

	query = [{
		'%s' %(relation): [{
			'name': None
		}],
		'name': None
	}]
	response = freebase_request(query, api_key, service_url)
	results = response['result']
	relation_triples = []

	# TODO: Relationen für aequivalente und hierarchische 1:n, m:1 und m:n-Relationen

	for entry in results:
		if entry[u'name'] is not None:
			relation_triples.append((entry[relation][0][u'name'], relation, entry[u'name']))
		else:
			relation_triples.append((entry[relation][0][u'name'], relation, entry[relation][1][u'name']))
	return relation_triples


def translate_name(name, lang="en"):
	api_key, service_url = read_credentials()
	query = [{
		"id":"/en/%s" %(name.lower().replace(" ", "_")),
		"name":[{
			"lang": "/lang/%s" %(lang),
			"value": None
		}]
	}]
	response = freebase_request(query, api_key, service_url)
	if u"error" in response.keys():
		return None
	results = response['result']
	if len(results) == 0:
		return None
	else:
		return results[0]['name'][0]['value']


def fetch_name(id, lang='en'):
	api_key, service_url = read_credentials()
	# query = [{'id': None, 'name': None, 'type': '/astronomy/planet'}]

	query = [{
		'id': '%s' %(id),
		'name': [{
			'lang': '/lang/%s' %(lang),
			'value': None
		}]
	}]
	response = freebase_request(query, api_key, service_url)
	if response[u'result'] == []:
		raise MissingTranslationException("No associated alias for id %s in language %s found in Freebase." %(id, lang))
	return response[u'result'][0][u'name'][0][u'value']


def freebase_request(query, api_key, service_url):
	params = {'query': json.dumps(query), 'key': api_key}
	url = service_url + '?' + urllib.urlencode(params)
	response = json.loads(urllib.urlopen(url).read())
	return response


def read_credentials():
	api_key = open("../../../rsc/rel/freebase_api_key").read()
	service_url = 'https://www.googleapis.com/freebase/v1/mqlread'
	return api_key, service_url


def format_fbid(id):
	return re.sub(r'm\.', '/m/', id)


def rl(infile):
	return infile.readline().strip()


class MissingTranslationException(Exception):
	def get_id(self):
		return re.findall(r'/m/\w+', self.message)[0]

if __name__ == '__main__':
	main()