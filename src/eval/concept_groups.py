#!/usr/bin/python
# -*- coding: utf-8 -*-

import optparse, codecs, random, sys, json, urllib, re, yaml, math
from sets import Set
from collections import defaultdict


def main():
	optparser = optparse.OptionParser()
	optparser.add_option('-s',
						 dest='input',
						 help='Sample relations for creation of concept groups.')
	optparser.add_option('--out',
						 dest='output',
						 help='path to output file')
	optparser.add_option('-n',
						 dest='n',
						 type='int',
						 help='number of samples')
	optparser.add_option('--size',
						 dest='size',
						 type='int',
						 help='sample size')
	optparser.add_option('-c',
						  dest='constraint',
						  default=0,
						  type='int',
						  help='frequency constraint')

	(options, args) = optparser.parse_args()
	if '-s' in sys.argv:
		sampleRelations(options.input, options.output, options.n, options.size, options.constraint)


def sampleRelations(inpath, outpath, n, sample_size, freq_constraint):
	with open(inpath, 'r') as infile:
		relation_tuples = yaml.load(infile)

	keys = Set([])
	relation_freqs = defaultdict(str)
	relation_pairs = defaultdict(str)
	for relation_tuple in relation_tuples:
			relation = relation_tuple[2]
			head = relation_tuple[0]
			tail = relation_tuple[3]
			if relation not in keys:
				relation_freqs[relation] = 1
				relation_pairs[relation] = [(head, tail)]
				keys.add(relation)
			else:
				relation_freqs[relation] += 1
				relation_pairs[relation].append((head, tail))

	relations_list = [relation for relation in keys if relation_freqs[relation] >= freq_constraint and
									len(relation_pairs[relation]) > sample_size]
	relations_list, sampled_relations = take_sample_from_list(relations_list, n*2)

	concept_groups = []
	while len(concept_groups) != n:
		# Inspect next relation
		relation = sampled_relations[len(sampled_relations)-1]
		coin_flip = random.randint(0, 1) # Decide from which end to take the entities from
		concept_group = sample_part(relation_pairs[relation], coin_flip, sample_size)
		if len(concept_group) == sample_size:
			concept_groups.append(concept_group)
		else:
			# Try the other side
			concept_group = sample_part(relation_pairs[relation], int(math.fabs(coin_flip-1)), sample_size)
			if len(concept_group) == sample_size:
				concept_groups.append(concept_group)
			if len(relations_list) == 0:
				raise Exception('No more relations to sample!')
			if len(sampled_relations) == 0:
				# Sample new relation
				relations_list, sampled_relations = take_sample_from_list(relations_list, 1)
		sampled_relations.pop() # Remove the relations used

	for concept_group in concept_groups:
		print '%i:\t%s' % (len(concept_group), str(concept_group))


def sample_part(relation_pairs, coin_flip, sample_size):
	concept_group = []
	for pair in relation_pairs:
		if pair[coin_flip] not in concept_group:
			concept_group.append(pair[coin_flip])
		if len(concept_group) == sample_size:
			break
	return concept_group


def take_sample_from_list(samplelist, n):
	if n > samplelist or len(samplelist) == 0:
		raise Exception('Not enough elements to satisfy sampling condition')
	samples = []
	while len(samples) != n:
		if len(samplelist) == 0:
			raise Exception('No more elements to sample!')
		#print "Samples: %s, %i" %(str(samples), len(samples))
		#print "Samplelist: %i" %(len(samplelist))
		sample_index = random.randint(0, len(samplelist))
		#print sample_index
		samples.append(samplelist[sample_index])
		samplelist.pop(sample_index)
	return samplelist, samples


if __name__ == '__main__':
	main()
