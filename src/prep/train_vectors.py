from gensim.models import word2vec
import argparse


def main():
	argparser = init_argparse()
	args = argparser.parse_args()


def train_model(size, workers, negative, add_context, cds):
	pass


def init_argparse():
	argparser = argparse.ArgumentParser()
	return argparser