import codecs
from numpy.random import uniform


def load_data():
	pass  # Load vectors
	pass  # Load relation


def train_relation(model, relations):
	dimensions = model[0].shape[0]
	link = uniform(-(6.0/dimensions**(1/2)), (6.0/dimensions**(1/2)), dimensions)  # initialize link

