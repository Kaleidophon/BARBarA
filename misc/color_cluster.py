from sklearn.cluster import DBSCAN
from sklearn.decomposition import PCA

import matplotlib
matplotlib.use('Agg')

import matplotlib.font_manager
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
import matplotlib.cm as cmx
import matplotlib.colors as colors
import numpy
from collections import defaultdict
import codecs
import sys
from sklearn import preprocessing
from random import shuffle


def main():
    infile = sys.argv[1]

    print "Load mappings..."
    indices, model = load_mappings_from_model(infile)
    X = numpy.array([model[key] for key in indices])
    print X
    X = preprocessing.scale(X)
    print "Start clustering..."
    dbscan = DBSCAN(eps=2.5, min_samples=20, p=2)
    dbscan.fit(X)
    labels = dbscan.labels_

    print "Start plotting..."
    clusters = aggregate_cluster(indices, labels)
    cmap = get_cmap(len(clusters.keys())+1)
    print "#clusters:", len(clusters.keys())+1
    randomizer = create_randomizer(len(clusters.keys())+1)

    point_colors = []
    for label in labels:
        point_colors.append(cmap(randomizer[label+1]))

    pca = PCA(n_components=3)
    pca.fit(X)
    print pca.explained_variance_ratio_
    X = pca.transform(X)
    xs = X[:, 0]
    ys = X[:, 1]
    zs = X[:, 2]
    fig = plt.figure()
    ax = Axes3D(fig)
    ax.scatter(xs, ys, zs, c=point_colors)
    plt.savefig("./fig.jpg", format="jpg")
    #plt.show()

def get_cmap(N):
    '''Returns a function that maps each index in 0, 1, ... N-1 to a distinct
    RGB color.'''
    color_norm = colors.Normalize(vmin=0, vmax=N)
    scalar_map = cmx.ScalarMappable(norm=color_norm, cmap='hsv')
    def map_index_to_rgb_color(index):
        return scalar_map.to_rgba(index)
    return map_index_to_rgb_color

def aggregate_cluster(points, labels):
	print "Aggregate clusters..."
	clusters = defaultdict(tuple)
	for i in range(len(labels)):
		label = labels[i]
		if label in clusters.keys():
			clusters[label].append(points[i])
		else:
			clusters[label] = [points[i]]
	return clusters

def load_mappings_from_model(mapping_inpath):
    indices_list = []
    mappings_dict = defaultdict(numpy.array)
    with codecs.open(mapping_inpath, "rb", "utf-8") as mapping_infile:
        line = mapping_infile.readline().strip()
        while line != "":
            parts = line.strip().split(" ")
            indices = (int(parts[0]), int(parts[1]))
            if len(parts[2:]) >= 100:
                vector = numpy.array([float(dimension.strip()) for dimension in parts[2:]])
                indices_list.append(indices)
                mappings_dict[indices] = vector
            line = mapping_infile.readline().strip()
	return indices_list, mappings_dict

def create_randomizer(n):
    a = range(n)
    b = range(n)
    shuffle(b)
    return dict(zip(a, b))

if __name__ == "__main__":
    main()
