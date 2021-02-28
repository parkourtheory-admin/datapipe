'''
Generate graph masks for extrapolation task. Here the traininig mask is the largest component, and validation and testing are nodes outside this main component.
This is supposed to simulate new node entering the graph that have no prerequisite moves.
'''
import os
import networkx as nx

class ExtrapolationMask(object):
	def __init__(self, config):
		self.cfg = config


	'''
	Masks for training on largest connected component and validation on all other components

	inputs:
	G           (nx.Graph) Graph data
	train_split (float)    Training split percentage
	val_split   (float)    Validation split percentage
	test_split  (float)    Test split percentage

	outputs:
	train_mask  (ndarray) Binary mask containing 1 at positions corresponding to nodes to train on
	val_mask    (ndarray) Binary mask containing 1 at positions correpsonding to nodes to validate on
	test_mask   (ndarray) Binary mask containing 1 at positions correpsonding to nodes to test on
	'''
	def get_component_mask(G, train_split, val_split, test_split):
		assert 0 <= test_split <= 1

		# TODO: How does dgl map node names to ids when converting from networks?
		g = dgl.to_networkx(G).to_undirected()
		num_nodes = len(g.nodes())
		train_mask = np.zeros(num_nodes)
		test_mask = None

		# create training mask from largest connected component
		for i in max(nx.connected_components(G), key=len):
			train_mask[i] = 1 # TODO: need map from strings to ids

		val_mask = 1-train_mask

		if test_split:
			val_idx = np.where(val_mask == 1)[0]
			test_idx = np.random.choice(val_idx, int(test_split*num_nodes))
			test_mask = val_mask[test_idx]
			val_mask -= test_mask

		return train_mask, val_mask, test_mask


	def run(self):
		with open(os.path.join(self.cfg.video_csv_out, 'adjlist'), 'r') as file:
			G = nx.Graph(json.load(file))
			train_mask, val_mask, test_mask = get_component_mask(G, self.train_split, self.val_split, self.test_split)