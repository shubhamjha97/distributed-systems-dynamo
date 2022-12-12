import hashlib
from collections import MutableMapping


def md5string(key):
    return hashlib.md5(str(key).encode("utf-8"))


def md5int(key):
    hash_str = md5string(key)
    return int(hash_str.hexdigest(), 16)


class MerkleTreeNode(object):
    def __init__(self):
        self.value = None
        self.parent = None

    def recalc_node_val(self):
        raise NotImplementedError


class MerkleBranchNode(MerkleTreeNode):

    def __init__(self, left, right):
        super(MerkleBranchNode, self).__init__()
        self.left = left
        left.parent = self
        self.right = right
        right.parent = self
        self.recalc_node_val()

    def recalc_node_val(self):
        self.value = hashlib.md5(self.left.value.digest() + self.right.value.digest())
        if self.parent is not None:
            self.parent.recalc_node_val()

    def __str__(self):
        return self.value.hexdigest()[:6]


class MerkleLeaf(MerkleTreeNode):

    def __init__(self, min_key, max_key, initdata=None):
        super(MerkleLeaf, self).__init__()
        self.min_key = min_key
        self.max_key = max_key

        if initdata is None:
            self._data = {}
        else:
            self._data = dict([(key, value) for key, value in initdata.items() if self._inrange(key)])
        self.value = md5string(self._data)

    def __str__(self):
        return "[%s,%s)=>%s" % (self.min_key, self.max_key, self.value.hexdigest()[:6])

    def _inrange(self, key):
        hashval = md5int(key)
        return hashval >= self.min_key and hashval < self.max_key

    def recalc_node_val(self):
        self.value = md5string(self._data)
        self.parent.recalc_node_val()


class MerkleTree(MutableMapping):
    def __init__(self, depth=12, min_key=0, max_key=(2 ** 128 - 1), initdata=None):
        self.min_key = min_key
        self.max_key = max_key
        self.depth = depth
        self.num_leaves = 2 ** self.depth
        self.leaf_size = ((self.max_key - self.min_key) + self.num_leaves - 1) // self.num_leaves

        self.nodes = []
        self.nodes.append([MerkleLeaf(self.min_key + ii * self.leaf_size,
                                      min(self.min_key + (ii + 1) * self.leaf_size,
                                          max_key),
                                      initdata)
                           for ii in range(self.num_leaves)])
        level = 1
        while level <= self.depth:
            self.nodes.append([MerkleBranchNode(self.nodes[level - 1][2 * ii],
                                                self.nodes[level - 1][2 * ii + 1])
                               for ii in range(len(self.nodes[level - 1]) // 2)])
            level = level + 1
        self.root = self.nodes[-1][0]

    def _lookup(self, key):
        hashval = md5int(key)
        if hashval < self.min_key or hashval >= self.max_key:
            raise KeyError("Key %s hashes to value outside range for this tree" % key)
        return hashval // self.leaf_size

    def __setitem__(self, key, value):
        leafidx = self._lookup(key)
        self.nodes[0][leafidx]._data[key] = value
        self.nodes[0][leafidx].recalc_node_val()

    def __delitem__(self, key):
        leafidx = self._lookup(key)
        del self.nodes[0][leafidx]._data[key]
        self.nodes[0][leafidx].recalc_node_val()

    def __getitem__(self, key):
        leafidx = self._lookup(key)
        return self.nodes[0][leafidx]._data[key]

    def __contains__(self, key):
        leafidx = self._lookup(key)
        return key in self.nodes[0][leafidx]._data

    def keys(self):
        results = []
        for leafidx in range(self.num_leaves):
            results.extend(self.nodes[0][leafidx]._data.keys())
        return results

    def __iter__(self):
        for leafidx in range(self.num_leaves):
            for key in self.nodes[0][leafidx]._data:
                yield key

    def iteritems(self):
        for leafidx in range(self.num_leaves):
            for key, value in self.nodes[0][leafidx]._data.items():
                yield key, value

    def __len__(self):
        return self.num_leaves

    def __str__(self):
        result = ""
        for level, nodelist in enumerate(self.nodes):
            result = result + "[%d] " % level
            for node in nodelist:
                result = result + str(node) + ' '
            result = result + '\n'
        return result


import sys
import copy
import random
import unittest

from utils import random_3str


class MerkleTestCase(unittest.TestCase):

    def setUp(self):
        self.keystore = dict((random_3str(), random.randint(0, 99)) for _ in range(50))
        self.keya = int(hashlib.md5('A').hexdigest(), 16)
        self.keyb = int(hashlib.md5('B').hexdigest(), 16)
        if self.keya < self.keyb:
            self.min_key = self.keya
            self.max_key = self.keyb
        else:
            self.min_key = self.keyb
            self.max_key = self.keya

    def testCreation(self):
        x = MerkleTree(3, self.min_key, self.max_key, self.keystore)
        xs = str(x)
        return xs

    def testCompare(self):
        keystore2 = copy.copy(self.keystore)
        keystore2['A'] = 'xyzzy'
        x0 = MerkleTree(3, self.min_key, self.max_key, self.keystore)
        x1 = MerkleTree(3, self.min_key, self.max_key, self.keystore)
        x2 = MerkleTree(3, self.min_key, self.max_key, keystore2)
        x0t = x0.root
        x1t = x1.root
        x2t = x2.root
        self.assertEqual(x0t.value.hexdigest(), x1t.value.hexdigest())
        self.assertNotEqual(x1t.value.hexdigest(), x2t.value.hexdigest())
        x1L = x1t.left
        x1R = x1t.right
        x2L = x2t.left
        x2R = x2t.right
        if x1L.value.hexdigest() == x2L.value.hexdigest():
            self.assertNotEqual(x1R.value.hexdigest(), x2R.value.hexdigest())
        else:
            self.assertEqual(x1R.value.hexdigest(), x2R.value.hexdigest())
            self.assertNotEqual(x1L.value.hexdigest(), x2L.value.hexdigest())

    def testLeafIdx(self):
        x = MerkleTree()
        for _ in range(10000):
            key = random_3str()
            leafidx = x._lookup(key)
            if not x.nodes[0][leafidx]._inrange(key):
                raise KeyError("Key %s hashes to value outside range for this leaf" % key)

    def testDict(self):
        d1 = MerkleTree()
        d1['a'] = 1
        d1['b'] = 2
        hash1 = d1.root.value.hexdigest()
        self.assertEqual(d1['a'], 1)
        self.assertEqual(d1['b'], 2)
        self.assertEqual(len(d1), 2)
        self.assertTrue('a' in d1)
        self.assertTrue('b' in d1)
        self.assertFalse('c' in d1)
        del d1['a']
        self.assertNotEqual(hash1, d1.root.value.hexdigest())
        self.assertRaises(KeyError, d1.__getitem__, *('a',))
        self.assertEqual(d1['b'], 2)
        self.assertEqual(len(d1), 1)
        self.assertFalse('a' in d1)
        self.assertTrue('b' in d1)
        self.assertFalse('c' in d1)

        d2 = MerkleTree(initdata={'a': 1, 'b': 2})
        self.assertEqual(d2['a'], 1)
        self.assertEqual(d2['b'], 2)
        self.assertEqual(len(d2), 2)
        self.assertNotEquals(d1, d2)
        del d2['a']
        self.assertEqual(d1, d2)

        d2.clear()
        self.assertFalse('a' in d2)
        self.assertFalse('b' in d2)
        self.assertFalse('c' in d2)

    def test002(self):
        d1 = MerkleTree(initdata={'a': 1, 'b': 2, 'c': 3})
        d2 = MerkleTree(initdata={'a': 1, 'b': 2, 'c': 3})
        self.assertEqual(d1, d2)
        self.assertEqual(d1.pop('a'), 1)
        self.assertEqual(d1.pop('b'), 2)
        self.assertEqual(d1.pop('x', 'yy'), 'yy')
        self.assertEqual(d1.popitem(), ('c', 3))

        d2.update({'x': 8, 'y': 9})
        self.assertEqual({'a', 'b', 'c', 'x', 'y'},
                         set(d2.keys()))
        d2.update((('u', 10), ('v', 11), ('w', 12)))
        self.assertEqual({'a', 'b', 'c', 'u', 'v', 'w', 'x', 'y'},
                         set(d2.keys()))


if __name__ == "__main__":
    ii = 1
    while ii < len(sys.argv):
        arg = sys.argv[ii]
        if arg == "-s" or arg == "--seed":
            random.seed(sys.argv[ii + 1])
            del sys.argv[ii:ii + 2]
        else:
            ii = ii + 1
    unittest.main()
