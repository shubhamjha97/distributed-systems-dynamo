import copy

from merkle_tree import md5int


class VectorClock(object):
    def __init__(self):
        self.clock = {}

    def update(self, node, timestamp):
        if node in self.clock and timestamp <= self.clock[node]:
            raise Exception("Node %s has gone backwards from %d to %d" %
                            (node, self.clock[node], timestamp))
        self.clock[node] = timestamp
        return self

    def __hash__(self):
        return md5int(list(self.clock))

    def __str__(self):
        return "{%s}" % ", ".join(["%s:%d" % (node, self.clock[node])
                                   for node in sorted(self.clock.keys())])

    def __eq__(self, other):
        return self.clock == other.clock

    def __lt__(self, other):
        for node in self.clock:
            if node not in other.clock:
                return False
            if self.clock[node] > other.clock[node]:
                return False
        return True

    def __ne__(self, other):
        return not (self == other)

    def __le__(self, other):
        return (self == other) or (self < other)

    def __gt__(self, other):
        return other < self

    def __ge__(self, other):
        return (self == other) or (self > other)

    @classmethod
    def combine(cls, vcs):
        results = []
        for vc in vcs:
            subsumed = False
            for ii, result in enumerate(results):
                if vc <= result:
                    subsumed = True
                    break
                if result < vc:
                    results[ii] = copy.deepcopy(vc)
                    subsumed = True
                    break
            if not subsumed:
                results.append(copy.deepcopy(vc))
        return results

    @classmethod
    def coalesce2(cls, vcs):
        results = []
        for obj, vc in vcs:
            if vc is None:
                vc = VectorClock()
            subsumed = False
            for ii, (resultobj, resultvc) in enumerate(results):
                if vc <= resultvc:
                    subsumed = True
                    break

                if resultvc < vc:
                    results[ii] = (obj, copy.deepcopy(vc))
                    subsumed = True
                    break
            if not subsumed:
                results.append((obj, copy.deepcopy(vc)))
        return results

    @classmethod
    def converge(cls, vcs):
        result = cls()
        for vc in vcs:
            if vc is None:
                continue
            for node, counter in vc.clock.items():
                if node in result.clock:
                    if result.clock[node] < counter:
                        result.clock[node] = counter
                else:
                    result.clock[node] = counter
        return result


import unittest


class VectorClockTestCase(unittest.TestCase):

    def setUp(self):
        self.c1 = VectorClock()
        self.c1.update('A', 1)
        self.c2 = VectorClock()
        self.c2.update('B', 2)

    def testSmall(self):
        self.assertEquals(str(self.c1), "{A:1}")
        self.c1.update('A', 2)
        self.assertEquals(str(self.c1), "{A:2}")
        self.c1.update('A', 200)
        self.assertEquals(str(self.c1), "{A:200}")
        self.c1.update('B', 1)
        self.assertEquals(str(self.c1), "{A:200, B:1}")

    def testInternalError(self):
        self.assertRaises(Exception, self.c2.update, 'B', 1)

    def testEquality(self):
        self.assertEquals(self.c1 == self.c2, False)
        self.assertEquals(self.c1 != self.c2, True)
        self.c1.update('B', 2)
        self.c2.update('A', 1)
        self.assertEquals(self.c1 == self.c2, True)
        self.assertEquals(self.c1 != self.c2, False)

    def testOrder(self):
        self.assertEquals(self.c1 < self.c2, False)
        self.assertEquals(self.c2 < self.c1, False)
        self.assertEquals(self.c1 <= self.c2, False)
        self.assertEquals(self.c2 <= self.c1, False)
        self.c1.update('B', 2)
        self.assertEquals(self.c1 < self.c2, False)
        self.assertEquals(self.c2 < self.c1, True)
        self.assertEquals(self.c1 <= self.c2, False)
        self.assertEquals(self.c2 <= self.c1, True)
        self.assertEquals(self.c1 > self.c2, True)
        self.assertEquals(self.c2 > self.c1, False)
        self.assertEquals(self.c1 >= self.c2, True)
        self.assertEquals(self.c2 >= self.c1, False)

    def testCoalesce(self):
        self.c1.update('B', 2)
        self.assertEquals(VectorClock.combine((self.c1, self.c1, self.c1)), [self.c1])
        c3 = copy.deepcopy(self.c1)
        c4 = copy.deepcopy(self.c1)
        c3.update('X', 200)
        c4.update('Y', 100)
        self.assertEquals(VectorClock.combine(((self.c1, c3, c4))), [c3, c4])
        self.assertEquals(VectorClock.combine((c3, self.c1, c3, c4)), [c3, c4])

    def testConverge(self):
        self.c1.update('B', 1)
        c3 = copy.deepcopy(self.c1)
        c4 = copy.deepcopy(self.c1)
        c3.update('X', 200)
        self.c1.update('Y', 100)
        cx = VectorClock.converge((self.c1, self.c2, c3, c4))
        self.assertEquals(str(cx), "{A:1, B:2, X:200, Y:100}")
        cy = VectorClock.converge(VectorClock.combine((self.c1, self.c2, c3, c4)))
        self.assertEquals(str(cy), "{A:1, B:2, X:200, Y:100}")


if __name__ == "__main__":
    unittest.main()
