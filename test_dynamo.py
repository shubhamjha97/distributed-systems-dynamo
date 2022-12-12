import sys
import random
import unittest
import logging

import messages
from emulation import Emulation, reset_all
from basenode import BaseNode
from history import History
import history
import logconfig

import dynamo

logconfig.init_logging()
_logger = logging.getLogger('dynamo')


class TestDynamo(unittest.TestCase):
    def setUp(self):
        _logger.info("Reset for next test")
        reset_all()
        dynamo.Node.reset()
        dynamo.Node.reset()
        dynamo.Node.reset()
        dynamo.Node.reset()
        dynamo.Node.reset()

    def tearDown(self):
        _logger.info("Reset after last test")
        reset_all()

    def test_simple_put(self):
        for _ in range(6):
            dynamo.Node()
        a = dynamo.Client('a')
        a.put('K1', None, 1)
        Emulation.run()
        print(History.ladder())

    def test_simple_get(self):
        for _ in range(6):
            dynamo.Node()
        a = dynamo.Client('a')
        a.put('K1', None, 1)
        Emulation.run()
        from_line = len(History.history)
        a.get('K1')
        Emulation.run()
        print(History.ladder(start_line=from_line))

    def test_double_put(self):
        for _ in range(6):
            dynamo.Node()
        a = dynamo.Client('a')
        b = dynamo.Client('b')
        a.put('K1', None, 1)
        Emulation.run(1)
        b.put('K2', None, 17)
        Emulation.run()
        print(History.ladder(spacing=14))

    def test_put1_fail_initial_node(self):
        self.put_fail_initial_node(dynamo)

    def test_put2_fail_initial_node(self):
        self.put_fail_initial_node(dynamo)

    def put_fail_initial_node(self, cls):
        for _ in range(6):
            cls.Node()
        a = cls.Client('a')
        destnode = random.choice(cls.Node.node_list)
        a.put('K1', None, 1, destnode=destnode)
        destnode.fail()
        Emulation.run()
        print(History.ladder())

    def test_put1_fail_initial_node2(self):
        self.put_fail_initial_node2(dynamo)

    def test_put2_fail_initial_node2(self):
        self.put_fail_initial_node2(dynamo)

    def put_fail_initial_node2(self, _):
        for _ in range(6):
            dynamo.Node()
        a = dynamo.Client('a')
        destnode = random.choice(dynamo.Node.node_list)
        a.put('K1', None, 1, destnode=destnode)
        Emulation.run(1)
        destnode.fail()
        Emulation.run()
        print(History.ladder())

    def test_put1_fail_node2(self):
        self.put_fail_node2(dynamo)

    def test_put2_fail_node2(self):
        self.put_fail_node2(dynamo)

    def put_fail_node2(self, cls):
        for _ in range(6):
            cls.Node()
        a = cls.Client('a')
        a.put('K1', None, 1)
        pref_list = cls.Node.consistent_hash_tbl.find_nodes('K1', 3)[0]
        Emulation.run(1)
        pref_list[1].fail()
        Emulation.run()
        a.get('K1')
        Emulation.run()
        print(History.ladder())

    def test_put1_fail_nodes23(self):
        self.put_fail_nodes23(dynamo)
        print(History.ladder(spacing=16))

    def test_put2_fail_nodes23(self):
        (_, pref_list) = self.put_fail_nodes23(dynamo)
        print(History.ladder(force_include=pref_list, spacing=16))

    def put_fail_nodes23(self, cls):
        for _ in range(6):
            cls.Node()
        a = cls.Client('a')
        pref_list = cls.Node.consistent_hash_tbl.find_nodes('K1', 5)[0]
        a.put('K1', None, 1, destnode=pref_list[0])
        Emulation.run(1)
        pref_list[1].fail()
        pref_list[2].fail()
        Emulation.run(timers_to_process=2)
        return a, pref_list

    def test_put2_fail_nodes23_2(self):
        (a, pref_list) = self.put_fail_nodes23(dynamo)
        coordinator = pref_list[0]
        from_line = len(History.history)
        a.put('K1', None, 2, destnode=coordinator)
        Emulation.run()
        print(History.ladder(force_include=pref_list, start_line=from_line, spacing=16))

    def test_put2_fail_nodes23_3(self):
        (a, pref_list) = self.put_fail_nodes23(dynamo)
        coordinator = pref_list[0]
        a.put('K1', None, 2, destnode=coordinator)
        Emulation.run(timers_to_process=0)
        from_line = len(History.history)
        Emulation.run(timers_to_process=3)
        print(History.ladder(force_include=pref_list, start_line=from_line, spacing=16))

    def test_put2_fail_nodes23_4a(self):
        (a, pref_list) = self.put_fail_nodes23(dynamo)
        coordinator = pref_list[0]
        a.put('K1', None, 2, destnode=coordinator)
        Emulation.run(timers_to_process=10)
        from_line = len(History.history)
        pref_list[1].recover()
        pref_list[2].recover()
        Emulation.run(timers_to_process=10)
        a.get('K1', destnode=coordinator)
        Emulation.run(timers_to_process=0)
        print(History.ladder(force_include=pref_list, start_line=from_line, spacing=16))

    def test_put2_fail_nodes23_4b(self):
        (a, pref_list) = self.put_fail_nodes23(dynamo)
        coordinator = pref_list[0]
        a.put('K1', None, 2, destnode=coordinator)
        Emulation.run(timers_to_process=10)
        from_line = len(History.history)
        pref_list[1].recover()
        pref_list[2].recover()
        Emulation.run(timers_to_process=15)
        a.put('K1', None, 3, destnode=coordinator)
        Emulation.run(timers_to_process=5)
        print(History.ladder(force_include=pref_list, start_line=from_line, spacing=16))

    def test_put2_fail_nodes23_5(self):
        (a, pref_list) = self.put_fail_nodes23(dynamo)
        coordinator = pref_list[0]
        from_line = len(History.history)
        a.put('K1', None, 2, destnode=coordinator)
        Emulation.run(timers_to_process=10)
        print(History.ladder(force_include=pref_list, start_line=from_line, spacing=16))

    def test_put2_fail_nodes23_6(self):
        (a, pref_list) = self.put_fail_nodes23(dynamo)
        coordinator = pref_list[0]
        a.put('K1', None, 2, destnode=coordinator)
        Emulation.run(timers_to_process=10)
        from_line = len(History.history)
        pref_list[1].recover()
        pref_list[2].recover()
        Emulation.run(timers_to_process=15)
        print(History.ladder(force_include=pref_list, start_line=from_line, spacing=16))

    def get_put_get_put(self):
        cls = dynamo
        for _ in range(6):
            cls.Node()
        a = cls.Client('a')
        pref_list = cls.Node.consistent_hash_tbl.find_nodes('K1', 5)[0]
        coordinator = pref_list[0]
        a.get('K1', destnode=coordinator)
        Emulation.run(timers_to_process=0)
        getrsp = a.prev_msg
        a.put('K1', getrsp.metadata, 1, destnode=coordinator)
        Emulation.run(timers_to_process=0)
        a.get('K1', destnode=coordinator)
        Emulation.run(timers_to_process=0)
        getrsp = a.prev_msg
        a.put('K1', getrsp.metadata, 2, destnode=coordinator)
        Emulation.run(timers_to_process=0)
        return (a, pref_list)

    def test_get_put_get_put(self):
        messages._show_metadata = True
        (a, pref_list) = self.get_put_get_put()
        print(History.ladder(force_include=pref_list, spacing=16))
        messages._show_metadata = False

    def get_put_put(self, a, coordinator):
        a.get('K1', destnode=coordinator)
        Emulation.run(timers_to_process=0)
        getrsp = a.prev_msg
        a.put('K1', getrsp.metadata, 3, destnode=coordinator)
        Emulation.run(timers_to_process=0)
        metadata = [a.prev_msg.metadata]
        a.put('K1', metadata, 4, destnode=coordinator)
        Emulation.run(timers_to_process=0)

    def test_get_put_put(self):
        messages._show_metadata = True
        (a, pref_list) = self.get_put_get_put()
        coordinator = pref_list[0]
        from_line = len(History.history)
        self.get_put_put(a, coordinator)
        print(History.ladder(force_include=pref_list, start_line=from_line, spacing=16))
        messages._show_metadata = False

    def test_metadata_simple_fail(self):
        messages._show_metadata = True
        (a, pref_list) = self.get_put_get_put()
        coordinator = pref_list[0]
        self.get_put_put(a, coordinator)
        from_line = len(History.history)
        metadata = [a.prev_msg.metadata]
        coordinator.fail()
        a.put('K1', metadata, 11, destnode=pref_list[1])
        Emulation.run(timers_to_process=0)
        a.get('K1', destnode=pref_list[1])
        Emulation.run(timers_to_process=0)
        print(History.ladder(force_include=pref_list, start_line=from_line, spacing=16))
        messages._show_metadata = False

    def partition(self):
        messages._show_metadata = True
        cls = dynamo
        A = cls.Node()
        B = cls.Node()
        C = cls.Node()
        D = cls.Node()
        E = cls.Node()
        F = cls.Node()
        a = cls.Client('a')
        b = cls.Client('b')
        all_nodes = {A, B, C, D, E, F, a, b}
        pref_list = cls.Node.consistent_hash_tbl.find_nodes('K1', 5)[0]
        coordinator = pref_list[0]
        a.get('K1', destnode=coordinator)
        Emulation.run(timers_to_process=0)
        getrsp = a.prev_msg
        a.put('K1', getrsp.metadata, 1, destnode=coordinator)
        Emulation.run(timers_to_process=0)
        a_metadata = [a.prev_msg.metadata]

        Emulation.disconnect((b, A, B, C), (D, E, F, a))
        Emulation.disconnect((D, E, F, a), (b, A, B, C))

        a.put('K1', a_metadata, 11, destnode=coordinator)
        Emulation.run(timers_to_process=2)
        a_metadata = [a.prev_msg.metadata]

        b.get('K1', destnode=coordinator)
        while b.prev_msg is None:
            Emulation.run(timers_to_process=1)
        getrsp = b.prev_msg
        b.put('K1', getrsp.metadata, 21, destnode=A)
        Emulation.run(timers_to_process=3)
        return all_nodes

    def test_partition(self):
        messages._show_metadata = True
        all_nodes = self.partition()

        print(History.ladder(force_include=all_nodes, spacing=16, key=lambda x: ' ' if x.node_to_name == 'b' else x.node_to_name))
        messages._show_metadata = False

    def partition_repair(self):
        History.add("announce", "Repair network partition")
        Emulation.unreachable_nodes = []
        Emulation.run(timers_to_process=12)

        a = BaseNode.name_to_node['a']
        a.get('K1')
        Emulation.run(timers_to_process=0)

    def test_partition_detect(self):
        messages._show_metadata = True
        all_nodes = self.partition()
        from_line = len(History.history)
        self.partition_repair()

        print(History.ladder(force_include=all_nodes, start_line=from_line, spacing=16,
                             key=lambda x: ' ' if x.node_to_name == 'b' else x.node_to_name))
        messages._show_metadata = False

    def test_partition_restore(self):
        messages._show_metadata = True
        all_nodes = self.partition()
        self.partition_repair()
        from_line = len(History.history)

        a = BaseNode.name_to_node['a']
        getrsp = a.prev_msg
        a.put('K1', getrsp.metadata, 101)
        Emulation.run(timers_to_process=0)

        print(History.ladder(force_include=all_nodes, start_line=from_line, spacing=16,
                             key=lambda x: ' ' if x.node_to_name == 'b' else x.node_to_name))
        messages._show_metadata = False

    def test_partition_detect_metadata(self):
        self.partition()
        self.partition_repair()
        a = BaseNode.name_to_node['a']
        getrsp = a.prev_msg
        print("%s@[%s]" % (getrsp.value, ",".join([str(x) for x in getrsp.metadata])))

    def test_partition_restore_metadata(self):
        self.partition()
        self.partition_repair()
        a = BaseNode.name_to_node['a']
        getrsp = a.prev_msg
        putmsg = a.put('K1', getrsp.metadata, 101)
        Emulation.run(timers_to_process=0)
        print(putmsg.metadata)


if __name__ == "__main__":
    ii = 1
    while ii < len(sys.argv):
        arg = sys.argv[ii]
        if (arg == "-s" or arg == "--seed") and (ii + 1) < len(sys.argv):
            random.seed(sys.argv[ii + 1])
            del sys.argv[ii:ii + 2]
        elif arg == "-u" or arg == "--unicode":
            history.GLYPHS = history.Glyphs
            del sys.argv[ii:ii + 1]
        else:
            ii += 1
    unittest.main()
