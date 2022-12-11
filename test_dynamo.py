import sys
import codecs
import locale
import random
import unittest
import logging

# Wrap sys.stdout into a StreamWriter to allow writing unicode.
sys.stdout = codecs.getwriter(locale.getpreferredencoding())(sys.stdout)

from framework import Framework, reset_all
from basenode import BaseNode
from history import History
import history
import logconfig

import dynamomessages
import dynamo

logconfig.init_logging()
_logger = logging.getLogger('dynamo')


class SimpleTestCase(unittest.TestCase):
    """Test simple Dynamo function"""
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
        Framework.schedule()
        print(History.ladder())

    def test_simple_get(self):
        for _ in range(6):
            dynamo.Node()
        a = dynamo.Client('a')
        a.put('K1', None, 1)
        Framework.schedule()
        from_line = len(History.history)
        a.get('K1')
        Framework.schedule()
        print(History.ladder(start_line=from_line))

    def test_double_put(self):
        for _ in range(6):
            dynamo.Node()
        a = dynamo.Client('a')
        b = dynamo.Client('b')
        a.put('K1', None, 1)
        Framework.schedule(1)
        b.put('K2', None, 17)
        Framework.schedule()
        print(History.ladder(spacing=14))

    def test_put1_fail_initial_node(self):
        self.put_fail_initial_node(dynamo)

    def test_put2_fail_initial_node(self):
        self.put_fail_initial_node(dynamo)

    def put_fail_initial_node(self, cls):
        for _ in range(6):
            cls.Node()
        a = cls.Client('a')
        destnode = random.choice(cls.Node.nodelist)
        a.put('K1', None, 1, destnode=destnode)
        # Fail at the forwarding node before it gets a chance to forward
        destnode.fail()
        Framework.schedule()
        print(History.ladder())

    def test_put1_fail_initial_node2(self):
        self.put_fail_initial_node2(dynamo)

    def test_put2_fail_initial_node2(self):
        self.put_fail_initial_node2(dynamo)

    def put_fail_initial_node2(self, cls):
        for _ in range(6):
            dynamo.Node()
        a = dynamo.Client('a')
        destnode = random.choice(dynamo.Node.nodelist)
        a.put('K1', None, 1, destnode=destnode)
        # Fail at the forwarding node after it gets a chance to forward
        Framework.schedule(1)
        destnode.fail()
        Framework.schedule()
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
        # Fail the second node in the preference list
        pref_list = cls.Node.consistent_hash_tbl.find_nodes('K1', 3)[0]
        Framework.schedule(1)
        pref_list[1].fail()
        Framework.schedule()
        a.get('K1')
        Framework.schedule()
        print(History.ladder())

    def test_put1_fail_nodes23(self):
        self.put_fail_nodes23(dynamo)
        print(History.ladder(spacing=16))

    def test_put2_fail_nodes23(self):
        (_, pref_list) = self.put_fail_nodes23(dynamo)
        # Force nodes that are of interest in put2_fail_nodes23_[234] to be included in the history
        print(History.ladder(force_include=pref_list, spacing=16))

    def put_fail_nodes23(self, cls):
        # Set up 6 nodes and 1 client node
        for _ in range(6):
            cls.Node()
        a = cls.Client('a')
        # Fail the second and third node in the preference list
        pref_list = cls.Node.consistent_hash_tbl.find_nodes('K1', 5)[0]
        a.put('K1', None, 1, destnode=pref_list[0])
        Framework.schedule(1)
        pref_list[1].fail()
        pref_list[2].fail()
        Framework.schedule(timers_to_process=2)
        return a, pref_list

    def test_put2_fail_nodes23_2(self):
        """Show second request for same key skipping failed nodes"""
        (a, pref_list) = self.put_fail_nodes23(dynamo)
        coordinator = pref_list[0]
        from_line = len(History.history)
        a.put('K1', None, 2, destnode=coordinator)  # Send client request to coordinator for clarity
        Framework.schedule()
        print(History.ladder(force_include=pref_list, start_line=from_line, spacing=16))

    def test_put2_fail_nodes23_3(self):
        """Show PingReq failing"""
        (a, pref_list) = self.put_fail_nodes23(dynamo)
        coordinator = pref_list[0]
        a.put('K1', None, 2, destnode=coordinator)  # Send client request to coordinator for clarity
        Framework.schedule(timers_to_process=0)
        from_line = len(History.history)
        Framework.schedule(timers_to_process=3)
        print(History.ladder(force_include=pref_list, start_line=from_line, spacing=16))

    def test_put2_fail_nodes23_4a(self):
        """Show PingReq recovering but an inconsistent Get being returned"""
        (a, pref_list) = self.put_fail_nodes23(dynamo)
        coordinator = pref_list[0]
        a.put('K1', None, 2, destnode=coordinator)  # Send client request to coordinator for clarity
        Framework.schedule(timers_to_process=10)
        from_line = len(History.history)
        pref_list[1].recover()
        pref_list[2].recover()
        Framework.schedule(timers_to_process=10)
        a.get('K1', destnode=coordinator)
        Framework.schedule(timers_to_process=0)
        print(History.ladder(force_include=pref_list, start_line=from_line, spacing=16))

    def test_put2_fail_nodes23_4b(self):
        """Show PingReq recovering, and a subsequent Put returning to the original preference list"""
        (a, pref_list) = self.put_fail_nodes23(dynamo)
        coordinator = pref_list[0]
        a.put('K1', None, 2, destnode=coordinator)  # Send client request to coordinator for clarity
        Framework.schedule(timers_to_process=10)
        from_line = len(History.history)
        pref_list[1].recover()
        pref_list[2].recover()
        Framework.schedule(timers_to_process=15)
        a.put('K1', None, 3, destnode=coordinator)
        Framework.schedule(timers_to_process=5)
        print(History.ladder(force_include=pref_list, start_line=from_line, spacing=16))

    def test_put2_fail_nodes23_5(self):
        """Show Put after a failure including handoff, and the resulting Pings"""
        (a, pref_list) = self.put_fail_nodes23(dynamo)
        coordinator = pref_list[0]
        from_line = len(History.history)
        a.put('K1', None, 2, destnode=coordinator)  # Send client request to coordinator for clarity
        Framework.schedule(timers_to_process=10)
        print(History.ladder(force_include=pref_list, start_line=from_line, spacing=16))

    def test_put2_fail_nodes23_6(self):
        """Show hinted handoff after recovery"""
        (a, pref_list) = self.put_fail_nodes23(dynamo)
        coordinator = pref_list[0]
        a.put('K1', None, 2, destnode=coordinator)  # Send client request to coordinator for clarity
        Framework.schedule(timers_to_process=10)
        from_line = len(History.history)
        pref_list[1].recover()
        pref_list[2].recover()
        Framework.schedule(timers_to_process=15)
        print(History.ladder(force_include=pref_list, start_line=from_line, spacing=16))

    def get_put_get_put(self):
        cls = dynamo
        for _ in range(6):
            cls.Node()
        a = cls.Client('a')
        pref_list = cls.Node.consistent_hash_tbl.find_nodes('K1', 5)[0]
        coordinator = pref_list[0]
        # Send in first get-then-put
        a.get('K1', destnode=coordinator)
        Framework.schedule(timers_to_process=0)
        getrsp = a.prev_msg
        a.put('K1', getrsp.metadata, 1, destnode=coordinator)
        Framework.schedule(timers_to_process=0)
        # Send in second get-then-put
        a.get('K1', destnode=coordinator)
        Framework.schedule(timers_to_process=0)
        getrsp = a.prev_msg
        a.put('K1', getrsp.metadata, 2, destnode=coordinator)
        Framework.schedule(timers_to_process=0)
        return (a, pref_list)

    def test_get_put_get_put(self):
        """Show 2 x get-then-put operation"""
        dynamomessages._show_metadata = True
        (a, pref_list) = self.get_put_get_put()
        print(History.ladder(force_include=pref_list, spacing=16))
        dynamomessages._show_metadata = False

    def get_put_put(self, a, coordinator):
        # Assume .get_put_get_put() has happened already.
        # Send in a get-then-put-put.
        a.get('K1', destnode=coordinator)
        Framework.schedule(timers_to_process=0)
        getrsp = a.prev_msg
        a.put('K1', getrsp.metadata, 3, destnode=coordinator)
        Framework.schedule(timers_to_process=0)
        metadata = [a.prev_msg.metadata]  # PutRsp has a single VectorClock
        a.put('K1', metadata, 4, destnode=coordinator)
        Framework.schedule(timers_to_process=0)

    def test_get_put_put(self):
        """Show get-then-put-then-put operation"""
        dynamomessages._show_metadata = True
        (a, pref_list) = self.get_put_get_put()
        coordinator = pref_list[0]
        from_line = len(History.history)
        self.get_put_put(a, coordinator)
        print(History.ladder(force_include=pref_list, start_line=from_line, spacing=16))
        dynamomessages._show_metadata = False

    def test_metadata_simple_fail(self):
        """Show a vector clock not mattering on simple failures"""
        dynamomessages._show_metadata = True
        (a, pref_list) = self.get_put_get_put()
        coordinator = pref_list[0]
        self.get_put_put(a, coordinator)
        from_line = len(History.history)
        metadata = [a.prev_msg.metadata]  # PutRsp has a single VectorClock
        # Fail the coordinator
        coordinator.fail()
        # Send in another put
        a.put('K1', metadata, 11, destnode=pref_list[1])
        Framework.schedule(timers_to_process=0)
        # Send in a get
        a.get('K1', destnode=pref_list[1])
        Framework.schedule(timers_to_process=0)
        print(History.ladder(force_include=pref_list, start_line=from_line, spacing=16))
        dynamomessages._show_metadata = False

    def partition(self):
        """Show a network partition"""
        dynamomessages._show_metadata = True
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
        # Set in a get-then-put
        # Send in first get-then-put
        a.get('K1', destnode=coordinator)
        Framework.schedule(timers_to_process=0)
        getrsp = a.prev_msg
        a.put('K1', getrsp.metadata, 1, destnode=coordinator)
        Framework.schedule(timers_to_process=0)
        a_metadata = [a.prev_msg.metadata]  # PutRsp has a single VectorClock

        # Now partition the network: (b A B C) (D E F a)
        Framework.cut_wires((b, A, B, C), (D, E, F, a))
        Framework.cut_wires((D, E, F, a), (b, A, B, C))

        # Subsequent Put from a
        a.put('K1', a_metadata, 11, destnode=coordinator)
        Framework.schedule(timers_to_process=2)
        a_metadata = [a.prev_msg.metadata]  # PutRsp has a single VectorClock

        # Get-then-Put from b
        b.get('K1', destnode=coordinator)
        while b.prev_msg is None:  # Wait for rsp to arrive
            Framework.schedule(timers_to_process=1)
        getrsp = b.prev_msg
        b.put('K1', getrsp.metadata, 21, destnode=A)
        Framework.schedule(timers_to_process=3)
        return all_nodes

    def test_partition(self):
        dynamomessages._show_metadata = True
        all_nodes = self.partition()

        # Display, tweaking ordering of nodes so partition is in the middle
        print(History.ladder(force_include=all_nodes, spacing=16, key=lambda x: ' ' if x.name == 'b' else x.name))
        dynamomessages._show_metadata = False

    def partition_repair(self):
        # Repair the partition
        History.add("announce", "Repair network partition")
        Framework.cuts = []
        Framework.schedule(timers_to_process=12)

        # Get from node a
        a = BaseNode.node['a']
        a.get('K1')
        Framework.schedule(timers_to_process=0)

    def test_partition_detect(self):
        dynamomessages._show_metadata = True
        all_nodes = self.partition()
        from_line = len(History.history)
        self.partition_repair()

        # Display, tweaking ordering of nodes so partition is in the middle
        print(History.ladder(force_include=all_nodes, start_line=from_line, spacing=16, key=lambda x: ' ' if x.name == 'b' else x.name))
        dynamomessages._show_metadata = False

    def test_partition_restore(self):
        dynamomessages._show_metadata = True
        all_nodes = self.partition()
        self.partition_repair()
        from_line = len(History.history)

        # Put a new value, which coalesces
        a = BaseNode.node['a']
        getrsp = a.prev_msg
        a.put('K1', getrsp.metadata, 101)
        Framework.schedule(timers_to_process=0)

        # Display, tweaking ordering of nodes so partition is in the middle
        print(History.ladder(force_include=all_nodes, start_line=from_line, spacing=16, key=lambda x: ' ' if x.name == 'b' else x.name))
        dynamomessages._show_metadata = False

    def test_partition_detect_metadata(self):
        self.partition()
        self.partition_repair()
        # Just output the final diverged metadata
        a = BaseNode.node['a']
        getrsp = a.prev_msg
        print("%s@[%s]" % (getrsp.value, ",".join([str(x) for x in getrsp.metadata])))

    def test_partition_restore_metadata(self):
        self.partition()
        self.partition_repair()
        # Put a new value, which coalesces
        a = BaseNode.node['a']
        getrsp = a.prev_msg
        putmsg = a.put('K1', getrsp.metadata, 101)
        Framework.schedule(timers_to_process=0)
        print(putmsg.metadata)


if __name__ == "__main__":
    ii = 1
    while ii < len(sys.argv):
        arg = sys.argv[ii]
        if (arg == "-s" or arg == "--seed") and (ii + 1) < len(sys.argv):
            random.seed(sys.argv[ii + 1])
            del sys.argv[ii:ii + 2]
        elif arg == "-u" or arg == "--unicode":
            history.GLYPHS = history.UnicodeGlyphs
            del sys.argv[ii:ii + 1]
        else:
            ii += 1
    unittest.main()
