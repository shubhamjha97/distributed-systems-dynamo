import copy
import random
import logging

import logconfig
from basenode import BaseNode
from timer import TimerManager
from emulation import Emulation
from hash_multiple import ConsistentHashTable
from dynamomessages import ClientPutReq, ClientGet, ClientPutRsp, ClientGetRsp
from dynamomessages import PutReq, GetReq, PutResp, GetRsp
from dynamomessages import DynamoRequestMessage
from dynamomessages import PingReq, PingRsp
from merkle import MerkleTree
from vectorclock import VectorClock

logconfig.init_logging()
_logger = logging.getLogger('dynamo')


class Node(BaseNode):
    timer_priority = 20
    T = 10  # Number of repeats for nodes in consistent hash table
    N = 3  # Number of nodes to replicate at
    W = 2  # Number of nodes that need to reply to a write operation
    R = 2  # Number of nodes that need to reply to a read operation
    node_list = []
    consistent_hash_tbl = ConsistentHashTable(node_list, T)

    def __init__(self):
        super(Node, self).__init__()
        self.local_store = MerkleTree()

        self.pending_put_msg = {}
        self.pending_put_rsp = {}
        self.pending_get_msg = {}
        self.pending_get_rsp = {}

        self.pending_req = {PutReq: {}, GetReq: {}}
        self.failed_nodes = []
        self.pending_handoffs = {}

        Node.node_list.append(self)
        Node.consistent_hash_tbl = ConsistentHashTable(Node.node_list, Node.T)

        self.retry_failed_node("retry")

    def put(self, key, value, metadata):
        self.local_store[key] = (value, metadata)

    def get(self, key):
        if key in self.local_store:
            return self.local_store[key]

        return (None, None)

    @classmethod
    def reset(cls):
        cls.node_list = []
        cls.consistent_hash_tbl = ConsistentHashTable(cls.node_list, cls.T)

    def retry_failed_node(self, _):
        if self.failed_nodes:
            node = self.failed_nodes.pop(0)
            pingmsg = PingReq(self, node)
            Emulation.send_message(pingmsg)
        TimerManager.start_timer(self, reason="retry", priority=15, callback=self.retry_failed_node)

    def process_PingReq(self, pingmsg):
        pingrsp = PingRsp(pingmsg)
        Emulation.send_message(pingrsp)

    def process_PingResp(self, pingmsg):
        recovered_node = pingmsg.from_node
        while recovered_node in self.failed_nodes:
            self.failed_nodes.remove(recovered_node)
        if recovered_node in self.pending_handoffs:
            for key in self.pending_handoffs[recovered_node]:
                (value, metadata) = self.get(key)
                putmsg = PutReq(self, recovered_node, key, value, metadata)
                Emulation.send_message(putmsg)
            del self.pending_handoffs[recovered_node]

    def rsp_timer_pop(self, reqmsg):
        _logger.info("Node %s now treating node %s as failed", self, reqmsg.to_node)
        self.failed_nodes.append(reqmsg.to_node)
        failed_requests = Emulation.cancel_timers_to(reqmsg.to_node)
        failed_requests.append(reqmsg)
        for failedmsg in failed_requests:
            self.retry_request(failedmsg)

    def retry_request(self, reqmsg):
        if not isinstance(reqmsg, DynamoRequestMessage):
            return

        preference_list = Node.consistent_hash_tbl.find_nodes(reqmsg.key, Node.N, self.failed_nodes)[0]
        kls = reqmsg.__class__

        if kls in self.pending_req and reqmsg.msg_id in self.pending_req[kls]:
            for node in preference_list:
                if node not in [req.to_node for req in self.pending_req[kls][reqmsg.msg_id]]:
                    newreqmsg = copy.copy(reqmsg)
                    newreqmsg.to_node = node
                    self.pending_req[kls][reqmsg.msg_id].add(newreqmsg)
                    Emulation.send_message(newreqmsg)

    def process_ClientPutReq(self, msg):
        preference_list, avoided = Node.consistent_hash_tbl.find_nodes(msg.key, Node.N, self.failed_nodes)
        avoided = avoided[:Node.N]
        non_extra_count = Node.N - len(avoided)
        if self not in preference_list:
            _logger.info("put(%s=%s) maps to %s", msg.key, msg.value, preference_list)
            coordinator = preference_list[0]
            Emulation.forward_message(msg, coordinator)
        else:
            seqno = self.generate_sequence_number()
            _logger.info("%s, %d: put %s=%s", self, seqno, msg.key, msg.value)
            metadata = copy.deepcopy(msg.metadata)
            metadata.update(self.name, seqno)
            self.pending_req[PutReq][seqno] = set()
            self.pending_put_rsp[seqno] = set()
            self.pending_put_msg[seqno] = msg
            reqcount = 0
            for ii, node in enumerate(preference_list):
                if ii >= non_extra_count:
                    handoff = avoided
                else:
                    handoff = None
                putmsg = PutReq(self, node, msg.key, msg.value, metadata, msg_id=seqno, handoff=handoff)
                self.pending_req[PutReq][seqno].add(putmsg)
                Emulation.send_message(putmsg)
                reqcount = reqcount + 1
                if reqcount >= Node.N:
                    break

    def process_ClientGetReq(self, msg):
        preference_list = Node.consistent_hash_tbl.find_nodes(msg.key, Node.N, self.failed_nodes)[0]
        if self not in preference_list:
            _logger.info("get(%s=?) maps to %s", msg.key, preference_list)
            coordinator = preference_list[0]
            Emulation.forward_message(msg, coordinator)
        else:
            seqno = self.generate_sequence_number()
            self.pending_req[GetReq][seqno] = set()
            self.pending_get_rsp[seqno] = set()
            self.pending_get_msg[seqno] = msg
            reqcount = 0
            for node in preference_list:
                getmsg = GetReq(self, node, msg.key, msg_id=seqno)
                self.pending_req[GetReq][seqno].add(getmsg)
                Emulation.send_message(getmsg)
                reqcount = reqcount + 1
                if reqcount >= Node.N:
                    break

    def process_PutReq(self, putmsg):
        _logger.info("%s: store %s=%s", self, putmsg.key, putmsg.value)
        self.put(putmsg.key, putmsg.value, putmsg.metadata)
        if putmsg.handoff is not None:
            for failed_node in putmsg.handoff:
                self.failed_nodes.append(failed_node)
                if failed_node not in self.pending_handoffs:
                    self.pending_handoffs[failed_node] = set()
                self.pending_handoffs[failed_node].add(putmsg.key)
        putrsp = PutResp(putmsg)
        Emulation.send_message(putrsp)

    def process_PutResp(self, putrsp):
        seqno = putrsp.msg_id
        if seqno in self.pending_put_rsp:
            self.pending_put_rsp[seqno].add(putrsp.from_node)
            if len(self.pending_put_rsp[seqno]) >= Node.W:
                _logger.info("%s: written %d copies of %s=%s so done", self, Node.W, putrsp.key, putrsp.value)
                _logger.debug("  copies at %s", [node.name for node in self.pending_put_rsp[seqno]])
                original_msg = self.pending_put_msg[seqno]
                del self.pending_req[PutReq][seqno]
                del self.pending_put_rsp[seqno]
                del self.pending_put_msg[seqno]
                client_putrsp = ClientPutRsp(original_msg, putrsp.metadata)
                Emulation.send_message(client_putrsp)
        else:
            pass

    def process_GetReq(self, getmsg):
        _logger.info("%s: retrieve %s=?", self, getmsg.key)
        (value, metadata) = self.get(getmsg.key)
        getrsp = GetRsp(getmsg, value, metadata)
        Emulation.send_message(getrsp)

    def process_GetResp(self, getrsp):
        seqno = getrsp.msg_id
        if seqno in self.pending_get_rsp:
            self.pending_get_rsp[seqno].add((getrsp.from_node, getrsp.value, getrsp.metadata))
            if len(self.pending_get_rsp[seqno]) >= Node.R:
                _logger.info("%s: read %d copies of %s=? so done", self, Node.R, getrsp.key)
                _logger.debug("  copies at %s", [(node.name, value) for (node, value, _) in self.pending_get_rsp[seqno]])

                results = VectorClock.coalesce2([(value, metadata) for (node, value, metadata) in self.pending_get_rsp[seqno]])

                original_msg = self.pending_get_msg[seqno]
                del self.pending_req[GetReq][seqno]
                del self.pending_get_rsp[seqno]
                del self.pending_get_msg[seqno]

                client_getrsp = ClientGetRsp(original_msg,
                                             [value for (value, metadata) in results],
                                             [metadata for (value, metadata) in results])
                Emulation.send_message(client_getrsp)
        else:
            pass

    def process_msg(self, msg):
        if isinstance(msg, ClientPutReq):
            self.process_ClientPutReq(msg)
        elif isinstance(msg, PutReq):
            self.process_PutReq(msg)
        elif isinstance(msg, PutResp):
            self.process_PutResp(msg)
        elif isinstance(msg, ClientGet):
            self.process_ClientGetReq(msg)
        elif isinstance(msg, GetReq):
            self.process_GetReq(msg)
        elif isinstance(msg, GetRsp):
            self.process_GetResp(msg)
        elif isinstance(msg, PingReq):
            self.process_PingReq(msg)
        elif isinstance(msg, PingRsp):
            self.process_PingResp(msg)
        else:
            raise TypeError("Unexpected message type %s", msg.__class__)

    def content_to_str(self):
        results = []
        for key, value in self.local_store.items():
            results.append("%s:%s" % (key, value[0]))
        return results


class Client(BaseNode):
    timer_priority = 17

    def __init__(self, name=None):
        super(Client, self).__init__(name)
        self.prev_msg = None

    def put(self, key, metadata, value, destnode=None):
        if destnode is None:
            destnode = random.choice(Node.node_list)

        if not metadata or (len(metadata) == 1 and metadata[0] is None):
            metadata = VectorClock()
        else:
            metadata = VectorClock.converge(metadata)
        putmsg = ClientPutReq(self, destnode, key, value, metadata)
        Emulation.send_message(putmsg)
        return putmsg

    def get(self, key, destnode=None):
        if destnode is None:
            destnode = random.choice(Node.node_list)
        getmsg = ClientGet(self, destnode, key)
        Emulation.send_message(getmsg)
        return getmsg

    def rsp_timer_pop(self, reqmsg):
        if isinstance(reqmsg, ClientPutReq):
            _logger.info("Put request timed out; retrying")
            self.put(reqmsg.key, [reqmsg.metadata], reqmsg.value)
        elif isinstance(reqmsg, ClientGet):
            _logger.info("Get request timed out; retrying")
            self.get(reqmsg.key)

    def process_msg(self, msg):
        self.prev_msg = msg
