import copy
import logging
from collections import deque

from basenode import BaseNode
from history import History
from timer import TimerManager
from messages import ResponseMessage
import logconfig

logconfig.init_logging()
_logger = logging.getLogger('dynamo')


class Emulation(object):
    unreachable_nodes = []
    pending_msg_queue = deque([])
    pending_timers = {}

    @classmethod
    def reset(cls):
        cls.unreachable_nodes = []
        cls.pending_msg_queue = deque([])
        cls.pending_timers = {}

    @classmethod
    def disconnect(cls, from_nodes, to_nodes):
        History.add("announce", "Cut %s -> %s" % ([str(x) for x in from_nodes], [str(x) for x in to_nodes]))
        cls.unreachable_nodes.append((from_nodes, to_nodes))

    @classmethod
    def is_reachable(cls, from_node, to_node):
        for (from_nodes, to_nodes) in cls.unreachable_nodes:
            if from_node in from_nodes and to_node in to_nodes:
                return False
        return True

    @classmethod
    def send_message(cls, msg, expect_reply=True):
        _logger.info("Enqueue %s->%s: %s", msg.from_node, msg.to_node, msg)
        cls.pending_msg_queue.append(msg)
        History.add("send", msg)
        if (expect_reply and
            not isinstance(msg, ResponseMessage) and
            'rsp_timer_pop' in msg.from_node.__class__.__dict__ and
            callable(msg.from_node.__class__.__dict__['rsp_timer_pop'])):
            cls.pending_timers[msg] = TimerManager.start_timer(msg.from_node, reason=msg, callback=Emulation.rsp_timer_pop)

    @classmethod
    def cancel_req_timer(cls, timer_msg):
        if timer_msg in cls.pending_timers:
            TimerManager.cancel_timer(cls.pending_timers[timer_msg])
            del cls.pending_timers[timer_msg]

    @classmethod
    def cancel_timers_for_node(cls, dest):
        failed_requests = []
        pending_timers_keys = list(cls.pending_timers.keys())
        for timer_msg in pending_timers_keys:
            if timer_msg.to_node == dest:
                TimerManager.cancel_timer(cls.pending_timers[timer_msg])
                del cls.pending_timers[timer_msg]
                failed_requests.append(timer_msg)
        return failed_requests

    @classmethod
    def rsp_timer_pop(cls, timer_msg):
        del cls.pending_timers[timer_msg]
        _logger.debug("Call on to rsp_timer_pop() for node %s" % timer_msg.from_node)
        timer_msg.from_node.rsp_timer_pop(timer_msg)

    @classmethod
    def forward_message(cls, msg, to_node):
        _logger.info("Enqueue(fwd) %s->%s: %s", msg.to_node, to_node, msg)
        fwd_msg = copy.copy(msg)
        fwd_msg.intermediate_node = fwd_msg.to_node
        fwd_msg.original_msg = msg
        fwd_msg.to_node = to_node
        cls.pending_msg_queue.append(fwd_msg)
        History.add("forward", fwd_msg)

    @classmethod
    def run(cls, msgs_to_process=None, timers_to_process=None):
        if msgs_to_process is None:
            msgs_to_process = 32768
        if timers_to_process is None:
            timers_to_process = 32768

        while cls._msgs_remaining():
            _logger.info("Start of schedule: %d (limit %d) pending messages, %d (limit %d) pending timers",
                         len(cls.pending_msg_queue), msgs_to_process, TimerManager.pending_count(), timers_to_process)
            while cls.pending_msg_queue:
                msg = cls.pending_msg_queue.popleft()
                if msg.to_node.failed:
                    _logger.info("Drop %s->%s: %s as destination down", msg.from_node, msg.to_node, msg)
                    History.add("drop", msg)
                elif not Emulation.is_reachable(msg.from_node, msg.to_node):
                    _logger.info("Drop %s->%s: %s as route down", msg.from_node, msg.to_node, msg)
                    History.add("cut", msg)
                else:
                    _logger.info("Dequeue %s->%s: %s", msg.from_node, msg.to_node, msg)
                    if isinstance(msg, ResponseMessage):
                        try:
                            reqmsg = msg.response_to.original_msg
                        except Exception:
                            reqmsg = msg.response_to
                        cls.cancel_req_timer(reqmsg)
                    History.add("deliver", msg)
                    msg.to_node.process_msg(msg)
                msgs_to_process = msgs_to_process - 1
                if msgs_to_process == 0:
                    return

            if TimerManager.pending_count() > 0 and timers_to_process > 0:
                TimerManager.pop_timer()
                timers_to_process = timers_to_process - 1
            if timers_to_process == 0:
                return

    @classmethod
    def _msgs_remaining(cls):
        if cls.pending_msg_queue or TimerManager.pending_count() > 0:
            return True
        return False


def reset():
    Emulation.reset()
    TimerManager.reset()
    History.reset()


def reset_all():
    reset()
    BaseNode.reset()
