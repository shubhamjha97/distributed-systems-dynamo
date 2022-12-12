import logging

from messages import TimerMessage
from history import History

_logger = logging.getLogger('dynamo')

DEFAULT_PRIORITY = 10


def _priority(msg):
    priority = DEFAULT_PRIORITY
    node = msg.from_node
    if 'timer_priority' in node.__class__.__dict__:
        priority = int(node.__class__.__dict__['timer_priority'])
    return priority


class TimerManager(object):
    pending_timers = []

    @classmethod
    def pending_count(cls):
        return len(cls.pending_timers)

    @classmethod
    def reset(cls):
        cls.pending_timers = []

    @classmethod
    def start_timer(cls, node, reason=None, callback=None, priority=None):
        if node.failed:
            return None
        tmsg = TimerMessage(node, reason, callback=callback)
        History.add("start", tmsg)
        if priority is None:
            priority = _priority(tmsg)
        _logger.debug("Start timer %s prio %d for node %s reason %s", id(tmsg), priority, node, reason)
        for ii in range(len(cls.pending_timers)):
            if priority > cls.pending_timers[ii][0]:
                cls.pending_timers.insert(ii, (priority, tmsg))
                return tmsg
        cls.pending_timers.append((priority, tmsg))
        return tmsg

    @classmethod
    def cancel_timer(cls, tmsg):
        for (this_prio, this_tmsg) in cls.pending_timers:
            if this_tmsg == tmsg:
                _logger.debug("Cancel timer %s for node %s reason %s", id(tmsg), tmsg.from_node, tmsg.reason)
                cls.pending_timers.remove((this_prio, this_tmsg))
                History.add("cancel", tmsg)
                return

    @classmethod
    def pop_timer(cls):
        while True:
            (_, tmsg) = cls.pending_timers.pop(0)
            if tmsg.from_node.failed:
                continue
            _logger.debug("Pop timer %s for node %s reason %s", id(tmsg), tmsg.from_node, tmsg.reason)
            History.add("pop", tmsg)
            if tmsg.callback is None:
                tmsg.from_node.timer_pop(tmsg.reason)
            else:
                tmsg.callback(tmsg.reason)
            return
