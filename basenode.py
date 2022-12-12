import logging
from history import History
from messages import InternalNodeMessage


_logger = logging.getLogger('dynamo')


class BaseNode(object):
    count = 0
    name_to_node = {}
    node_to_name = {}

    @classmethod
    def reset(cls):
        cls.count = 0
        cls.name_to_node = {}
        cls.node_to_name = {}

    @classmethod
    def next_name(cls):
        if cls.count < 26:
            name = chr(ord('A') + cls.count)
        elif cls.count < (26 * 26):
            hi = cls.count / 26
            lo = cls.count % 26
            name = chr(ord('A') + hi - 1) + chr(ord('A') + lo)
        else:
            raise NotImplemented
        cls.count = cls.count + 1
        return name

    def __init__(self, name=None):
        if name is None:
            self.node_to_name = BaseNode.next_name()
        else:
            self.node_to_name = name
        self.next_sequence_number = 0
        self.included = True
        self.failed = False

        BaseNode.name_to_node[self.node_to_name] = self
        BaseNode.node_to_name[self] = self.node_to_name
        _logger.debug("Create node %s", self)
        History.add('add', InternalNodeMessage(self))

    def content_to_str(self):
        return []

    def __str__(self):
        return self.node_to_name

    def fail(self):
        self.failed = True
        _logger.debug("Node %s fails", self)
        History.add('fail', InternalNodeMessage(self))

    def recover(self):
        self.failed = False
        _logger.debug("Node %s recovers", self)
        History.add('recover', InternalNodeMessage(self))

    def remove(self):
        self.included = False
        _logger.debug("Node %s removed from system", self)
        History.add('remove', InternalNodeMessage(self))

    def restore(self):
        self.included = True
        _logger.debug("Node %s restored to system", self)
        History.add('add', InternalNodeMessage(self))

    def get_next_sequence_number(self):
        self.next_sequence_number = self.next_sequence_number + 1
        return self.next_sequence_number

    def process_msg(self, msg):
        raise NotImplemented

    def timer_pop(self, reason=None):
        raise NotImplemented
