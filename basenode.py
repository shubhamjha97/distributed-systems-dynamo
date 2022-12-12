import logging
from history import History
from messages import InternalNodeMessage


_logger = logging.getLogger('dynamo')


class BaseNode(object):
    """Base class for a Dynamo node."""
    count = 0
    node = {}  # name  -> Node
    name = {}  # Node -> name

    @classmethod
    def reset(cls):
        cls.count = 0
        cls.node = {}
        cls.name = {}

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
            self.name = BaseNode.next_name()
        else:
            self.name = name
        self.next_sequence_number = 0
        self.included = True  # Whether this node is included in lists of nodes
        self.failed = False  # Indicates current failure
        # Keep track of node object <-> node name
        BaseNode.node[self.name] = self
        BaseNode.name[self] = self.name
        _logger.debug("Create node %s", self)
        History.add('add', InternalNodeMessage(self))

    def content_to_str(self):
        return []

    def __str__(self):
        return self.name

    def fail(self):
        """Mark this Node as currently failed; all messages to it will be dropped"""
        self.failed = True
        _logger.debug("Node %s fails", self)
        History.add('fail', InternalNodeMessage(self))

    def recover(self):
        """Mark this Node as not failed"""
        self.failed = False
        _logger.debug("Node %s recovers", self)
        History.add('recover', InternalNodeMessage(self))

    def remove(self):
        """Remove this Node from the system-wide lists of Nodes"""
        self.included = False
        _logger.debug("Node %s removed from system", self)
        History.add('remove', InternalNodeMessage(self))

    def restore(self):
        """Restore this Node to the system-wide lists of Nodes"""
        self.included = True
        _logger.debug("Node %s restored to system", self)
        History.add('add', InternalNodeMessage(self))

    def generate_sequence_number(self):
        """Generate next sequence number for this Node"""
        self.next_sequence_number = self.next_sequence_number + 1
        return self.next_sequence_number

    def process_msg(self, msg):
        raise NotImplemented

    def timer_pop(self, reason=None):
        raise NotImplemented
