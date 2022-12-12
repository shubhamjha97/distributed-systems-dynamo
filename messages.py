class BaseMessage(object):
    def __init__(self, from_node, to_node, msg_id=None):
        self.from_node = from_node
        self.to_node = to_node
        self.msg_id = msg_id

    def __str__(self):
        return self.__class__.__name__


class ResponseMessage(BaseMessage):
    def __init__(self, req):
        super(ResponseMessage, self).__init__(req.to_node, req.from_node, msg_id=req.msg_id)
        self.response_to = req


class InternalNodeMessage(BaseMessage):
    def __init__(self, node):
        super(InternalNodeMessage, self).__init__(node, node)


class TimerMessage(BaseMessage):
    def __init__(self, node, reason, callback=None):
        super(TimerMessage, self).__init__(node, node)
        self.reason = reason
        self.callback = callback


class DynamoRequestMessage(BaseMessage):
    def __init__(self, from_node, to_node, key, msg_id=None):
        super(DynamoRequestMessage, self).__init__(from_node, to_node, msg_id=msg_id)
        self.key = key

    def __str__(self):
        return "%s(%s=?)" % (self.__class__.__name__, self.key)


class DynamoResponseMessage(ResponseMessage):
    def __init__(self, req, value, metadata):
        super(DynamoResponseMessage, self).__init__(req)
        self.key = req.key
        self.value = value
        self.metadata = metadata

    def __str__(self):
        return "%s(%s=%s)" % (self.__class__.__name__, self.key, _show_value(self.value, self.metadata))


class ClientPutRequestMessage(DynamoRequestMessage):
    def __init__(self, from_node, to_node, key, value, metadata, msg_id=None):
        super(ClientPutRequestMessage, self).__init__(from_node, to_node, key, msg_id=msg_id)
        self.value = value
        self.metadata = metadata

    def __str__(self):
        return "ClientPut(%s=%s)" % (self.key, _show_value(self.value, self.metadata))


class ClientPutResponseMessage(DynamoResponseMessage):
    def __init__(self, req, metadata=None):
        if metadata is None:
            metadata = req.metadata
        super(ClientPutResponseMessage, self).__init__(req, req.value, metadata)


class PutRequestMessage(DynamoRequestMessage):
    def __init__(self, from_node, to_node, key, value, metadata, msg_id=None, handoff=None):
        super(PutRequestMessage, self).__init__(from_node, to_node, key, msg_id)
        self.value = value
        self.metadata = metadata
        self.handoff = handoff

    def __str__(self):
        if self.handoff is None:
            return "PutReq(%s=%s)" % (self.key, _show_value(self.value, self.metadata))
        else:
            return ("PutReq(%s=%s, handoff=(%s))" %
                    (self.key,
                     _show_value(self.value, self.metadata),
                     ",".join([str(x) for x in self.handoff])))


class PutResponseMessage(DynamoResponseMessage):
    def __init__(self, req):
        super(PutResponseMessage, self).__init__(req, req.value, req.metadata)


class ClientGetRequestMessage(DynamoRequestMessage):
    pass


class ClientGetResponseMessage(DynamoResponseMessage):
    pass


class GetRequestMessage(DynamoRequestMessage):
    pass


class GetResponseMessage(DynamoResponseMessage):
    pass


class PingRequestMessage(BaseMessage):
    pass


class PingResponseMessage(ResponseMessage):
    pass


_show_metadata = False


def _show_value(value, metadata):
    if _show_metadata:
        try:
            return "%s@[%s]" % (value, ",".join([str(x) for x in metadata]))
        except TypeError:
            return "%s@%s" % (value, metadata)
    else:
        return "%s" % value
