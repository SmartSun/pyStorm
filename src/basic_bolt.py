import zmq
import os, sys
import logging
import threading as thread

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from utils import send_msg

logger = logging.getLogger(__file__)

class basic_bolt(thread.Thread):
    _in_sockets = {}
    _out_socket = None
    _out_streams = []
    _id = None
    _component_name = None
    _poller = zmq.Poller()
    _in_tuple_count = 0
    _out_tuple_count = 0

    def __init__(self):
        """
        TODO: override this method with your own initial code
        """
        pass

    def process(self, data=None, component_name=None):
        """
        TODO: override this method with your own process code
        """
        pass

    def emit(self, data=None):
        send_msg(self._socket, self._out_streams, self._component_name, data)
        self._out_tuple_count += 1


    def init_in_sockets(self, stream_id, servers, id):
        try:
            socket = zmq.Context().socket(zmq.SUB)
            for s in servers:
                socket.connect("tcp://%s" % s)
            topic = '%s_%d' % (stream_id, id)
            socket.setsockopt(zmq.SUBSCRIBE, topic)
            self._in_sockets[stream_id] = socket
            self._poller.register(socket, zmq.POLLIN)
            return True
        except Exception as ex:
            logger.warn(str(ex))
            return False

    def init_out_socket(self, port):
        try:
            self._out_socket = zmq.Context().socket(zmq.PUB)
            self._out_socket.bind("tcp://127.0.0.1:%d" % port)
            return True
        except Exception as ex:
            logger.warn(str(ex))
            return False

    def add_server(self, stream_id, server):
        try:
            self._in_sockets[stream_id].connect("tcp://%s" % server)
            return True
        except Exception as ex:
            logger.warn(str(ex))
            return False

    def del_server(self, stream_id, server):
        try:
            self._in_sockets[stream_id].disconnect("tcp://%s" % server)
            return True
        except Exception as ex:
            logger.warn(str(ex))
            return False

    def set_subscrption(self, stream_id, id):
        topic = '%s_%d' % (stream_id, id)
        self._in_sockets[stream_id].setsockopt(zmq.SUBSCRIBE, topic)

    def register_zk_node(self):
        pass

    def run(self):
        while 1:
            socks = dict(self._poller.poll())
            for s in self._in_sockets.values():
                if socks.get(s) == zmq.POLLIN:
                    msg_id, component_name, data = s.recv_json()
                    self.process(data, component_name)
                    self._in_tuple_count += 1