import zmq
import os, sys
import logging
import threading as thread

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

logger = logging.getLogger(__file__)

class basic_bolt(thread.Thread):
    _in_sockets = {}
    _out_socket = None
    id = None
    _poller = zmq.Poller()
    _in_tuple_count = 0
    _out_tuple_count = 0

    def __init__(self):
        """
        TODO: override this method with your own initial code
        """
        pass

    def process(self, data=None):
        """
        TODO: override this method with your own process code
        """
        pass

    def emit(self, data=None):
        self.out_socket.send(data)
        self._out_tuple_count += 1

    def init_in_sockets(self, id, type, servers):
        try:
            socket = zmq.Context().socket(type)
            for s in servers:
                socket.connect("tcp://%s" % s)
            self._in_sockets[id] = socket
            self._poller.register(socket, zmq.POLLIN)
            return True
        except Exception as ex:
            logger.warn(str(ex))
            return False

    def init_out_socket(self, type, port):
        try:
            self._out_socket = zmq.Context().socket(type)
            self._out_socket.bind("tcp://127.0.0.1:%s" % port)
            return True
        except Exception as ex:
            logger.warn(str(ex))
            return False

    def add_server(self, id, server):
        try:
            self._in_sockets[id].connect("tcp://%s" % server)
            return True
        except Exception as ex:
            logger.warn(str(ex))
            return False

    def del_server(self, id, server):
        try:
            self._in_sockets[id].disconnect("tcp://%s" % server)
            return True
        except Exception as ex:
            logger.warn(str(ex))
            return False

    def register_zk_node(self):
        pass

    def run(self):
        while 1:
            socks = dict(self._poller.poll())
            for s in self._in_sockets.values():
                if s in socks and socks[s] == zmq.POLLIN:
                    data = s.recv()
                    self.process(data)
                    self._in_tuple_count += 1