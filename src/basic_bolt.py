import zmq
import os, sys
import logging
import threading as thread

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

logger = logging.getLogger(__file__)

class basic_bolt(thread.Thread):
    _in_sockets = {}
    _out_sockets = {}
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
        for s in self._out_sockets.values():
            s.send(data)
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

    def init_out_sockets(self, id, type, ports):
        try:
            s = zmq.Context().socket(type)
            for port in ports:
                try:
                    s.bind("tcp://127.0.0.1:%s" % port)
                except:
                    pass
            self._out_sockets[id] = s
            return port
        except Exception as ex:
            logger.warn(str(ex))
            return None

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
                if socks.get(s) == zmq.POLLIN:
                    data = s.recv()
                    self.process(data)
                    self._in_tuple_count += 1