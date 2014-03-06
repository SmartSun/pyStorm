import zmq
import logging
import threading as thread
from utils import send_msg

logger = logging.getLogger(__file__)


class BasicSpout(thread.Thread):
    _socket = None
    _id = None
    _component_name = None
    _tuple_count = 0
    _out_streams = []

    def next_tuple(self):
        """
        TODO: override this method with your own tuple generation code
        """
        raise NotImplementedError()

    def emit(self, data=None):
        send_msg(self._socket, self._out_streams, self._component_name, data)
        self._tuple_count += 1

    def init_socket(self, port):
        try:
            self._socket = zmq.Context().socket(zmq.PUB)
            self._socket.bind("tcp://127.0.0.1:%d" % port)
            return True
        except Exception as ex:
            logger.warn(str(ex))
            return False

    def run(self):
        while 1:
            self.next_tuple()
