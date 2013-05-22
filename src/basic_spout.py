import zmq
import os, sys
import logging
import threading as thread

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from utils import send_msg

logger = logging.getLogger(__file__)


class basic_spout(thread.Thread):
	_socket = None
	_id = None
	_component_name = None
	_tuple_count = 0
	_streams = {}

	def __init__(self):
		"""
        TODO: override this method with your own initial code
        """
		pass

	def next_tuple(self):
		"""
        TODO: override this method with your own tuple generation code
        """
		pass

	def emit(self, data=None):
		send_msg(self._socket, self._streams, self._component_name, data)
		self._tuple_count += 1

	def init_socket(self, port):
		try:
			self._socket = zmq.Context().socket(zmq.PUB)
			self._socket.bind("tcp://127.0.0.1:%d" % port)
			return True
		except Exception as ex:
			logger.warn(str(ex))
			return False

	def register_zk_node(self):
		pass

	def run(self):
		while 1:
			self.next_tuple()

