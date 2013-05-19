import zmq
import os, sys
import logging
import threading as thread

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

logger = logging.getLogger(__file__)


class basic_spout(thread.Thread):
	_socket = None
	id = None
	_tuple_count = 0

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
		self._socket.send(data)
		self._tuple_count += 1

	def init_socket(self, type, port):
		try:
			self._socket = zmq.Context().socket(type)
			self._socket.bind("tcp://127.0.0.1:%s" % port)
			return True
		except Exception as ex:
			logger.warn(str(ex))
			return False

	def register_zk_node(self):
		pass

	def run(self):
		while 1:
			self.next_tuple()

