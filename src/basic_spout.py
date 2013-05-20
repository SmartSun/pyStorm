import zmq
import os, sys
import logging
import threading as thread

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

logger = logging.getLogger(__file__)


class basic_spout(thread.Thread):
	_sockets = {}
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
		for s in self._sockets.values():
			s.send(data)
		self._tuple_count += 1

	def init_socket(self, id, type, ports):
		try:
			s = zmq.Context().socket(type)
			for port in ports:
				try:
					s.bind("tcp://127.0.0.1:%s" % port)
					break
				except:
					pass
			self._sockets[id] = s
			return port
		except Exception as ex:
			logger.warn(str(ex))
			return None

	def register_zk_node(self):
		pass

	def run(self):
		while 1:
			self.next_tuple()

