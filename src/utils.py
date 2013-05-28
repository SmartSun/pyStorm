from constants import GROUPING_TYPE
import random
import hashlib

def send_msg(socket, streams, component_name, data):
	msg_id = ''
	for id in streams:
		if streams[id]['type'] == GROUPING_TYPE.ALL:
			msg_id = id
		elif streams[id]['type'] == GROUPING_TYPE.SHUFFLE:
			msg_id = '%s_%d' % (id, random.randint(1, streams[id]['count']))
		elif streams[id]['type'] == GROUPING_TYPE.FIELDS:
			fields = '_'.join([str(getattr(data, f)) for f in streams[id]['fields']])
			msg_id = '%s_%d' % (id, int(hashlib.md5(fields).hexdigest(), 16) % streams[id]['count'])
		socket.send_json([msg_id, component_name, data])