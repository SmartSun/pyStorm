import random
import hashlib

from constants import GroupingType


def send_msg(socket, streams, component_name, data):
    msg_id = ''
    for s in streams:
        if s.grouping_type == GroupingType.ALL:
            msg_id = s.id
        elif s.grouping_type == GroupingType.SHUFFLE:
            msg_id = '%s_%d' % (
                s.id, random.randint(1, s.to_component.workers))
        elif s.grouping_type == GroupingType.FIELDS:
            fields = '_'.join([str(data[f]) for f in s.grouping_fields])
            msg_id = '%s_%d' % (
                s.id, int(hashlib.md5(fields).hexdigest(), 16) % s.to_component.workers)
        socket.send_json([msg_id, component_name, data])
