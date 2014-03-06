import logging
import uuid
import random
from constants import GroupingType
#from conf import site

logger = logging.getLogger(__file__)


class Stream():

    def __init__(self, from_component, to_component, grouping_type):
        self.from_component = from_component
        self.to_component = to_component
        self.grouping_type = grouping_type
        self.grouping_fields = []
        self.id = str(uuid.uuid1())


class Component():

    def __init__(self, name, component_type,
                 component_class, workers=1, fields=None):
        self.name = name
        self.fields = fields
        self.workers = workers
        self.component_class = component_class
        self.servers = []
        self.type = component_type


class Topology():
    _current_bolt_name = None
    _streams = []
    _components = []
    _server_graph = {}
    _current_bolt = None

    def __init__(self):
        self._id = str(uuid.uuid1())

    def _get_component_by_name(self, component_name):
        for component in self._components:
            if component.name == component_name:
                return component
        return None

    def set_spout(self, spout_name, spout_class, workers, fields=None):
        spout = Component(spout_name, 'spout', spout_class, workers, fields)
        self._components.append(spout)

    def set_bolt(self, bolt_name, bolt_class, workers, fields=None):
        bolt = Component(bolt_name, 'bolt', bolt_class, workers, fields)
        self._current_bolt = bolt
        self._components.append(bolt)
        return self

    def shuffle_grouping(self, component_name):
        component = self._get_component_by_name(component_name)
        self._streams.append(Stream(component,
                                    self._current_bolt,
                                    GroupingType.SHUFFLE))
        return self

    def all_grouping(self, component_name):
        component = self._get_component_by_name(component_name)
        self._streams.append(Stream(component,
                                    self._current_bolt,
                                    GroupingType.ALL))
        return self

    def fields_grouping(self, component_name, fields):
        component = self._get_component_by_name(component_name)
        fg_stream = Stream(component, self._current_bolt, GroupingType.FIELDS)
        i = 0
        for f in component.fields:
            if f in fields:
                fg_stream.grouping_fields.append(i)
            i += 1
        self._streams.append(fg_stream)
        return self

    def _get_avail_workers(self):
        """
        {
            ip: [ports]
        }
        """
        return {}

    def _update_server_status_in_zk(self, servers):
        pass

    def _set_servers(self, component, workers, all_avail_workers):
        servers = random.sample(all_avail_workers, workers)
        for ip in servers:
            port = random.choice(all_avail_workers[ip])
            component.servers.append('%s:%d' % (ip, port))
            if ip not in self._server_graph:
                self._server_graph[ip] = {}
            self._server_graph[ip][port] = component
            all_avail_workers[ip].remove(port)
            if not all_avail_workers[ip]:
                del all_avail_workers[ip]

    def _send_class_file(self, ip):
        return True

    def _begin_server_in_zk(self, ip):
        return True

    def submit(self):
        all_avail_workers = self._get_avail_workers()
        servers_list = []
        for component in self._components:
            avail_list_length = sum([len(v) for v in all_avail_workers.values()])
            if component.workers > avail_list_length:
                logger.info('Not enough workers available.')
                return False
            remain_workers = component.workers
            while remain_workers > len(all_avail_workers):
                self._set_servers(component, len(all_avail_workers), all_avail_workers)
                remain_workers -= len(all_avail_workers)
            self._set_servers(component, remain_workers, all_avail_workers)
            servers_list.extend(component.servers)

        self._update_server_status_in_zk(servers_list)
        for ip in self._server_graph:
            if not self._send_class_file(ip):
                return False
            if not self._begin_server_in_zk(ip):
                return False
