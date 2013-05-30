import os,sys
import logging
import uuid

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from constants import GROUPING_TYPE
#from conf import site

logger = logging.getLogger(__file__)

class stream():
	from_component = None
	to_component = None
	grouping_type = None
	grouping_fields = []
	id = None

	def __init__(self, from_component,
				 to_component, grouping_type):
		self.from_component = from_component
		self.to_component = to_component
		self.grouping_type = grouping_type
		self.id = str(uuid.uuid1())

class component():
	name = None
	fields = None
	workers = None
	component_class = None

	def __init__(self, name, component_class, workers=1, fields=None, ):
		self.name = name
		self.fields = fields
		self.workers = workers
		self.component_class = component_class


class topology():
	_streams = []
	_components = []
	_current_bolt_name = None
	_id = None

	def __init__(self):
		self._id = str(uuid.uuid1())

	def _get_component_by_name(self, component_name):
		for component in self._components:
			if component.name == component_name:
				return component
		return None

	def set_spout(self, spout_name, spout_class, workers, fields=None):
		spout = component(spout_name, spout_class, workers, fields)
		self._components.append(spout)

	def set_bolt(self, bolt_name, bolt_class, workers, fields=None):
		bolt = component(bolt_name, bolt_class, workers, fields)
		self._current_bolt = bolt
		self._components.append(bolt)
		return self

	def shuffle_grouping(self, component_name):
		component = self._get_component_by_name(component_name)
		self._streams.append(stream(component,
									self._current_bolt,
									GROUPING_TYPE.SHUFFLE))
		return self

	def all_grouping(self, component_name):
		component = self._get_component_by_name(component_name)
		self._streams.append(stream(component,
									self._current_bolt,
									GROUPING_TYPE.ALL))
		return self

	def fields_grouping(self, component_name, fields):
		component = self._get_component_by_name(component_name)
		fg_stream = stream(component, self._current_bolt, GROUPING_TYPE.FIELDS)
		i = 0
		for f in component.fields:
			if f in fields:
				fg_stream.grouping_fields.append(i)
			i += 1
		self._streams.append(fg_stream)
		return self

	def submit(self):
		pass
		
	