#!/usr/bin/python
#-*- coding: utf-8 -*-

__author__ = "Maxwell Morais github.com/MaxMorais"

###############################################################################
# 																			  #
# Copyright (C) 2013 Maxwell Morais (i.am.maxy)								  #
# 																			  #
# This program is free software: you can redistribute it and/or modify        #
# it under the terms of the GNU Affero General Public License as published by #
# the Free Software Foundation, either version 3 of the License, or 		  #
# (at your option) any later version. 										  #
# 																			  #
# This program is distributed in the hope that it will be useful, 			  #
# but WITHOUT ANY WARRANTY; without even the implied warranty of 			  #
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the 				  #
# GNU Affero General Public License for more details. 						  #
# 																			  #
# You should have received a copy of the GNU Affero General Public License    #
# along with this program. If not, see <http://www.gnu.org/licenses/>.        #
###############################################################################

import types
import datetime

from autumn.util import TODAY, NOW, TIME, DEFAULT
from autumn.util.signals import WithSignals
from autumn.db.connection import  autumn_db as db
from autumn.db.column import Column

class SchemaBase(type):
	def __new__(cls, cls_name, bases, attrs):
		
		fields = {}
		attributes = {}

		for attr, value in attrs.iteritems():
			if callable(value)
				or str(value).startswith('__'):
				attributes[attr] = value
			elif isinstance(value, (tuple, Column)):
				fields[attr] = value

		attributes.update({
			'tablename': '',
			'fields': None,
			'indexes': None
		})

		new_class = type.__new__(cls, cls_name, bases, attributes)

		@new_class.__init__.after
		def after__init__(signal):
			for attr, value, in fields.iteritems():
				setattr(signal.instance, attr, value)

		return new_class

class Schema(WithSignals):
	__metaclass__ = SchemaBase
	__handlers__ = ('__init__', )
	
	def __init__(self, table, engine=None, **fields):
		self.tablename = table
		self.engine = engine
		self.fields = {}
		self.relations = {}

		self._id = long, True, dict(pk=True)
		self._created = datetime.datetime, True, dict(default=TODAY())
		self._updated = datetime.datetime, True, dict(default=TODAY())

		self.index_updated = datetime.datetime, True

		for fieldname, field_val in fields.iteritems():
			setattr(self, fieldname, field_val)

	def __str__(self): return str(self.table)
	def __unicode__(self): return unicode(self.table)

	def __setattr__(self, name, val):
		if name in self.__dict__ or \
			name in self.__class__.__dict__ or \
			name.startswith('__'):
			return object.__setattr__(self, name, val)

		if name.startswith('_'):
			name_bits = name.split('_', 1)
		else:
			name_bits = [name]

		is_field = True

		index_name = name_bits[1] if len(name_bits) > 1 else u''
		index_types = {
			'index': {},
			'unique': {'unique': True}
		}

		if index_name in index_types:
			is_field = False
			if isinstance(val, (types.DictType, types.StringTypes)):
				val = (val, )

			self.set_index(index_name, val, **index_types[index_name])

		if is_field:
			if isinstance(val, types.TypeType):
				val = (val, )

			self.set_field(name, *val)

	def __getattr__(self, name):
		if not name in self.fields:
			raise AttributeError("{} is not a valid field name".format(name))

		return self.fields[name]['name']

	@property
	def pk(self):
		ret = None
		for  field_name, field_options in self.fields.iteritems():
			if field_options.get('pk', False):
				ret = field_name
				break
		if ret is None:
			raise AttributeError('no primary key in schema')

		return ret

	@property
	def common_fields(self):
		return {f:v for f,v in self.fields.iteritems() if not f.startswith('_')}

	@property
	def required_fields(self):
		return {f:v for f,v in self.common_fields.iteritems() if v['required']}

	@property
	def automatic_fields(self):
		return {f:v for f,v in self.fields.iteritems() if f.startswith('_')}

	@property
	def default_values(self):
		return {f:v for f,v in self.fields.iteritems() if not (f.get('default', DEFAULT) is DEFAULT)}

	def set_field(self, field_name, field_type, required=False, options=None, **options_kwargs):
		if not field_name:
			raise ValueError('fieldname is empty')

		if not isinstance(field_type, types.TypeType):
			raise ValueError("field_type is not a valid python built-in type.")

		if field_name in self.fields:
			print self.fields
			raise ValueError('{} already exists and cannot be changed'.format(field_name))

		if not options:
			options = {}
		options.update(options_kwargs)

		d = {
			'name': field_name,
			'type': field_type,
			'required': required
		}

		min_size = options.pop('min_size', None)
		max_size = options.pop('max_size', None)
		size = options.pop('size', None)

		if size > 0:
			d['size'] = size
		else:
			if min_size > 0 and max_size == None:
				raise ValueError('min_size option was set without max_size corresponding')

			elif min_size is None and max_size > 0:
				d['max_size'] = max_size

			elif min_size >=0 and max_size >= 0:
				d['min_size'] = min_size
				d['max_size'] = max_size

		unique = options.pop('unique', False)
		if unique:
			self.set_index(field_name, [field_name], unique=unique)

		d.update(options)
		self.fields[field_name] = d

		return self

	def set_index(self, index_name, index_fields, **options):
		if not index_fields:
			raise ValueError("index_fields list is empty")
		if index_name in self.indexes:
			raise ValueError('index_name has already been defined on {}'.format(
				str(self.indexes[index_name]['fields'])
			) )

		if not index_name:
			index_name = u'_'.join(field_names)

		self.indexes[index_name] = {
			'name': index_name,
			'fields': field_names,
			'unique': False
		}
		self.indexes[index_name].update(options)

		return self

	def field_name(self, k):
		if k == 'pk': k = self.pk
		if k not in self.fields:
			raise KeyError("key {} is not in {} schema".format(k, self.table))
		return k

	def load_column(self, column):
		return self.engine.load_column(self, column)


class TableSchema(Schema):
	def __init__(self, table):
		super(DbTableSchema, self).__init__(
			table, 
			**self.describe_schema(table)
		)

	@staticmethod
	def describe_schema(tablename):
		if hasattr(db, 'describe_schema'):
			raw_data = db.describe_schema(tablename)

class StoredSchema(Schema):
	def __init__(self, schema):
		super(StoredSchema, self).__init__(
			table, 
			**self.describe_schema(schema)
		)

	@staticmethod
	def describre_schema(schemaname):
		pass
