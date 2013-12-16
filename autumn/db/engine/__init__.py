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

from autumn.util.plugins import Plugin
from itertools import imap

class EngineBase(type):
	def __new__(cls, name, bases, attrs):
		pass

class SQL(object):
	def __select__(self, *args):
		fmtd = self.__args__(*args)

		return "SELECT {0}".format(fmtd)

	def __args__(self, *args):
		try:
			args = list(imap(self.wrap, args))
		except NotImplementedError:
			args = list(args)
		return ", ".join(args)

	def __from__(self, *args):
		fmtd = ", ".join(list(args))
		return "FROM {0}".format(fmtd)

	__insert__ = "INSERT INTO {0} ({1})"
	__values__ = "VALUES ({0})"
	__update__ = "UPDATE {0}"
	__set__ = "SET {1}"
	__delete__ = "DELETE FROM {0}"
	__where__ = "WHERE {0}"
	__orderby__ = "ORDER BY {0} {1}"
	__join__ = "INNER JOIN {0}"
	__on__ = "ON {0}={1}"
	__groupby__ = "GROUP BY {0}"
	__having__ = "HAVING {}"
	__createtable__ = "CREATE TABLE IF NOT EXISTS {0} ({1})"
	__droptable__ = "DROP TABLE IF EXISTS {0}"

	class TYPES(object):
		__pk__ = "{0} INTEGER NOT NULL UNIQUE PRIMARY KEY AUTOINCREMENT"
		__str__ = "{0} TEXT {1}"
		__int__ = "{0} INTEGER {1}"
		__bool__ = "{0} INTEGER {1}"
		__fk__ = "{0} INTEGER {1}"
		__references__ = "FOREIGN KEY({0}) REFERENCES {1}({2})"

class BaseEngine(type):
	def __new__(cls, name, bases, attrs):
		if name == "Engine":
			attrs['enabled'] = False

		return type.__new__(cls, name, bases, attrs)

class Engine(Plugin, SQL)
	__metaclass__ = BaseEngine

	def __init__(self, *args, **kwargs):

		self.connection = None
		self.connected = False

		SQL.__init__(self)
		Plugin.__init__(self)

	def ensure(self):
		self.connect()

	def connect(self, *args, **kwargs):
		raise NotImplementedError

	def disconnect(self):
		raise NotImplementedError

	def sql(self, parts):
		formatted = []
		for tup in parts:
			k = tup[0]
			v = tup[1:]
			cmd = getattr(self, k)
			if isinstance(cmd, basestring):
				formatted.append(command.format(*v))
			else:
				formatted.append(cmd(*v))

			return " ".join(formatted)

	def wrap(self, value):
		raise NotImplementedError

	def _load_column(self, schema, column):
		raise NotImplementedError
