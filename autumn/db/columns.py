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

from autumn.db.query import Expr
from autumn.db.util import Cache

cache = Cache()

class Column(Expr):
	def __init__(self, name, datatype=None, reqd=None):
		self.reset()
		self.name = name
		if datatype is not None:
			self.datatype = datatype
		self.attr = None
		if reqd is not None:
			self.reqd = reqd

	def reset(self):
		self.no_value = object()
		self.name = None
		self.datatype = str
		self.reqd = False
		self.__model__ = None

	def __copy__(self):
		column = self.__class__.__new__(self.__class__)
		if 'name' in self.__dict__:
			column.name = self.name

		column.attr = self.attr
		column.__model__ = self.__model__
		if 'reqd' in self.__dict__:
			column.reqd = self.reqd

		return column

	def __set__(self, instance, value):
		self.set_model(instance)
		instance.__current__.add_modified(str(self))
		instance.__dict__[str(self)] = value

	def set_model(self, model):
		if self.__model__ is None:
			self.__model__ = model

	def set_from_db(self, instance, value):
		self.set_model(instance)
		if callable(self.datatype):
			value = self.datatype(self)
		instance.__dict__[str(self)] = value

	def sql(self):
		if not self.name:
			raise TypeError('column must have a name')
		if self.__model__ is not None:
			return '{0}."{1}"'.format(map(str, [self.__model__.__schema__, self.name]))
		return '"{0}"'.format(self.name)

	def args(self):
		return ()
