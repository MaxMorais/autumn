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

class Expression()

class Field(object):
	def __init__(self, schema):
		super(Field, self).__init__()
		self.__schema__ = schema

	@property
	def name(self):
		return self.__class__.__name__

	def __get__(self, obj, _type=None):
		if obj is None:
			return self
		else:
			return obj.__dict__.get(self.name, None)

	def __set__(self, obj, value):
		if obj is None:
			raise ValueError('You can\'t set a value for Model class, try a instance')
		else:
			obj.__dict__[self.name] = value

	def __eq__(self):

