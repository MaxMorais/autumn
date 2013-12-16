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

from autumn.db.engine import Engine
import sqlite3

class SqliteEngine(Engine):
	def __init__(self, **kwargs):
		self.path = kwargs.pop('path')
		assert self.path is not None

		super(Engine, self).__init__()

	def connect(self, **kwargs):
		if not self.connected :
			self.connected = False
			try:
				self.connection = sqlite3(self.path)
			except Exception, e:
				raise e
			self.connected = True
		return self.connected

	def disconnect(self):
		if self.connected:
			self.connection.close()
			self.connection = None
			self.connected = False
		return True

	def _load_column(self, schema, column):