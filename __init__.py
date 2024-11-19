import sys
import os
import time as TM
import tqdm
import contextlib
from .condition import *
from .database import DataBase
from pandas import Series, DataFrame
from functools import partial
from threading import Thread, Timer
import asyncio
import types
from typing import Union

if sys.version_info >= (3, 7) and sys.version_info < (3, 12):
	import imp
	def load_source(method, module_path):
		"""
		 Load a module and return its queryPY. This is a wrapper around imp. load_source that allows you to pass an absolute path to the module without having to worry about the path being relative to the Python interpreter.
		 
		 @param method - The name of the method to load. For example'py'or'cpython '.
		 @param module_path - The absolute path to the module.
		 
		 @return The : class : ` ~pyspark. sql. RowProxy ` that was loaded from the module
		"""
		try:
			module = imp.load_source(method, module_path)
			return module.queryPY
		except BaseException as e:
			raise e
elif sys.version_info >= (3, 12):
	import importlib
	def load_source(method, module_path):
		"""
		 Load a module and return its queryPY. This is a convenience function for calling : func : ` importlib. util. module_from_file_location ` with
		 
		 @param method - Name of the method to use
		 @param module_path - Path to the module to load
		 
		 @return A : class : ` pytest. queryPY. QueryPy ` object or None if the module could not be
		"""
		spec = importlib.util.spec_from_file_location(method, module_path)
		module = importlib.util.module_from_spec(spec)
		try:
			spec.loader.exec_module(module)
			return module.queryPY
		except BaseException as e:
			raise e
else:
	raise ImportError('The Python version does not support this module. Module available since Python >= 3.6.x')

class RunInterval(Timer):

	def __init__(self,obj,args=None, kwargs=None,t=1,to=None):
		"""
		 Initialize the timer. This is called by __init__ and should not be called directly. Instead use start () and cancel () to start the timer
		 
		 @param obj - The object that will be used to run the timer
		 @param args - Positional arguments to pass to the object's __init__ method
		 @param kwargs - Keyword arguments to pass to the object's __init__ method
		 @param t - Time in seconds to wait before starting the timer
		 @param to - Time in seconds to wait before canceling the
		"""
		super(RunInterval,self).__init__(t,obj,args,kwargs)
		# sleeps to the given time.
		if to != None:
			self.start()
			TM.sleep(to)
			self.cancel()

	def run(self):
		"""
		 Run the function until self. finished is set to False. This is a blocking call so you don't have to worry about this
		"""
		# Wait for the interval to finish.
		while not self.finished.wait(self.interval):
			self.function(*self.args,**self.kwargs)

class ProgressBar:
	def __init__(self, max_value, desc="Loading: ", disable=True):
		"""
		 Initialize the class. Called by __init__. Do not call directly. You should use this method instead
		 
		 @param max_value - Maximum value to show.
		 @param desc - Description of the setting. Default is " Loading : "
		 @param disable - Whether to disable the
		"""
		self.max_value = max_value
		self.disable = disable
		self.desc = desc
		self.p = self.pbar()

	def pbar(self):
		"""
		 Create a progress bar for this task. This is a convenience method to make it easier to use in a : class : ` ~kivy. graph_objs. ProgressBar `.
		 
		 
		 @return A : class : ` ~kivy. graph_objs. ProgressBar ` object that can be used to draw the progress bar
		"""
		return tqdm.tqdm(
			total=self.max_value,
			desc=self.desc,
			disable=self.disable
		)

	def update(self, update_value):
		"""
		 Update the parameter with a new value. This is useful for debugging and to avoid having to re - run the program every time you change the parameter
		 
		 @param update_value - The value to update
		"""
		self.p.update(update_value)

	def close(self):
		"""
		 Close the PulseBlaster. This is a no - op if PulseBlaster is already closed
		"""
		self.p.close()

class QueryRead:

	__DATA_DIR__ = os.path.dirname(__file__)

	__METHODS__ = ['sqlite','mysql','_1c','excel','postgresql','sqlserver','googlesheet','access','oracle']

	def __init__(self,
			bd:dict={},
			thread:bool=False, limit:int=0, prog:bool=False,
			auto_commit:bool=True, 
			**data
		):
		"""
			 Initialize QueryRead with data. This is the constructor for QueryRead. It sets the parameters and starts the thread that reads from the database
			 
			 @param bd - dictionary of parameters to be passed to the query
			 @param thread - if True thread is started else it is started
			 @param limit - maximum number of records to read
			 @param prog - if True print progress to stdout ( default False )
			 @param auto_commit - if True auto commit to database ( default False
		"""
		super(QueryRead, self).__init__()
		self.limit = max(int(limit),0)
		self.prog = prog
		self.thread = thread
		self.parameters = {
			'auto_commit':auto_commit
		}
		self.LENGTH = 0
		self.WORKING_TIME = TM.time()
		self.__bd = bd if len(bd)>0 else data
		self.step = 0
		self.finished = lambda : len(self.__bd)==self.step
		self.__threads = []
		self.__active_thread = None
		self.__enjoin()
		self.WORKING_TIME = TM.time() - self.WORKING_TIME

	def __enjoin(self):
		"""
		 Enjoin the methods and set the data to the object @throws QueryException if the method doesn't exist
		"""
		# Set the data for the given method and data.
		for method, data in self.__bd.items():
			# Raise a QueryException if the method is not defined
			if method.lower().strip() not in self.__METHODS__:
				raise QueryException(f"'{method}' method does not exist. You entered it incorrectly or it does not exist yet")
			# Raise a QueryException if data is not a list or tuple.
			if not isinstance(data, (list,tuple) ):
				raise QueryException(f"The data type must be list or tuple")
			# Set the object method and data.
			if self.thread:
				self.__start_thread(self.__set_object,method,data)
			else:
				self.__set_object(method,data)
			self.step += 1

	def __set_object(self,method,data):
		"""
		 Set object to be used by methods. This is a wrapper around __get_full to make it easier to use
		 
		 @param method - Name of method to call
		 @param data - Data to send to method ( sans headers
		"""
		res = asyncio.run(self.__get_full(method,data))
		setattr(self,method,DataBase(method,res))

	def __start_thread(self,func,*args,**kwargs):
		"""
		 Starts a thread to run the given function. This is a helper for __init__ to avoid having to do it every time
		 
		 @param func - function to run in
		"""
		current_thread = Thread(target=func, args=args, kwargs=kwargs, daemon=True)
		current_thread.start()

	async def __load_table(self, connect, tab, cols, method):
		"""
		 Load data from a table. This is a wrapper around : meth : ` ~pysnmp. i3s. I3S. query_f ` and
		 
		 @param connect - A connection to the database
		 @param tab - The name of the table
		 @param cols - The columns to load from the table
		 @param method - The method to use for loading the table.
		 
		 @return A DataFrame with the data loaded from the table and the number of rows loaded ( self. LENGTH +
		"""
		def add(a,b):
			"""
			 Add two lists and update p1. This is used for debugging. The list is returned as a list so it can be passed to other functions in this module
			 
			 @param a - list to be extended.
			 @param b - list to be added. If a is a list b is appended to the list.
			 
			 @return a + b in order of a. append ( b ) and p1. update ( 1 )
			"""
			a = list(a)
			a.extend(b)
			# Update the program if the program is running.
			if self.prog: p1.update(1)
			return a
		cqr = ['_query_', '_upgraded_', '_connection_']
		vqr = [connect.query_f, partial(self.__start_thread, self.__enjoin), partial(lambda: connect)]
		value = connect.query_f('SELECT', {tab: cols}, Condition())
		value = tuple(value.values())[0] if self.limit==0 else tuple(value.values())[0][:self.limit]
		# If the program is not running in progress.
		if self.prog: p1 = ProgressBar(len(value), f'Loading "{tab}" from {method}', not self.prog)
		# Return a DataFrame with the values of the value.
		if len(value) >0:
			self.LENGTH += len(value)
			data = DataFrame(list(map(lambda x: dict(zip(add(cols,cqr),add(x,vqr))),value)))
			# close the program if it s a program
			if self.prog: p1.close()
			return data
		cols, val = list(cols), [NoneValue()] * len(cols)
		cols.extend(['_query_', '_upgraded_', '_connection_'])
		val.extend([connect.query_f, partial(self.__start_thread, self.__enjoin), partial(lambda: connect)])
		return DataFrame([dict(zip(cols, val))])

	async def __tables(self, connect, method):
		"""
		 Loads and returns tables. This is a generator that yields all tables and their data. The method determines which methods are used to load the data.
		 
		 @param connect - A DB API 2 connection to the database.
		 @param method - The method used to load the data. Can be one of'get'' post_load'or'get_all '.
		 
		 @return A Series with the tables and their data. Each row in the Series is a dict with the keys'_tablename'and'_upgraded_ '
		"""
		data_base = Series()
		tables = connect.query_f('SHOW_TABLE')
		columns = connect.query_f('SHOW_COLUMNS', tables).items()
		# Load the data for each tab in columns.
		for tab, cols in columns:
			data_base[tab] = await self.__load_table(connect, tab, cols, method)
		data_base['_query_'] = connect.query_f
		data_base['_upgraded_'] = partial(self.__start_thread, self.__enjoin)
		data_base['_connection_'] = partial(lambda: connect)
		return data_base

	async def __get_full(self, method, data):
		"""
		 Get a Series object from a list of data. This is a wrapper around : meth : ` __connect ` and
		 
		 @param method - The method to call.
		 @param data - The list of data to fetch. It must be a list of dicts.
		 
		 @return An object with the DB_NAME as key and a list of tables as value. Example :. from iota import iota_client sage : await sage. get_full ('db'data
		"""
		obj = Series()
		# Add the tables to the object.
		for o in data:
			connect = await self.__connect(method,o)
			obj[connect.DB_NAME] = await self.__tables(connect,method)
		return obj

	async def __load(self, method:str):
		"""
		 Load a method from database_lib. py. This is a generator that yields instances of the method
		 
		 @param method - Name of the method to load
		 
		 @return Instance of the method or None if not found or error while loading the module >>> from pycldf. sql import Database >>> db = Database
		"""
		lib_path = os.path.join(self.__DATA_DIR__, 'database_lib')
		# Raise QueryException if module directory is not found
		if not os.path.isdir(lib_path):
			raise QueryException('Module directory not found')
		module_path = os.path.join(lib_path, method + ".py")
		# Raise QueryException if module is not found
		if not os.path.isfile(module_path):
			raise QueryException('Module not found')
		return load_source(method, module_path)

	async def __connect(self,method:str, data:dict ):
		"""
		 Connect to method and return object. This is a coroutine. It will be called by __init__ when a connection is made
		 
		 @param method - Name of method to connect
		 @param data - Data to pass to method
		 
		 @return Object that can be used to send data to the method or None if no method is found for the
		"""
		# Prints out the connection to the server.
		if self.prog:
			print(f'Connection to {method}...')
		module = await self.__load(method)
		return module(data,self.parameters)

