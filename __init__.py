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
		try:
			module = imp.load_source(method, module_path)
			return module.queryPY
		except BaseException as e:
			raise e
elif sys.version_info >= (3, 12):
	import importlib
	def load_source(method, module_path):
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
		super(RunInterval,self).__init__(t,obj,args,kwargs)
		if to != None:
			self.start()
			TM.sleep(to)
			self.cancel()

	def run(self):
		while not self.finished.wait(self.interval):
			self.function(*self.args,**self.kwargs)

class ProgressBar:
	def __init__(self, max_value, desc="Loading: ", disable=True):
		self.max_value = max_value
		self.disable = disable
		self.desc = desc
		self.p = self.pbar()

	def pbar(self):
		return tqdm.tqdm(
			total=self.max_value,
			desc=self.desc,
			disable=self.disable
		)

	def update(self, update_value):
		self.p.update(update_value)

	def close(self):
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
		for method, data in self.__bd.items():
			if method.lower().strip() not in self.__METHODS__:
				raise QueryException(f"'{method}' method does not exist. You entered it incorrectly or it does not exist yet")
			if not isinstance(data, (list,tuple) ):
				raise QueryException(f"The data type must be list or tuple")
			if self.thread:
				self.__start_thread(self.__set_object,method,data)
			else:
				self.__set_object(method,data)
			self.step += 1

	def __set_object(self,method,data):
		res = asyncio.run(self.__get_full(method,data))
		setattr(self,method,DataBase(method,res))

	def __start_thread(self,func,*args,**kwargs):
		current_thread = Thread(target=func, args=args, kwargs=kwargs, daemon=True)
		current_thread.start()

	async def __load_table(self, connect, tab, cols, method):
		def add(a,b):
			a = list(a)
			a.extend(b)
			if self.prog: p1.update(1)
			return a
		cqr = ['_query_', '_upgraded_', '_connection_']
		vqr = [connect.query_f, partial(self.__start_thread, self.__enjoin), partial(lambda: connect)]
		value = connect.query_f('SELECT', {tab: cols}, Condition())
		value = tuple(value.values())[0] if self.limit==0 else tuple(value.values())[0][:self.limit]
		if self.prog: p1 = ProgressBar(len(value), f'Loading "{tab}" from {method}', not self.prog)
		if len(value) >0:
			self.LENGTH += len(value)
			data = DataFrame(list(map(lambda x: dict(zip(add(cols,cqr),add(x,vqr))),value)))
			if self.prog: p1.close()
			return data
		cols, val = list(cols), [NoneValue()] * len(cols)
		cols.extend(['_query_', '_upgraded_', '_connection_'])
		val.extend([connect.query_f, partial(self.__start_thread, self.__enjoin), partial(lambda: connect)])
		return DataFrame([dict(zip(cols, val))])

	async def __tables(self, connect, method):
		data_base = Series()
		tables = connect.query_f('SHOW_TABLE')
		columns = connect.query_f('SHOW_COLUMNS', tables).items()
		for tab, cols in columns:
			data_base[tab] = await self.__load_table(connect, tab, cols, method)
		data_base['_query_'] = connect.query_f
		data_base['_upgraded_'] = partial(self.__start_thread, self.__enjoin)
		data_base['_connection_'] = partial(lambda: connect)
		return data_base

	async def __get_full(self, method, data):
		obj = Series()
		for o in data:
			connect = await self.__connect(method,o)
			obj[connect.DB_NAME] = await self.__tables(connect,method)
		return obj

	async def __load(self, method:str):
		lib_path = os.path.join(self.__DATA_DIR__, 'database_lib')
		if not os.path.isdir(lib_path):
			raise QueryException('Module directory not found')
		module_path = os.path.join(lib_path, method + ".py")
		if not os.path.isfile(module_path):
			raise QueryException('Module not found')
		return load_source(method, module_path)

	async def __connect(self,method:str, data:dict ):
		if self.prog:
			print(f'Connection to {method}...')
		module = await self.__load(method)
		return module(data,self.parameters)

