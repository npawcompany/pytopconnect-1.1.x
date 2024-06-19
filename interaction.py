import os
import re
import sys
import json
import codecs
from glob import glob
from .condition import *
from .datatypes import *
from .database import *
from .storage import *


class Import:

	def __init__(self):
		pass

class Export:

	def __init__(self):
		pass

class File:

	def __init__(self,file:str,mode:str='rb',charset:str='utf-8',_codecs:bool=False,*args,**kwargs):
		if not os.path.isfile(file):
			return Exception(f'The file at this path "{file}" does not exist')
		self.charset = charset
		if _codecs:
			self.__file__ = codecs.open(file,mode,encoding=charset)
		else:
			self.__file__ = open(file,mode)

	def read(self,line:bool=False,decode:bool=False,_json:bool=False) -> Union[str,list,tuple,dict,bytes]:
		if line:
			return self.__file__.readlines()
		if decode:
			return self.__file__.read().decode(self.charset)
		if _json:
			return json.load(self.__file__)
		return self.__file__.read()

	def write(self,data) -> bool:
		try:
			if isinstance(data,(list,tuple,dict)):
				self.__file__.write(json.dumps(data, encoding=self.charset, ensure_ascii=False))
			else:
				self.__file__.write(data)
			return True
		except:
			return False

	def close(self):
		self.__file__.close()

class Files(dict):

	def __init__(self,_path:str=os.getcwd(),mode:str='r',_type='*.sql',charset:str='utf-8',_codecs:bool=False,*args,**kwargs):
		super(Files,self).__init__(*args,**kwargs)
		self.FILES = []
		for file in glob(os.path.join(_path,'**',_type), recursive=True):
			file = os.path.abspath(file)
			_file = File(file,mode,charset,_codecs)
			self.FILES.append(_file)
			self.update({file:_file})

	def get(self,file_path:str='',default=None) -> Union[File,None,tuple]:
		if len(file_path.strip())==0:
			return tuple(self.values())
		for key,file in tuple(self.items()):
			if file_path.lower().strip() in key.lower().strip():
				return file
		return default

	def all_close(self):
		for key,file in tuple(self.items()):
			if isinstance(file,File):
				file.close()
				del self[key]


