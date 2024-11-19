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
	"""
	A class representing an import operation.

	This class serves as a placeholder for functionality related to importing data or resources. 
	Currently, it does not implement any specific behavior but can be extended in the future.

	Args:
		None
	"""

	def __init__(self):
		pass

class Export:
	"""
	A class representing an export operation.

	This class serves as a placeholder for functionality related to exporting data or resources. 
	Currently, it does not implement any specific behavior but can be extended in the future.

	Args:
		None
	"""

	def __init__(self):
		pass

class File:
	"""
	A class for handling file operations with support for different modes and encodings.

	This class provides methods to read from and write to files, allowing for various formats 
	such as text and JSON. It ensures that the file exists before attempting to open it and 
	manages the file's encoding and mode.

	Args:
		file (str): The path to the file to be opened.
		mode (str, optional): The mode in which to open the file. Defaults to 'rb'.
		charset (str, optional): The character set to use for encoding. Defaults to 'utf-8'.
		_codecs (bool, optional): If True, uses codecs for file opening. Defaults to False.
		*args: Additional positional arguments.
		**kwargs: Additional keyword arguments.

	Attributes:
		charset (str): The character set used for encoding.
		__file__: The file object for reading or writing.

	Methods:
		read(line: bool = False, decode: bool = False, _json: bool = False) -> Union[str, list, tuple, dict, bytes]:
			Reads data from the file based on specified options.
		write(data) -> bool:
			Writes data to the file, supporting various data types.
		close():
			Closes the file.
	"""

	def __init__(self,file:str,mode:str='rb',charset:str='utf-8',_codecs:bool=False,*args,**kwargs):
		if not os.path.isfile(file):
			return Exception(f'The file at this path "{file}" does not exist')
		self.charset = charset
		if _codecs:
			self.__file__ = codecs.open(file,mode,encoding=charset)
		else:
			self.__file__ = open(file,mode)

	def read(self, line: bool = False, decode: bool = False, _json: bool = False) -> Union[str, list, tuple, dict, bytes]:
		"""
		Read data from the opened file with various options.

		This function retrieves data from the file based on the specified parameters, allowing for 
		reading lines, decoding the content, or loading JSON data. It returns the data in the appropriate 
		format based on the options provided.

		Args:
			line (bool, optional): If True, reads the file line by line. Defaults to False.
			decode (bool, optional): If True, decodes the file content using the specified charset. Defaults to False.
			_json (bool, optional): If True, loads the content as JSON. Defaults to False.

		Returns:
			Union[str, list, tuple, dict, bytes]: The content of the file in the specified format, 
													depending on the options selected.
		"""
		if line:
			return self.__file__.readlines()
		if decode:
			return self.__file__.read().decode(self.charset)
		return json.load(self.__file__) if _json else self.__file__.read()

	def write(self, data) -> bool:
		"""
		Write data to the opened file.

		This function handles writing various types of data to the file, including lists, tuples, 
		dictionaries, and strings. It returns a boolean indicating the success of the write operation.

		Args:
			data: The data to be written to the file, which can be of various types including 
				list, tuple, dict, or str.

		Returns:
			bool: True if the data was successfully written to the file, False otherwise.

		Raises:
			Exception: If an error occurs during the write operation.
		"""
		try:
			if isinstance(data,(list,tuple,dict)):
				self.__file__.write(json.dumps(data, encoding=self.charset, ensure_ascii=False))
			else:
				self.__file__.write(data)
			return True
		except Exception as e:
			return False

	def close(self):
		"""
		Close the opened file.

		This function terminates the file operation by closing the file associated with the current 
		instance. It ensures that any buffered data is flushed and resources are released.

		Returns:
			None
		"""
		self.__file__.close()

class Files(dict):
	"""
	A class for managing a collection of file objects.

	This class extends the dictionary to store and manage multiple file instances, allowing for 
	easy access and manipulation of files based on specified criteria. It provides methods to 
	retrieve files, close all open files, and manage file attributes.

	Args:
		_path (str, optional): The directory path to search for files. Defaults to the current working directory.
		mode (str, optional): The mode in which to open the files. Defaults to 'r'.
		_type (str, optional): The file type pattern to match (e.g., '*.sql'). Defaults to '*.sql'.
		charset (str, optional): The character set to use for file encoding. Defaults to 'utf-8'.
		_codecs (bool, optional): If True, uses codecs for file opening. Defaults to False.
		*args: Additional positional arguments for the dictionary.
		**kwargs: Additional keyword arguments for the dictionary.

	Attributes:
		FILES (list): A list of File objects managed by the Files class.

	Methods:
		get(file_path: str = '', default=None) -> Union[File, None, tuple]:
			Retrieve a specific file or all files if no path is provided.
		all_close():
			Close all open file instances and remove them from the collection.
	"""

	def __init__(self,_path:str=os.getcwd(),mode:str='r',_type='*.sql',charset:str='utf-8',_codecs:bool=False,*args,**kwargs):
		super(Files,self).__init__(*args,**kwargs)
		self.FILES = []
		for file in glob(os.path.join(_path,'**',_type), recursive=True):
			file = os.path.abspath(file)
			_file = File(file,mode,charset,_codecs)
			self.FILES.append(_file)
			self.update({file:_file})

	def get(self, file_path: str = '', default=None) -> Union[File, None, tuple]:
		"""
		Retrieve a file object from the collection based on the specified file path.

		This function searches for a file in the collection that matches the provided file path. 
		If no path is specified, it returns all files as a tuple; if a matching file is found, 
		it returns that file; otherwise, it returns a default value.

		Args:
			file_path (str, optional): The path of the file to retrieve. Defaults to an empty string.
			default: The value to return if no matching file is found.

		Returns:
			Union[File, None, tuple]: The matching File object, None if no match is found, or a tuple 
									of all files if no path is provided.
		"""
		if not file_path.strip():
			return tuple(self.values())
		return next(
			(
				file
				for key, file in self.items()
				if file_path.lower().strip() in key.lower().strip()
			),
			default,
		)

	def all_close(self):
		"""
		Close all open file instances in the collection and remove them.

		This function iterates through all files in the collection, closing each file that is an 
		instance of the File class and then removing it from the collection. This helps to manage 
		resources effectively by ensuring that all files are properly closed.

		Returns:
			None
		"""
		for key,file in tuple(self.items()):
			if isinstance(file,File):
				file.close()
				del self[key]
