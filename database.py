from .condition import *
from .datatypes import DataTypes, Index
from .storage import Procedure, Function, Trigger
from pandas import Series, DataFrame
from fuzzywuzzy import fuzz, process
from datetime import *
from decimal import Decimal
from dateutil import parser as dps
import importlib
import inspect

import types
from typing import Union
import json

def __default_values__(key, args, n, t=False):
	"""
	Generate default values for a database column based on its constraints and specifications.

	This function interprets various SQL-like column constraints and generates appropriate default values.
	It handles NOT NULL constraints, auto-increment fields, and various data types including timestamps,
	booleans, and numeric values.

	Parameters:
	key (str): The name of the column for which default values are being generated.
	args (str, tuple, or list): The constraints or specifications for the column. Can be a string
								representing a single constraint or a list/tuple of constraints.
	n (int): The number of default values to generate.
	t (bool, optional): If True, returns a list of NoneValue objects regardless of other parameters.
						Defaults to False.

	Returns:
	list: A list of default values for the column. The type of values in the list depends on the
		  constraints specified in 'args'.

	Raises:
	QueryException: If a NOT NULL constraint is specified but default values cannot be determined.

	Note:
	- The function attempts to parse dates, JSON, and numeric values.
	- For unrecognized formats, it returns the string value without quotes.
	"""
	n = max(n,0)
	if t:
		return [NoneValue]*n
	if (any([v.strip()=='NOT NULL' for v in args]) if isinstance(args,(tuple,list)) else args.strip()=='NOT NULL'):
		raise QueryException(f'Column "{key}" must not be NOT NULL')
	if re.search(r'INCREMENT', str(args)) is not None:
		return list(range(1,n+1))
	args = re.search(r'DEFAULT\((.*?)\)', str(args))
	args = args if args is None else args.group(1)
	args = None if args=="NULL" else args
	if args is None:
		return [None]*n
	try:
		return [dps.parse(args)]*n
	except:
		try:
			return [json.loads(args)]*n
		except:
			if bool(re.match(r'^-?\d+(\.\d+)?$', args)):
				return [float(args)]*n
			elif args == 'CURRENT_TIMESTAMP':
				return [datetime.now()]*n
			elif args in ['TRUE','FALSE']:
				return [True if args=='TRUE' else False]*n
			return [args[1:-1]]*n

def __default_to_value__(key, val):
	"""
	Convert a given value to its appropriate Python data type.

	This function attempts to convert the input value to a more specific data type
	based on its content. It handles numeric values, timestamps, booleans, dates,
	and JSON strings.

	Parameters:
	key (str): The key associated with the value. Not used in the function but 
			   included for potential future use or consistency with other functions.
	val (str or None): The value to be converted. If "NULL" or None, it remains None.

	Returns:
	The converted value in its appropriate data type (float, datetime, bool, date, dict/list for JSON),
	or the original string value if no conversion is applicable.
	"""
	val = None if val == "NULL" or val is None else str(val)
	if val is not None:
		if bool(re.match(r'^-?\d+(\.\d+)?$', val)):
			return float(val)
		elif val == 'CURRENT_TIMESTAMP':
			return datetime.now()
		elif val in ['TRUE', 'FALSE']:
			return True if val == 'TRUE' else False
		try:
			return dps.parse(val[1:-1])
		except:
			try:
				return json.loads(val[1:-1])
			except:
				return val[1:-1]
	return val

def __check_variable_name__(text):
	"""
	Check if the given text is a valid Python variable name.

	This function uses a regular expression to verify if the input text
	follows the rules for a valid Python variable name: it must start with
	a letter or underscore, followed by any number of letters, numbers, or
	underscores.

	Parameters:
	text (str): The string to be checked as a potential variable name.

	Returns:
	re.Match object or None: If the text is a valid variable name, a match object is returned.
							 If not valid, None is returned.
	"""
	pattern = r'^[a-zA-Z_][a-zA-Z0-9_]*$'
	return re.match(pattern, text)

class DataBase:
	"""
	A class representing a database connection and operations.
	"""

	def __init__(self, method: str, data: types.GeneratorType, *args, **kwargs):
		"""
		Initialize the DataBase object.

		Args:
			method (str): The database connection method.
			data (types.GeneratorType): A generator containing database data.
			*args: Variable length argument list.
			**kwargs: Arbitrary keyword arguments.

		Raises:
			QueryException: If the data type does not match Series.
		"""
		super(DataBase, self).__init__()
		if not isinstance(data, Series):
			raise QueryException(f"Data types do not match")
		self.names = []
		self.data_bases = []
		self.method = method
		for name, tables in data.items():
			self.names.append(name)
			if not __check_variable_name__(f'tc_{name}'):
				raise QueryException(f"The '{name}' data base name must follow the variable creation rules")
			data_bases = Tables(method,name,tables)
			self.data_bases.append(data_bases)
			setattr(self, f'tc_{name}', data_bases)
	
	def get(self,name:str,default=None):
		return getattr(self,name,default)

	def create_index(self, table):
		"""
		Create an index for the given table.

		Args:
			table: The table object for which to create an index.

		Raises:
			QueryException: If the '_tc_indexs_' attribute is not of the correct type.
		"""
		name_table = getattr(table, '_tc_name_table_', table.__class__.__name__)
		if hasattr(table, '_tc_indexs_'):
			indexs = getattr(table, '_tc_indexs_')
			if not isinstance(indexs, (types.MethodType, Index, tuple, list)):
				raise QueryException(f"'_tc_indexs_' must be a method or list or tuple or Index")
			indexs = indexs() if isinstance(indexs, types.MethodType) else indexs
			if isinstance(indexs, (tuple, list)):
				for index in indexs:
					if not self._query_('CREATE_INDEX', {name_table: index}):
						return 
			else:
				if not self._query_('CREATE_INDEX', {name_table: indexs}):
					return

	def create_foreign(self, table):
		"""
		Create foreign key constraints for the given table.

		Args:
			table: The table object for which to create foreign key constraints.

		Raises:
			QueryException: If the '_tc_foreigns_' attribute is not of the correct type.
		"""
		name_table = getattr(table, '_tc_name_table_', table.__class__.__name__)
		if hasattr(table, '_tc_foreigns_'):
			foreigns = getattr(table, '_tc_foreigns_')
			if not isinstance(foreigns, (types.MethodType, Index, tuple, list)):
				raise QueryException(f"'_tc_foreigns_' must be a method or list or tuple or Forein")
			foreigns = foreigns() if isinstance(foreigns, types.MethodType) else foreigns
			if isinstance(foreigns, (tuple, list)):
				for forein in foreigns:
					if not self._query_('CREATE_FOREIGN', name_table, forein):
						return 
			else:
				if not self._query_('CREATE_FOREIGN', name_table, foreigns):
					return

	def create(self, obj=None, *args, **kwargs):
		"""
		Create a new table in the database.

		Args:
			obj: The object to be used for table creation (optional).
			*args: Variable length argument list.
			**kwargs: Arbitrary keyword arguments.

		Returns:
			The created table object.

		Raises:
			QueryException: If the input is not a valid object or if there are issues with table creation.
		"""
		def append(func):
			if not isinstance(func, type):
				raise QueryException(f"An object must be passed")
			table = func(self.method, *args, **kwargs)
			name_table = getattr(table, '_tc_name_table_', table.__class__.__name__)
			if not __check_variable_name__(name_table.strip()):
				raise QueryException(f"The '{name_table}' table name must follow the variable creation rules")
			r = getattr(table, '_tc_options_', {})
			r = r() if isinstance(r, types.MethodType) else r
			prefix = getattr(table, '_tc_prefix_', '').strip()
			prefix = (prefix if __check_variable_name__(prefix.strip()) else '') if isinstance(prefix, str) else ''
			if len(prefix) > 0:
				name_table = prefix + "_" + name_table
			if '__TC_DATANAME__' in table.__dict__.keys():
				del table.__dict__['__TC_DATANAME__']
			if not isinstance(r, dict):
				raise QueryException("'_tc_options_' must be a dictionary")
			if self.is_table(name_table):
				return self.get(name_table)
			if not self._query_('CREATE', {name_table: table.__dict__}, r):
				return
			self.create_index(table)
			self.create_foreign(table)
			values = getattr(table, '_tc_values_', {})
			if not isinstance(values, (dict, types.MethodType)):
				raise QueryException(f"'_tc_values_' must be a method or dictionary")
			values = values() if isinstance(values, types.MethodType) else values
			if isinstance(values, dict):
				if len(values) > 0:
					if not self._query_('INSERT', {name_table: {
						'columns': values.keys(),
						'values': map(lambda x: [str(i) if isinstance(i, (int, tuple)) else f" '{i}'  " for i in x], zip(*values.values()))
					}}):
						return
			ns = [len(v) for v in values.values()]
			n = int(sum(ns) / max(len(ns), 1))
			if not all([n == i for i in ns]):
				raise QueryException(f"Number of values do not match")
			self[name_table] = [{
				'_query_': self._query_,
				'_upgraded_': self._upgraded_
			}]
			for key, val in table.__dict__.items():
				if key not in ['__TC_DATANAME__']:
					if not any([ik in table.__dict__.keys() for ik in values.keys()]):
						self[name_table][0][key] = NoneValue()
						self[name_table][0]['_query_'] = self._query_
						self[name_table][0]['_upgraded_'] = self._upgraded_
					else:
						i = 0
						for value in values.get(key, __default_values__(key, val, n, key in values.keys())):
							try:
								self[name_table][i][key] = value
							except IndexError:
								self[name_table].append({key: value})
							self[name_table][i]['_query_'] = self._query_
							self[name_table][i]['_upgraded_'] = self._upgraded_
							i += 1
			self.enjoin()
			self._upgraded_()
			return self.get(name_table)
		return append if obj is None else partial(append, obj)

class Tables(Series, DataBase):
	"""
	A class representing database tables, inheriting from Series and DataBase.
	"""

	def __init__(self, method: str, name: str, items: Series, *args, **kwargs):
		"""
		Initialize the Tables object.

		Args:
			method (str): The database method (e.g., 'mysql', 'postgresql').
			name (str): The name of the database.
			items (Series): The initial items for the Tables object.
			*args: Variable length argument list.
			**kwargs: Arbitrary keyword arguments.
		"""
		super(Tables, self).__init__(items)
		self.name = name
		self.method = method
		self.parent = kwargs.get('parent', None)
		self.dataTypes = DataTypes(self.method)
		self.enjoin()
		self.version = self._connection_().version()
		self.db_name = self._connection_().DB_NAME_ORG
		if self.method in ['mysql', 'postgresql']:
			procedures = self._query_('SHOW_PROCEDURE', self.db_name)
			functions = self._query_('SHOW_FUNCTION', self.db_name)
			for procedure, value in procedures.items():
				proc = Procedure(self)
				proc.name = procedure
				proc.list_paramets = list(filter(None, value))
				setattr(self, f'ptc_{procedure}', proc)
			for function, value in functions.items():
				func = Function(self)
				func.name = function
				func.list_paramets = list(filter(None, value))
				setattr(self, f'ftc_{function}', func)

	def enjoin(self):
		"""
		Join tables and set attributes for each table.

		Raises:
			QueryException: If a table name doesn't follow variable creation rules.
		"""
		self.ALL_TABLES = self.get_tables()
		for key, val in self.items():
			if not __check_variable_name__(f'tc_{key}'):
				raise QueryException(f"The '{key}' table name must follow the variable creation rules")
			if not isinstance(val, (types.LambdaType, types.FunctionType, types.MethodType, partial)):
				if not self.is_table(key):
					setattr(self, f'tc_{key}', Items(key, val, parent=self))
			else:
				setattr(self, key, val)

	def is_active(self) -> bool:
		"""
		Check if the database connection is active.

		Returns:
			bool: True if the connection is active, False otherwise.
		"""
		return self._connection_().is_active()

	def open(self) -> bool:
		"""
		Open the database connection if it's not already active.

		Returns:
			bool: True if the connection was opened, False if it was already active.
		"""
		if self.is_active():
			return False
		self._connection_().open()
		return True

	def close(self) -> bool:
		"""
		Close the database connection if it's active.

		Returns:
			bool: True if the connection was closed, False if it was already inactive.
		"""
		if not self.is_active():
			return False
		self._connection_().close()
		return True

	def commit(self):
		"""
		Commit the current transaction.
		"""
		self._connection_().commit()

	def rollback(self):
		"""
		Rollback the current transaction.
		"""
		self._connection_().rollback()

	def get_procedure(self, procedure: str, default=None):
		"""
		Get a stored procedure by name.

		Args:
			procedure (str): The name of the procedure.
			default: The default value to return if the procedure is not found.

		Returns:
			The procedure object if found, otherwise the default value.
		"""
		return getattr(self, f'ptc_{procedure}', default)

	def get_procedures(self):
		"""
		Get all stored procedures.

		Returns:
			tuple: A tuple of all stored procedure objects.
		"""
		return tuple(map(lambda y: self.get_procedure(y.replace('ptc_', '', 1)), filter(lambda x: x.startswith('ptc_'), self.__dict__.keys())))

	def get_function(self, function: str, default=None):
		"""
		Get a stored function by name.

		Args:
			function (str): The name of the function.
			default: The default value to return if the function is not found.

		Returns:
			The function object if found, otherwise the default value.
		"""
		return getattr(self, f'ftc_{function}', default)

	def get_functions(self):
		"""
		Get all stored functions.

		Returns:
			tuple: A tuple of all stored function objects.
		"""
		return tuple(map(lambda y: self.get_function(y.replace('ftc_', '', 1)), filter(lambda x: x.startswith('ftc_'), self.__dict__.keys())))

	def is_table(self, *tables) -> bool:
		"""
		Check if all specified tables exist.

		Args:
			*tables: Variable length argument list of table names.

		Returns:
			bool: True if all specified tables exist, False otherwise.
		"""
		return all([hasattr(self, f'tc_{table}') for table in tables])

	def get(self, table: str, default=None):
		"""
		Get a table by name.

		Args:
			table (str): The name of the table.
			default: The default value to return if the table is not found.

		Returns:
			The table object if found, otherwise the default value.
		"""
		return getattr(self, f'tc_{table}', default)

	def get_tables(self, *tables) -> list:
		"""
		Get all tables or specified tables.

		Args:
			*tables: Variable length argument list of table names.

		Returns:
			list: A list of table objects.
		"""
		if len(tables) == 0:
			return list(self.keys())
		return [self.get(table) for table in tables if self.is_table(table)]

	def remove(self, table: str) -> bool:
		"""
		Remove a table from the database.

		Args:
			table (str): The name of the table to remove.

		Returns:
			bool: True if the table was successfully removed, False otherwise.

		Raises:
			QueryException: If the table does not exist.
		"""
		try:
			if not self.is_table(table):
				raise QueryException(f"Table '{table}' does not exist")
			if not self._query_('DROP', table):
				return False
			self.drop(table, inplace=True)
			delattr(self, f'tc_{table}')
			self.enjoin()
			return True
		except BaseException as e:
			raise e

	def rename_tables(self, old_table: str, new_table: str) -> bool:
		"""
		Rename a table in the database.

		Args:
			old_table (str): The current name of the table.
			new_table (str): The new name for the table.

		Returns:
			bool: True if the table was successfully renamed, False otherwise.

		Raises:
			QueryException: If the old table does not exist, the new table name already exists,
							or the new table name doesn't match creation rules.
		"""
		try:
			if not self.is_table(old_table):
				raise QueryException(f"Table '{old_table}' does not exist")
			if self.is_table(new_table):
				raise QueryException(f'"{new_table}" name already exists')
			if not __check_variable_name__(new_table):
				raise QueryException(f'"{new_table}" name does not match the creation rules')
			if not self._query_('RENAME_TABLE', {old_table: new_table}):
				return False
			table = self.get(old_table)
			table.table = new_table
			setattr(self, f'tc_{new_table}', table)
			delattr(self, f'tc_{old_table}')
			self.rename(index={old_table: new_table}, inplace=True)
			self.enjoin()
			self._upgraded_()
			return True
		except BaseException as e:
			raise e

class Items(DataFrame):
	"""
	A class representing a database table, inheriting from DataFrame.
	"""

	def __init__(self, table: str, data: DataFrame, *args, **kwargs):
		"""
		Initialize an Items object.

		Args:
			table (str): The name of the table.
			data (DataFrame): The data to initialize the DataFrame with.
			*args: Variable length argument list.
			**kwargs: Arbitrary keyword arguments.

		Returns:
			None
		"""
		super(Items, self).__init__(data)
		self.table = table
		self.parent = kwargs.get('parent', None)
		self.enjoin()
		self.LENGTH = self.get_count_row()
		self.dataTypes = self.parent.dataTypes
		if self.parent.method in ['mysql', 'postgresql']:
			triggers = self._query_('SHOW_TRIGGER', self.table)
			for trigger, val in triggers.items():
				trig = Trigger(self, val.get('time'), val.get('event'))
				trig.name = trigger
				setattr(self, f'ttc_{trigger}', trig)

	def constructor(self, data):
		"""
		Create a new Items object with the given data and additional attributes.

		Args:
			data: The data to initialize the new Items object with.

		Returns:
			Items: A new Items object with the given data and additional attributes.
		"""
		data = data.copy()
		data.loc[:, '_query_'] = [self._query_] * max(len(data), 1)
		data.loc[:, '_upgraded_'] = [self._upgraded_] * max(len(data), 1)
		data.loc[:, '_connection_'] = [self._connection_] * max(len(data), 1)
		return Items(self.table, data, parent=self.parent)

	def __setattr__(self, key, value):
		"""
		Set an attribute on the object.

		Args:
			key: The name of the attribute to set.
			value: The value to set the attribute to.

		Returns:
			None
		"""
		self.__dict__[key] = value

	def enjoin(self):
		"""
		Update the object's attributes based on its current data.

		This method updates ALL_COLUMNS and LENGTH attributes, and creates new attributes
		for each column in the data.

		Returns:
			None

		Raises:
			QueryException: If a column name doesn't follow variable creation rules.
		"""
		def is_noneValue(value):
			return isinstance(value, NoneValue)
		self.ALL_COLUMNS = self.getColumns()
		self.LENGTH = self.get_count_row()
		for key, val in self.to_dict(orient='list').items():
			if len(val) > 0:
				if not all(isinstance(v, (types.LambdaType, types.FunctionType, types.MethodType, partial)) for v in val):
					if not __check_variable_name__(f'tc_{key}'):
						raise QueryException(f"The '{key}' column name must follow the variable creation rules")
					setattr(self, f'tc_{key}', Column(self.table, key, val, parent=self))
				else:
					setattr(self, key, val[0])
					del self[key]
		mask = self.apply(lambda col: col.apply(is_noneValue))
		indexs = list(self[mask.all(axis=1)].index)
		if len(indexs) > 0:
			self.drop(indexs, inplace=True)

	def to_str(self, x) -> str:
		"""
		Convert a value to its string representation.

		Args:
			x: The value to convert.

		Returns:
			str: The string representation of the input value.
		"""
		if isinstance(x, str):
			return repr(x)
		elif isinstance(x, bool):
			return f"{int(x)}"
		elif isinstance(x, (int, float)):
			return f"{x}"
		elif isinstance(x, Decimal):
			return f"{float(x)}"
		elif isinstance(x, (list, tuple, dict)):
			return f"'{json.dumps(x, ensure_ascii=False)}'"
		elif isinstance(x, (datetime, date, time)):
			return f"'{x}'"
		else:
			return "NULL"

	def is_empty(self) -> bool:
		"""
		Check if the Items object is empty.

		Returns:
			bool: True if the object is empty, False otherwise.
		"""
		return len(self) == 0

	def is_column(self, *columns) -> bool:
		"""
		Check if all specified columns exist in the Items object.

		Args:
			*columns: Variable length argument list of column names.

		Returns:
			bool: True if all specified columns exist, False otherwise.
		"""
		return all([hasattr(self, f'tc_{column}') for column in columns])

	def get_column(self, column: str):
		"""
		Get a column by name.

		Args:
			column (str): The name of the column to retrieve.

		Returns:
			The column object if found, None otherwise.
		"""
		return getattr(self, f'tc_{column}', None)

	def getColumns(self, *columns) -> list:
		"""
		Get specified columns or all columns from the DataFrame.
	
		This function retrieves either all columns (excluding certain system columns) 
		or specific columns provided as arguments.
	
		Args:
			*columns: Variable length argument list of column names to retrieve.
	
		Returns:
			list or DataFrame: If no columns are specified, returns a list of all column names.
							   If columns are specified, returns a DataFrame with the specified columns.
	
		Raises:
			QueryException: If the columns argument is not a tuple containing strings.
		"""
		if len(columns) == 0:
			try:
				return list(filter(lambda x: x not in ['_query_', '_upgraded_', '_connection_'], self.keys()))
			except IndexError:
				return getattr(self, 'ALL_COLUMNS', [])
		if not isinstance(columns, tuple):
			raise QueryException("Columns must be of the following data types: 'tuple' and must contain a string")
		columns = tuple(filter(self.is_column, columns))
		return DataFrame(dict(zip(columns, map(self.get_column, columns))))
	
	def get_count_columns(self) -> int:
		"""
		Get the number of columns in the DataFrame.
	
		Returns:
			int: The number of columns if the DataFrame is not empty, otherwise None.
		"""
		if not self.is_empty():
			return len(list(self.ALL_COLUMNS))
	
	def get_row(self, i: int) -> dict:
		"""
		Get a specific row from the DataFrame.
	
		Args:
			i (int): The index of the row to retrieve.
	
		Returns:
			dict: The row as a dictionary if the DataFrame is not empty, otherwise None.
		"""
		if not self.is_empty():
			return self[i]
	
	def get_count_row(self) -> int:
		"""
		Get the number of rows in the DataFrame.
	
		Returns:
			int: The number of rows in the DataFrame.
		"""
		return len(self)
	
	def is_required(self, column) -> bool:
		"""
		Check if a column is required.
	
		Args:
			column: The name of the column to check.
	
		Returns:
			bool: True if the column is required, False otherwise.
		"""
		return column in self.required_columns()
	
	def search_by_type(self, x, value=None, func: Union[types.FunctionType, types.MethodType, types.LambdaType, str] = fuzz.WRatio, similarity: Union[int, float] = 75, is_none: bool = False) -> bool:
		"""
		Search for a value in a given data type using a specified function and similarity threshold.
	
		Args:
			x: The value to search in.
			value: The value to search for. Defaults to None.
			func: The function to use for comparison. Can be a function, method, lambda, or string. Defaults to fuzz.WRatio.
			similarity: The similarity threshold for string comparisons. Defaults to 75.
			is_none: Whether to consider None values as a match. Defaults to False.
	
		Returns:
			bool: True if a match is found, False otherwise.
	
		Raises:
			QueryException: If the func, similarity, or is_none arguments are of incorrect types.
		"""
		if not isinstance(func, [types.FunctionType, types.MethodType, types.LambdaType, str]):
			raise QueryException('The "func" argument must be FunctionType, MethodType, LambdaType or str')
		if not isinstance(similarity, [int, float]):
			raise QueryException('The "similarity" argument must be int or float')
		if not isinstance(is_none, bool):
			raise QueryException('The "is_none" argument must be bool')
	
		if isinstance(func, str):
			func = func.split('.')
			mod = importlib.import_module('.'.join(func[:1]))
			if hasattr(mod, func[1]):
				func = getattr(mod, func[1])
			else:
				func = fuzz.WRatio
	
		if isinstance(x, str) and isinstance(value, str):
			return func(x, value) >= similarity
		elif isinstance(x, (list, tuple)) and isinstance(value, str):
			data = func(value, x)
			return data[1] >= similarity if isinstance(data, tuple) else any(map(lambda x: x[1] >= similarity, data))
		elif isinstance(x, str) and isinstance(value, (list, tuple)):
			data = func(x, value)
			return data[1] >= similarity if isinstance(data, tuple) else any(map(lambda x: x[1] >= similarity, data))
		elif isinstance(x, (int, float, Decimal)) and isinstance(value, (int, float, Decimal)):
			if not isinstance(func, types.LambdaType):
				func = lambda a, b: a == b
			return func(x, value)
		elif isinstance(x, (datetime, date, time)) and isinstance(value, (datetime, date, time)):
			if not isinstance(func, types.LambdaType):
				func = lambda a, b: a <= b
			return func(x, value)
	
		return (x is None) and is_none
	
	def search(self, use_and: bool = True, **items) -> DataFrame:
		"""
		Search for items in the DataFrame based on specified criteria.
	
		Args:
			use_and (bool): If True, use AND logic for multiple search criteria. If False, use OR logic. Defaults to True.
			**items: Keyword arguments specifying the search criteria for each column.
	
		Returns:
			DataFrame: A DataFrame containing the rows that match the search criteria.
		"""
		def change_params(val):
			obj = {'value': None, 'func': fuzz.WRatio, 'similarity': 75, 'is_none': False}
			if isinstance(val, dict):
				obj.update(val)
			elif isinstance(val, tuple):
				obj.update(dict(zip(obj.keys(), val)))
			else:
				obj['value'] = val
			return obj
		data = self.copy()
		if not self.is_column(*tuple(items.keys())):
			return data
		condition = [data[key].apply(partial(self.search_by_type, **change_params(value))) for key, value in items.items()]
		if use_and:
			return data[reduce(lambda x, y: x & y, condition)]
		return data[reduce(lambda x, y: x | y, condition)]

	def required_columns(self) -> list:
		"""
		Retrieve a list of required columns in the database table.

		This function checks the types of fields in the table and returns a list of column names 
		that are marked as required. It is useful for validating data before insertion or updates.

		Returns:
			list: A list of names of the required columns in the table.
		"""
		return [key for key, val in self.types().items() if val.get('required')]

	def get_trigger(self, trigger: str, default=None):
		"""
		Retrieve a stored trigger by its name.

		This function attempts to access a trigger attribute based on the provided name. 
		If the trigger does not exist, it returns a specified default value.

		Args:
			trigger (str): The name of the trigger to retrieve.
			default: The value to return if the trigger does not exist.

		Returns:
			The trigger object if found, otherwise the default value.
		"""
		return getattr(self, f'ttc_{trigger}', default)

	def get_triggers(self):
		"""
		Retrieve all stored triggers in the database.

		This function collects all attributes that represent triggers, filters them based on a naming convention, 
		and returns them as a tuple. It is useful for obtaining a list of all available triggers for further operations.

		Returns:
			tuple: A tuple containing all stored trigger objects.
		"""
		return tuple(map(lambda y: self.get_trigger(y.replace('ttc_','',1)),filter(lambda x: x.startswith('ttc_') ,self.__dict__.keys())))

	def commit(self):
		"""
		Commit the current transaction to the database.

		This function finalizes all changes made during the current transaction by calling the commit method 
		on the underlying database connection. It is essential for ensuring that all operations performed 
		since the last commit are saved to the database.

		Returns:
			None

		Raises:
			Exception: If there is an error during the commit operation.
		"""
		self._connection_().commit()

	def rollback(self):
		"""
		Rollback the current transaction in the database.

		This function undoes all changes made during the current transaction by calling the rollback method 
		on the underlying database connection. It is useful for reverting any operations that should not be 
		finalized due to errors or other conditions.

		Returns:
			None

		Raises:
			Exception: If there is an error during the rollback operation.
		"""
		self._connection_().rollback()

	def types(self, column: str = '') -> dict:
		"""
		Retrieve the types and attributes of fields in the database table.

		This function queries the database for field information and constructs a dictionary 
		that includes details such as field number, values, type, required status, default value, 
		and whether the field is a primary key. If a specific column name is provided, it returns 
		the details for that column; otherwise, it returns the details for all columns.

		Args:
			column (str, optional): The name of the column to retrieve types for. Defaults to an empty string.

		Returns:
			dict: A dictionary containing field types and attributes. If a specific column is requested, 
				returns its details; otherwise, returns details for all columns.
		"""
		FIELDS = self._query_('FIELDS',[self.table])
		FIELDS = { FIELD[1]:{
			"number":FIELD[0],
			"values":self.get_column(FIELD[1]),
			"type":FIELD[2],
			"required": bool(FIELD[3]),
			"default": __default_to_value__(FIELD[1],FIELD[4]),
			"is_primary":bool(FIELD[5])
		} for FIELD in FIELDS if self.is_column(FIELD[1]) }
		return FIELDS.get(column, FIELDS)

	def added(self, values: list, columns: list) -> list:
		"""
		Add values to the specified columns in the database.

		This function takes a list of values and a corresponding list of column names, ensuring that 
		each value matches the expected column length. It checks for required columns, assigns default 
		values where necessary, and handles primary key assignments, returning the updated list of values.

		Args:
			values (list): A list of values to be added to the specified columns.
			columns (list): A list of column names corresponding to the values.

		Returns:
			list: The updated list of values after processing.

		Raises:
			QueryException: If the lengths of values and columns do not match, or if required columns are missing.
		"""
		n = -1
		for i in range(len(values)):
			if len(values[i])!=len(columns):
				raise QueryException(f"Column and value lengths are not equal")
			value = dict(zip(columns,values[i]))
			for k,v in self.types().items():
				if self.is_required(k) and k not in value.keys() and not v.get('is_primary'):
					raise QueryException(f"The '{k}' column must be required")
				if k not in value.keys():
					if v.get('is_primary'):
						val = v.get('values',[])
						if n == -1:
							n = val.max() if len(val)>0 else 1
						n += 1
						value[k] = n
					else:
						value[k] = v.get('default')
			values[i] = value
		return values

	def get(self, columns: list = None, condition: Condition = Condition(), distinct: bool = False, sql: bool = False) -> DataFrame:
		"""
		Retrieve data from the database based on specified columns and conditions.

		This function allows for flexible querying of the database, enabling the retrieval of specific columns, 
		application of conditions, and options for distinct results or raw SQL queries. It returns the data as a 
		DataFrame, making it easy to manipulate and analyze.

		Args:
			columns (list, optional): A list of column names to retrieve. If None, retrieves all columns.
			condition (Condition, optional): A condition object to filter the results. Defaults to an empty condition.
			distinct (bool, optional): If True, retrieves only distinct rows. Defaults to False.
			sql (bool, optional): If True, executes a raw SQL query. Defaults to False.

		Returns:
			DataFrame: The retrieved data as a DataFrame.

		Raises:
			QueryException: If the data types of columns and condition do not match.
		"""
		if not isinstance(columns,(list, tuple, type(None))) and not isinstance(condition,Condition):
			raise QueryException(f"Data types do not match")
		if columns is None:
			columns = self.ALL_COLUMNS
		if sql:
			data = self._query_('SELECT_DISTINCT' if distinct else 'SELECT',{self.table:columns},condition)
			return list(map(lambda x: dict(zip(columns,x)),data))
		data = self.getColumns(*columns)
		for func in condition.functions:
			data = list(func(data))
		if distinct:
			return data.drop_duplicates()
		return data

	def add(self, values: list, columns: list) -> bool:
		"""
		Add new rows of data to the specified columns in the database.

		This function validates the provided column names and values, ensuring they match the existing 
		schema before attempting to insert the data into the database. It returns a boolean indicating 
		the success of the operation, and raises exceptions for any issues encountered during the process.

		Args:
			values (list): A list of dictionaries containing the values to be added.
			columns (list): A list of column names corresponding to the values.

		Returns:
			bool: True if the data was successfully added, False otherwise.

		Raises:
			QueryException: If the data types do not match or if the specified columns do not exist.
		"""
		if not isinstance(columns,(list, tuple)) and not isinstance(values,(list, tuple)):
			raise QueryException(f"Data types do not match")
		if not self.is_column(*columns):
			raise QueryException(f"Column does not exist. Existing columns in your table {self.ALL_COLUMNS}")
		try:
			self.added(values,columns)
			if not self._query_('INSERT',{self.table:{
				'columns':columns,
				'values':[map(self.to_str ,val) for val in [[i for col, i in v.items() if col in columns] for v in values]]
			}}):
				return False
			for value in values:
				self.loc[self.index.max() + 1] = value
			self.enjoin()
			self._upgraded_()
			return True
		except BaseException as e:
			raise e

	def update(self, items: dict, condition: Condition = Condition()) -> bool:
		"""
		Update existing records in the database based on specified conditions.

		This function modifies the values of specified fields in the database according to the provided 
		items and condition. It returns a boolean indicating the success of the update operation and raises 
		exceptions for any issues encountered during the process.

		Args:
			items (dict): A dictionary containing the fields and their new values to be updated.
			condition (Condition, optional): A condition object to filter which records to update. 
											Defaults to an empty condition.

		Returns:
			bool: True if the records were successfully updated, False otherwise.

		Raises:
			QueryException: If the data types do not match or if the condition is not suitable.
		"""
		if not isinstance(items,dict) and not isinstance(condition,Condition):
			raise QueryException(f"Data types do not match")
		try:
			indexs = self.index
			if not self._query_('UPDATE',{self.table:{k:self.to_str(v) for k,v in items.items() if k in self.ALL_COLUMNS}},condition):
				return False
			if len(condition.functions)>0:
				if not isinstance(condition.course[0],Where):
					raise QueryException("The condition is not suitable. The condition must only be 'WHERE'")
				indexs = condition.functions[0](self).index
			for key, val in items.items():
				self.loc[indexs, key] = val
			self.enjoin()
			self._upgraded_()
			return True
		except BaseException as e:
			raise e

	def delete(self, condition: Condition = Condition()) -> bool:
		"""
		Remove records from the database based on specified conditions.

		This function deletes records that match the provided condition from the database table. 
		It returns a boolean indicating the success of the deletion operation and raises exceptions 
		for any issues encountered during the process.

		Args:
			condition (Condition, optional): A condition object to filter which records to delete. 
											Defaults to an empty condition.

		Returns:
			bool: True if the records were successfully deleted, False otherwise.

		Raises:
			QueryException: If the data types do not match or if the condition is not suitable.
		"""
		if not isinstance(condition,Condition):
			raise QueryException(f"Data types do not match")
		try:
			if not self._query_('DELETE',[self.table], condition):
				return False
			if len(condition.functions)==0:
				self.drop(self.index,inplace=True)
			else:
				if not isinstance(condition.course[0],Where):
					raise QueryException("The condition is not suitable. The condition must only be 'WHERE'")
				indexs = condition.functions[0](self).index
				if type(indexs) is pd.Index:
					self.drop(indexs,inplace=True) 
			self.enjoin()
			self._upgraded_()
			return True
		except BaseException as e:
			raise e

	def get_foreigns(self) -> list:
		"""
		Retrieve foreign key constraints for the database table.

		This function queries the database to obtain a list of foreign key constraints associated 
		with the specified table. It ensures that the latest schema information is reflected before 
		returning the foreign key details.

		Returns:
			list: A list of foreign key constraints for the table.
		"""
		foreigns = self._query_('SHOW_FOREIGN',self.table,self.parent.db_name)
		self._upgraded_()
		return foreigns

	def get_index(self) -> list:
		"""
		Retrieve index information for the database table.

		This function queries the database to obtain a list of indexes associated with the specified 
		table. It ensures that the latest schema information is reflected before returning the index details.

		Returns:
			list: A list of index information for the table.

		Raises:
			Exception: If there is an error during the query operation.
		"""
		try:
			indexs = self._query_('SHOW_INDEX',self.table,self.parent.db_name)
			self._upgraded_()
			return indexs
		except BaseException as e:
			raise e

	def set_index(self, index) -> bool:
		"""
		Create an index for the specified database table.

		This function attempts to create an index based on the provided index definition. 
		It returns a boolean indicating the success of the operation and ensures that the 
		latest schema information is reflected after the index is created.

		Args:
			index: The definition of the index to be created.

		Returns:
			bool: True if the index was successfully created, False otherwise.

		Raises:
			Exception: If there is an error during the index creation process.
		"""
		try:
			if not self._query_('CREATE_INDEX',{self.table:index}):
				return False
			self._upgraded_()
			return True
		except BaseException as e:
			raise e

	def del_index(self, name) -> bool:
		"""
		Delete an index from the specified database table.

		This function attempts to remove an index identified by the given name from the database table. 
		It returns a boolean indicating the success of the operation and ensures that the latest schema 
		information is reflected after the index is deleted.

		Args:
			name: The name of the index to be deleted.

		Returns:
			bool: True if the index was successfully deleted, False otherwise.

		Raises:
			QueryException: If the provided name is not a string or if there is an error during the deletion process.
		"""
		if not isinstance(name,str):
			raise QueryException(f"Data types do not match")
		try:
			if not self._query_('DROP_INDEX',{self.table:name}):
				return False
			self._upgraded_()
			return True
		except BaseException as e:
			raise e

	def rename_table(self, new_name: str) -> bool:
		"""
		Rename the specified database table to a new name.

		This function attempts to change the name of the database table to the provided new name. 
		It returns a boolean indicating the success of the renaming operation.

		Args:
			new_name (str): The new name for the database table.

		Returns:
			bool: True if the table was successfully renamed, False otherwise.
		"""
		if not self.parent.rename_tables(self.table,new_name):
			return False
		return True

	def remove(self) -> bool:
		"""
		Remove the specified database table.

		This function attempts to delete the database table associated with the current instance. 
		It returns a boolean indicating the success of the removal operation.

		Returns:
			bool: True if the table was successfully removed, False otherwise.
		"""
		if not self.parent.remove(self.table):
			return False
		return True

	def add_column(self, column: str, values: list = [], *args) -> bool:
		"""
		Add a new column to the database table with specified values and attributes.

		This function creates a new column in the database table, ensuring that the column name is unique 
		and that the necessary attributes are provided. It returns a boolean indicating the success of the 
		operation and raises exceptions for any issues encountered during the process.

		Args:
			column (str): The name of the column to be added.
			values (list, optional): A list of initial values for the new column. Defaults to an empty list.
			*args: Additional arguments specifying the attributes of the column.

		Returns:
			bool: True if the column was successfully added, False otherwise.

		Raises:
			QueryException: If the column name is not a string, if the column already exists, or if no arguments are provided.
		"""
		if not isinstance(column,str):
			raise QueryException('Column must be string')
		if self.is_column(column):
			raise QueryException(f"Column '{column}' name already exists")
		if len(args)==0:
			raise QueryException('Arguments must not be empty')
		args = map(lambda x: x[0](self.dataTypes,*x[1:]), args)
		if not self._query_('ADD_COLUMN',{self.table:{column:args}}):
			return False
		if not self.add([values],[column]):
			return False
		values.extend(__default_values__(column,args,len(self)-len(values)))
		self[column] = values
		self.enjoin()
		self._upgraded_()
		return True

	def edit_column(self, column: str, **kwargs) -> bool:
		"""
		Modify the attributes of an existing column in the database table.

		This function allows for the alteration of specified properties of a column, such as its data type 
		or constraints. It returns a boolean indicating the success of the operation and raises exceptions 
		for any issues encountered during the process.

		Args:
			column (str): The name of the column to be edited.
			**kwargs: The attributes to modify, provided as keyword arguments.

		Returns:
			bool: True if the column was successfully edited, False otherwise.

		Raises:
			QueryException: If the column name is not a string, if the column does not exist, or if no arguments are provided.
		"""
		if not isinstance(column,str):
			raise QueryException('Column must be string')
		if not self.is_column(column):
			raise QueryException(f"Column '{column}' does not exist")
		if len(kwargs)==0:
			raise QueryException('Arguments must not be empty')
		kwargs = [{'type':k.replace('_',' ').upper(),'value': v(self.dataTypes) if isinstance(v,(partial,types.FunctionType,types.LambdaType,types.MethodType)) else self.to_str(v) } for k,v in kwargs.items()]
		if not self._query_('ALTER_COLUMN',{self.table:{column:kwargs}}):
			return False
		self._upgraded_()
		return True

	def rename_column(self, old_column: str, new_column: str) -> bool:
		"""
		Rename an existing column in the database table.

		This function changes the name of a specified column to a new name, ensuring that the new name 
		does not already exist and adheres to naming rules. It returns a boolean indicating the success 
		of the renaming operation and raises exceptions for any issues encountered during the process.

		Args:
			old_column (str): The current name of the column to be renamed.
			new_column (str): The new name for the column.

		Returns:
			bool: True if the column was successfully renamed, False otherwise.

		Raises:
			QueryException: If the old column does not exist, if the new column name already exists, 
							or if the new name does not match the creation rules.
		"""
		if not self.is_column(old_column):
			raise QueryException(f"Column '{old_column}' does not exist")
		if self.is_column(new_column):
			raise QueryException(f'"{new_column}" name already exists')
		if not __check_variable_name__(new_column):
			raise QueryException(f'"{new_column}" name does not match the creation rules')
		if not self._query_('RENAME_COLUMN', {self.table:{'old_name':old_column,'new_name':new_column}}):
			return False
		column_old = self.get_column(old_column)
		setattr(self,f'tc_{new_column}', column_old)
		delattr(self,f'tc_{old_column}')
		self.rename(columns={old_column: new_column})
		self.enjoin()
		self._upgraded_()
		return True

	def remove_column(self, column: str) -> bool:
		"""
		Remove a specified column from the database table.

		This function deletes a column from the database, ensuring that the column name is valid and exists 
		in the table. It returns a boolean indicating the success of the removal operation and raises 
		exceptions for any issues encountered during the process.

		Args:
			column (str): The name of the column to be removed.

		Returns:
			bool: True if the column was successfully removed, False otherwise.

		Raises:
			QueryException: If the column name is not a string or if the column does not exist.
		"""
		if not isinstance(column,str):
			raise QueryException('Column must be string')
		if not self.is_column(column):
			raise QueryException(f"Column '{column}' does not exist")
		if not self._query_('DROP_COLUMN',{self.table:column}):
			return False
		delattr(self, f'tc_{column}')
		self.drop(column,axis=1)
		self.enjoin()
		self._upgraded_()
		return True

class Column(Series):
	"""
	A class representing a database column, inheriting from Series.

	This class encapsulates the properties and behaviors of a database column, allowing for 
	operations such as setting attributes, managing foreign keys, and performing calculations 
	on the column's data. It provides methods to manipulate the column's characteristics and 
	interact with the parent database structure.

	Args:
		table (str): The name of the table to which the column belongs.
		key (str): The name of the column.
		values (list): The initial values for the column.
		*args: Variable length argument list for additional parameters.
		**kwargs: Arbitrary keyword arguments for additional attributes.

	Attributes:
		_name (str): The name of the column.
		table (str): The name of the table associated with the column.
		parent: The parent database object.
		key (str): The key name of the column.
		column (str): The full column reference in the format 'table.key'.
		__copy_data__ (list): A copy of the initial data for restoration purposes.
		default (callable): A function to retrieve the default value for the column.
		number (callable): A function to retrieve the column number.
		type (callable): A function to retrieve the column type.
		required (callable): A function to check if the column is required.
		is_primary (callable): A function to check if the column is a primary key.
	"""

	def __init__(self, table: str, key: str, values: list, *args, **kwargs):
		"""
		Initialize a Column object representing a database column.

		This constructor sets up the column with its name, associated table, and initial values. 
		It also defines various properties and methods related to the column, such as default values, 
		data types, and whether the column is required or a primary key.

		Args:
			table (str): The name of the table to which the column belongs.
			key (str): The name of the column.
			values (list): The initial values for the column.
			*args: Variable length argument list for additional parameters.
			**kwargs: Arbitrary keyword arguments for additional attributes.

		Attributes:
			_name (str): The name of the column.
			table (str): The name of the table associated with the column.
			parent: The parent database object.
			key (str): The key name of the column.
			column (str): The full column reference in the format 'table.key'.
			__copy_data__ (list): A copy of the initial data for restoration purposes.
			default (callable): A function to retrieve the default value for the column.
			number (callable): A function to retrieve the column number.
			type (callable): A function to retrieve the column type.
			required (callable): A function to check if the column is required.
			is_primary (callable): A function to check if the column is a primary key.
		"""

		def is_noneValue(value):
			return isinstance(value,NoneValue)
		super(Column, self).__init__(values)
		self._name = key
		self.table = table
		self.parent = kwargs.get('parent',None)
		self.key = key
		self.column = f'{self.table}.{self.key}'
		self.__copy_data__ = self.copy()
		self.default = lambda : self.parent.types(self.key).get('default', None)
		self.number = lambda : self.parent.types(self.key).get('number', -1)
		self.type = lambda : self.parent.types(self.key).get('type', None)
		self.required = lambda : self.parent.types(self.key).get('required', False)
		self.is_primary = lambda : self.parent.types(self.key).get('is_primary', False)
		mask = self.apply(is_noneValue)
		indexs = mask[mask].index
		if len(indexs)>0:
			self.drop(indexs, inplace=True)

	def constructor(self, data):
		"""
		Create a new Column object with the specified data.

		This function initializes a new Column instance using the current table and key, along with 
		the provided data. It sets the parent attribute to ensure proper association with the parent 
		database structure.

		Args:
			data: The data to initialize the new Column object with.

		Returns:
			Column: A new Column object initialized with the specified data.
		"""
		return Column(self.table,self.key,data,parent=self.parent)

	def __setattr__(self, key, value):
		"""
		Set an attribute on the object.

		This method allows for the dynamic assignment of attributes to the object by directly 
		updating the instance's dictionary. It enables the flexibility to add or modify attributes 
		at runtime.

		Args:
			key: The name of the attribute to set.
			value: The value to assign to the attribute.

		Returns:
			None
		"""
		self.__dict__[key] = value

	def get_foreign(self) -> list:
		"""
		Retrieve foreign key relationships associated with the column.

		This function collects foreign key constraints that reference the current column and merges 
		the related data from the corresponding tables. It returns a Series containing the merged 
		data for each foreign key relationship.

		Returns:
			list: A Series containing the merged data for the foreign key relationships.

		Raises:
			Exception: If there is an error retrieving or merging the foreign key data.
		"""
		data = {}
		for i in filter(lambda a: a['from']==self.key, self.parent.get_foreigns()):
			table = self.parent.parent.get(i.get('table'))
			data[i['name']]=self.parent.merge(table, left_on=i['from'], right_on=i['to'], how='inner')
		return Series(data)

	def set_foreign(self, name: str, references: list) -> bool:
		"""
		Set a foreign key constraint for the specified column.

		This function establishes a foreign key relationship for the column, ensuring that the 
		specified name is valid and not already in use. It checks that the referenced table has 
		a primary key and returns a boolean indicating the success of the operation.

		Args:
			name (str): The name of the foreign key constraint to be created.
			references (list): A list of tables that the foreign key references.

		Returns:
			bool: True if the foreign key was successfully set, False otherwise.

		Raises:
			QueryException: If the name is not a string, if the name is already taken, if the 
							references are invalid, or if the referenced table is missing a primary key.
		"""
		if self.parent.parent.method in ['sqlite']:
			return False
		if not isinstance(name,str):
			raise QueryException(f'"name" must be a string')
		if not __check_variable_name__(f'{name}'):
			raise QueryException(f"The '{name}' data base name must follow the variable creation rules")
		foreigns = self.parent.get_foreigns()
		if foreigns is not None:
			foreigns = tuple(filter(lambda x: x.get('name')==name,foreigns))
			if len(foreigns)>0:
				raise QueryException(f'The name "{name}" is already taken. Choose another')
		if not isinstance(references,list):
			raise QueryException(f'Table was not found or does not exist')
		prime_key = tuple(dict(filter(lambda x: x[1].get('is_primary',False) ,references.types().items())).keys())
		if len(prime_key)==0:
			raise QueryException(f'Table "{references.table}" is missing a primary key')
		if not self.parent._query_("CREATE_FOREIGN",self.table,self.parent.dataTypes.FOREIGN(self.key,references,name,self.parent)):
			return False
		return True

	def set_default(self, value: Union[str, bool, int, float, list, tuple, dict, datetime, date, time]) -> bool:
		"""
		Set the default value for the column.

		This function assigns a default value to the column, which will be used when no value is provided 
		during data insertion. It returns a boolean indicating the success of the operation.

		Args:
			value (Union[str, bool, int, float, list, tuple, dict, datetime, date, time]): 
				The default value to be set for the column.

		Returns:
			bool: True if the default value was successfully set, False otherwise.
		"""
		return self.parent.edit_column(self.key, default=lambda this: DataTypes.DEFAULT(this,value) )

	def set_type(self, data_type: Union[str, types.LambdaType, types.MethodType, types.FunctionType, partial]) -> bool:
		"""
		Set the data type for the column.

		This function assigns a specific data type to the column, which dictates how the data 
		will be stored and validated. It returns a boolean indicating the success of the operation.

		Args:
			data_type (Union[str, types.LambdaType, types.MethodType, types.FunctionType, partial]): 
				The data type to be set for the column.

		Returns:
			bool: True if the data type was successfully set, False otherwise.
		"""
		return self.parent.edit_column(self.key, data_type=data_type )

	def set_required(self, on: bool) -> bool:
		"""
		Set the required status for the column.

		This function designates whether the column is mandatory for data entries, ensuring that 
		any data inserted into the column must not be null. It returns a boolean indicating the 
		success of the operation.

		Args:
			on (bool): If True, the column is set as required; if False, it is optional.

		Returns:
			bool: True if the required status was successfully set, False otherwise.
		"""
		return self.parent.edit_column(self.key, null=lambda this: DataTypes.NULL(this,on) )

	def set_primary(self, on: bool) -> bool:
		"""
		Set the primary key status for the column.

		This function designates whether the column serves as a primary key in the database, 
		which uniquely identifies each record in the table. It returns a boolean indicating 
		the success of the operation.

		Args:
			on (bool): If True, the column is set as a primary key; if False, it is not.

		Returns:
			bool: True if the primary key status was successfully set, False otherwise.
		"""
		return self.parent.edit_column(self.key, primary_key=lambda this: DataTypes.PRIMARY(this,on) )

	def rename(self, new_name: str) -> bool:
		"""
		Rename the column to a new specified name.

		This function changes the name of the current column to the provided new name, ensuring 
		that the new name adheres to the naming rules. It returns a boolean indicating the success 
		of the renaming operation.

		Args:
			new_name (str): The new name for the column.

		Returns:
			bool: True if the column was successfully renamed, False otherwise.
		"""
		return self.parent.rename_column(self.key,new_name)

	def edit(self, **kwargs) -> bool:
		"""
		Modify the attributes of the current column.

		This function allows for the dynamic editing of the column's properties by passing 
		keyword arguments that specify the attributes to be changed. It returns a boolean indicating 
		the success of the operation.

		Args:
			**kwargs: The attributes to modify, provided as keyword arguments.

		Returns:
			bool: True if the column was successfully edited, False otherwise.
		"""
		return self.parent.edit_column(self.key,**kwargs)

	def remove(self) -> bool:
		"""
		Remove the current column from the database table.

		This function attempts to delete the column associated with the current instance from the 
		database table. It returns a boolean indicating the success of the removal operation.

		Returns:
			bool: True if the column was successfully removed, False otherwise.
		"""
		return self.parent.remove_column(self.key)

	def clearing(self) -> bool:
		"""
		Clear the values of the current column in the database.

		This function sets the values of the column to a NoneValue, effectively clearing the data 
		while ensuring that the column is not required or a primary key. It returns a boolean indicating 
		the success of the clearing operation.

		Returns:
			bool: True if the column values were successfully cleared, False otherwise.

		Raises:
			QueryException: If the column is required or is a primary key, preventing it from being cleared.
		"""
		default = self.default()
		if (self.required() and default is None) or self.is_primary():
			raise QueryException(f'The "{self.key}" column cannot be cleared because it is required')
		if not self.parent.update({self.key:default}):
			return False
		data = self.parent
		self.parent.clear()
		self.parent.extend(map(lambda x: {**x,self.key:NoneValue}, data))
		self.parent.enjoin()
		self.parent._upgraded_()
		return True

	def restore(self):
		"""
		Restore the column to its original state.

		This function clears the current values of the column and then repopulates it with the 
		initial data that was copied during the column's creation. It effectively undoes any 
		changes made to the column since its initialization.

		Returns:
			None
		"""
		self.clear()
		self.extend(self.__copy_data__)

	def get(self, attr, *args, **kwargs):
		"""
		Retrieve the value of a specified attribute from the object.

		This function checks if the requested attribute exists and returns its value, allowing for 
		optional arguments to be passed to the attribute's method if applicable. If the attribute 
		does not exist, it raises a QueryException.

		Args:
			attr (str): The name of the attribute to retrieve.
			*args: Additional positional arguments to pass to the attribute's method.
			**kwargs: Additional keyword arguments to pass to the attribute's method.

		Returns:
			The value of the specified attribute.

		Raises:
			QueryException: If the specified attribute does not exist.
		"""
		if attr == 'count':
			attr = 'length'
		if hasattr(self,attr):
			return getattr(self,attr)(*args,**kwargs)
		raise QueryException(f'Attribute "{attr}" does not exist')

	def __run_function__(self, fun, condition) -> list:
		"""
		Execute a specified function on the object's data, optionally filtering the data based on a condition.

		This method checks if the provided condition is a valid lambda function and applies it to filter 
		the object's data before executing the specified function. It returns the result of the function 
		execution, either on the filtered data or on the entire dataset if no condition is provided.

		Args:
			fun: The function to be executed on the data.
			condition: An optional lambda function used to filter the data before applying the specified function.

		Returns:
			list: The result of the function execution on the data.

		Raises:
			QueryException: If the condition is not a valid lambda function or None.
		"""
		if not isinstance(condition,(types.LambdaType, type(None))):
			raise QueryException(f"Data types do not match")
		if condition is not None:
			return fun(list(filter(condition, self)))
		return fun(self)

	def max(self, condition: types.LambdaType = None, sql: bool = False) -> Union[int, str]:
		"""
		Calculate the maximum value of the column's data.

		This function returns the maximum value from the data in the specified column, optionally 
		applying a filter condition. If the SQL flag is set to True, it returns the SQL syntax for 
		calculating the maximum value instead of executing the calculation.

		Args:
			condition (types.LambdaType, optional): An optional lambda function to filter the data.
			sql (bool, optional): If True, returns the SQL syntax for the MAX function. Defaults to False.

		Returns:
			Union[int, str]: The maximum value of the column's data or the SQL syntax if sql is True.
		"""
		return f'''MAX({self.column})''' if sql else self.__run_function__(max,condition)

	def min(self, condition: types.LambdaType = None, sql: bool = False) -> Union[int, str]:
		"""
		Calculate the minimum value of the column's data.

		This function returns the minimum value from the data in the specified column, with the option 
		to apply a filter condition. If the SQL flag is set to True, it returns the SQL syntax for 
		calculating the minimum value instead of executing the calculation.

		Args:
			condition (types.LambdaType, optional): An optional lambda function to filter the data.
			sql (bool, optional): If True, returns the SQL syntax for the MIN function. Defaults to False.

		Returns:
			Union[int, str]: The minimum value of the column's data or the SQL syntax if sql is True.
		"""
		return f'''MIN({self.column})''' if sql else self.__run_function__(min,condition)

	def length(self, condition: types.LambdaType = None, sql: bool = False) -> Union[int, str]:
		"""
		Calculate the number of entries in the column's data.

		This function returns the count of values in the specified column, with the option to apply 
		a filter condition. If the SQL flag is set to True, it returns the SQL syntax for counting 
		the entries instead of executing the calculation.

		Args:
			condition (types.LambdaType, optional): An optional lambda function to filter the data.
			sql (bool, optional): If True, returns the SQL syntax for the COUNT function. Defaults to False.

		Returns:
			Union[int, str]: The count of entries in the column's data or the SQL syntax if sql is True.
		"""
		return f'''COUNT({self.column})''' if sql else self.__run_function__(len,condition)

	def sum(self, condition: types.LambdaType = None, sql: bool = False) -> Union[int, float, str]:
		"""
		Calculate the total sum of the values in the column's data.

		This function returns the sum of the values in the specified column, with the option to apply 
		a filter condition. If the SQL flag is set to True, it returns the SQL syntax for calculating 
		the sum instead of executing the calculation.

		Args:
			condition (types.LambdaType, optional): An optional lambda function to filter the data.
			sql (bool, optional): If True, returns the SQL syntax for the SUM function. Defaults to False.

		Returns:
			Union[int, float, str]: The total sum of the column's values or the SQL syntax if sql is True.
		"""
		return f'''SUM({self.column})''' if sql else self.__run_function__(sum,condition)

	def avg(self, condition: types.LambdaType = None, sql: bool = False) -> Union[int, float, str]:
		"""
		Calculate the average of the values in the column's data.

		This function returns the average of the values in the specified column, with the option to apply 
		a filter condition. If the SQL flag is set to True, it returns the SQL syntax for calculating 
		the average instead of executing the calculation.

		Args:
			condition (types.LambdaType, optional): An optional lambda function to filter the data.
			sql (bool, optional): If True, returns the SQL syntax for the AVG function. Defaults to False.

		Returns:
			Union[int, float, str]: The average of the column's values or the SQL syntax if sql is True.
		"""
		return f'''AVG({self.column})''' if sql else self.__run_function__(lambda arr:sum(arr)/len(arr),condition)

	def round(self, k: int, condition: types.LambdaType = None, sql: bool = False) -> Union[list, str]:
		"""
		Round the values in the column's data to a specified number of decimal places.

		This function rounds each value in the specified column to the nearest decimal place defined 
		by the parameter `k`, with the option to apply a filter condition. If the SQL flag is set to 
		True, it returns the SQL syntax for rounding instead of executing the rounding operation.

		Args:
			k (int): The number of decimal places to round to.
			condition (types.LambdaType, optional): An optional lambda function to filter the data.
			sql (bool, optional): If True, returns the SQL syntax for the ROUND function. Defaults to False.

		Returns:
			Union[list, str]: A list of rounded values or the SQL syntax if sql is True.
		"""
		return f'''ROUND({self.column},{k})''' if sql else self.__run_function__(lambda arr:list(map(lambda x: round(x,k), arr)),condition)

	def random(self, k: int, condition: types.LambdaType = None) -> list:
		"""
		Retrieve a random selection of values from the column's data.

		This function returns a list of randomly selected values from the specified column, with the 
		number of selections determined by the parameter `k`. An optional condition can be applied to 
		filter the data before the random selection is made.

		Args:
			k (int): The number of random values to retrieve.
			condition (types.LambdaType, optional): An optional lambda function to filter the data.

		Returns:
			list: A list of randomly selected values from the column's data.
		"""
		return self.__run_function__(lambda arr: random.choices(arr, k=k), condition)

	def random_one(self, condition: types.LambdaType = None) -> list:
		"""
		Retrieve a single random value from the column's data.

		This function returns one randomly selected value from the specified column, with the option 
		to apply a filter condition to limit the selection. If a condition is provided, it will filter 
		the data before making the random selection.

		Args:
			condition (types.LambdaType, optional): An optional lambda function to filter the data.

		Returns:
			list: A list containing the randomly selected value from the column's data.
		"""
		return self.__run_function__(lambda arr: random.choice(arr), condition)

	def shuffle(self, n: int = 1, condition: types.LambdaType = None) -> list:
		"""
		Shuffle the values in the column's data a specified number of times.

		This function randomly rearranges the order of the values in the specified column, 
		repeating the shuffle operation `n` times. An optional condition can be applied to filter 
		the data before shuffling.

		Args:
			n (int, optional): The number of times to shuffle the data. Defaults to 1.
			condition (types.LambdaType, optional): An optional lambda function to filter the data.

		Returns:
			list: A list of the shuffled values from the column's data.
		"""
		data = self.__run_function__(lambda arr: arr, condition)
		n = abs(int(n))
		for _ in range(n):
			random.shuffle(data)
		return data

	def mult(self, condition: types.LambdaType = None) -> Union[int, float]:
		"""
		Calculate the product of the values in the column's data.

		This function multiplies all values in the specified column, with the option to apply 
		a filter condition to limit the data included in the calculation. It returns the total 
		product of the values.

		Args:
			condition (types.LambdaType, optional): An optional lambda function to filter the data.

		Returns:
			Union[int, float]: The product of the column's values or 0 if no values are present.
		"""
		return self.__run_function__(lambda arr: reduce(lambda x, y: x * y, arr), condition)

	def diff(self, condition: types.LambdaType = None) -> Union[int, float]:
		"""
		Calculate the difference of the values in the column's data.

		This function computes the result of subtracting all values in the specified column, 
		with the option to apply a filter condition to limit the data included in the calculation. 
		It returns the total difference of the values.

		Args:
			condition (types.LambdaType, optional): An optional lambda function to filter the data.

		Returns:
			Union[int, float]: The difference of the column's values or 0 if no values are present.
		"""
		return self.__run_function__(lambda arr: reduce(lambda x, y: x - y, arr), condition)

	def quot(self, condition: types.LambdaType = None) -> Union[int, float]:
		"""
		Calculate the quotient of the values in the column's data.

		This function computes the result of dividing all values in the specified column, 
		with the option to apply a filter condition to limit the data included in the calculation. 
		It returns the total quotient of the values.

		Args:
			condition (types.LambdaType, optional): An optional lambda function to filter the data.

		Returns:
			Union[int, float]: The quotient of the column's values or 0 if no values are present.
		"""
		return self.__run_function__(lambda arr: reduce(lambda x, y: x / y, arr), condition)

	def filter(self, func: types.FunctionType = None) -> list:
		"""
		Filter the values in the column's data based on a specified function.

		This function applies the provided filtering function to the values in the specified column, 
		returning a list of values that meet the criteria defined by the function. If no function is 
		provided, it returns all values in the column.

		Args:
			func (types.FunctionType, optional): A function to determine which values to include in the result.

		Returns:
			list: A list of filtered values from the column's data.
		"""
		return self.__run_function__(lambda arr: filter(func, arr) , None)

	def map(self, func: types.FunctionType, condition: types.LambdaType = None) -> list:
		"""
		Apply a specified function to each value in the column's data.

		This function maps the provided function to the values in the specified column, returning 
		a list of results. An optional condition can be applied to filter the data before the mapping.

		Args:
			func (types.FunctionType): The function to apply to each value in the column.
			condition (types.LambdaType, optional): An optional lambda function to filter the data.

		Returns:
			list: A list of results after applying the function to the column's data.
		"""
		return self.__run_function__(lambda arr: map(func, arr) , condition)

	def enumerate(self, key: types.FunctionType = None, reverse: bool = False, condition: types.LambdaType = None) -> list:
		"""
		Enumerate the values in the column's data, optionally applying a filter.

		This function returns a list of tuples containing the index and value of each item in the 
		specified column, with the option to reverse the order or apply a filtering condition. 
		It provides a way to access both the index and the corresponding value for further processing.

		Args:
			key (types.FunctionType, optional): An optional function to apply to the keys.
			reverse (bool, optional): If True, reverses the order of the enumeration. Defaults to False.
			condition (types.LambdaType, optional): An optional lambda function to filter the data.

		Returns:
			list: A list of tuples containing the index and value of each item in the column's data.
		"""
		return self.__run_function__(enumerate, condition)

	def mirror(self, condition: types.LambdaType = None) -> list:
		"""
		Reverse the order of the values in the column's data.

		This function returns a list of the column's values in reverse order, with the option to 
		apply a filtering condition to limit the data before reversing. It provides a simple way to 
		access the data in a mirrored format.

		Args:
			condition (types.LambdaType, optional): An optional lambda function to filter the data.

		Returns:
			list: A list of the column's values in reversed order.
		"""
		return self.__run_function__(lambda arr: arr[::-1], condition)

	def power(self, x: Union[int, float] = 2, condition: types.LambdaType = None) -> Union[int, float]:
		"""
		Raise the values in the column's data to a specified power.

		This function computes the result of raising each value in the specified column to the power 
		of `x`, with the option to apply a filter condition to limit the data included in the calculation. 
		It returns the modified values after applying the power operation.

		Args:
			x (Union[int, float], optional): The exponent to which the column's values will be raised. Defaults to 2.
			condition (types.LambdaType, optional): An optional lambda function to filter the data.

		Returns:
			Union[int, float]: The modified values of the column raised to the specified power.
		"""
		return self.__run_function__(lambda arr: arr**x, condition)

	def join(self, t: str = '', condition: types.LambdaType = None) -> str:
		"""
		Join the values in the column's data into a single string.

		This function concatenates the values in the specified column using a specified delimiter, 
		with the option to apply a filter condition to limit the data included in the joining process. 
		It returns the resulting string after joining the values.

		Args:
			t (str, optional): The string delimiter to use for joining the values. Defaults to an empty string.
			condition (types.LambdaType, optional): An optional lambda function to filter the data.

		Returns:
			str: A string containing the joined values from the column's data.
		"""
		return self.__run_function__(lambda arr: t.join(arr), condition)

