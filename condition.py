import re
import random
from datetime import *
import types
from typing import Union
from functools import reduce, partial
import numpy as np
import pandas as pd

def check_number_type(input_str):
	"""
	 Checks if input_str is a number. If it is it will return it as an int.
	 
	 @param input_str - The string to check.
	 
	 @return The number or the string itself if it is not
	"""
	# Return the number of digits in the input string.
	if input_str.isdigit():
		return int(input_str)
	parts = list(filter(None,input_str.split('.')))
	# Returns the float value of the first part of the input string.
	if len(parts) == 2 and all(part.isdigit() for part in parts):
		return float(input_str)
	return input_str

class QueryException(Exception):

	def __init__(self,message):
		"""
		 Initializes the exception with a message. This is used to provide error messages to the user
		 
		 @param message - The message to be
		"""
		self.message = message

	def __str__(self):
		"""
		 Returns the message of the error. This is useful for debugging. If you want to print the error message use : py : meth : ` __str__ ` instead.
		 
		 
		 @return The error message as a string or None if there was no error
		"""
		return self.message

class NoneValue:
	def __init__(self):
		"""
		 Initialize the object. This is called by __init__ and should not be called directly
		"""
		pass

class Like:
	def __init__(self, column:str, params:str, operator:str='', _not:bool=False):
		"""
		 Creates a : class : ` _expression. Query ` object. This is the constructor for
		 
		 @param column - The column to search on
		 @param params - The parameters to search for
		 @param operator - The operator to use in the query default is''
		 @param _not - Whether or not to use NOT instead of
		"""
		operator = operator.lower().strip()
		# Raise an exception if the operator is not in the empty string or contain and or or.
		if operator not in ['','and', 'or']:
			raise QueryException("The operator must be the empty string or contain 'and' or 'or'")
		self.params = f'''{operator.upper()} {'NOT' if _not else ''} {column} LIKE '{params}' '''.strip()
		pattern = re.sub(r'(?<!\\)[_%]', lambda x: '.' if x.group(0) == '_' else '.*', params)
		# If pattern starts with a or if it starts with a regular expression it is added to the pattern.
		if not pattern.startswith('^'):		pattern = '^' + pattern
		# If pattern is not a valid pattern add a.
		if not pattern.endswith('$'):		pattern = pattern + '$'
		self.func = f'''{operator} {'not' if _not else ''} {column}.str.contains({pattern})'''.strip()

	def __str__(self):
		"""
		 Returns a string representation of the parameter set. This is useful for debugging and to avoid having to re - generate the string every time it is called.
		 
		 
		 @return A string representation of the parameter set in the format used by L { __str__ }
		"""
		return self.params

class WhereHaving:

	def __init__(self, col, params:types.LambdaType, like:Like=None, _not:bool=False, **kwargs):
		"""
		 Initializes the query. This method is called by __init__ and should not be called directly.
		 
		 @param col - The : class : `. Column ` to query.
		 @param params - The parameters to use for the query. If this is a string it is treated as a sequence of words.
		 @param like - The like object to use for
		 @param _not
		"""
		# If the data type is not of type Like or None raises a QueryException.
		if not isinstance(like,(type(None),Like)):
			raise QueryException(f'The "like" data type must be {type(Like)} or None')

		self.col = col
		self.table = col.table
		self.key = col.key
		self.params = f'''{self.change_words(params)}'''
		self.func = repr(f'''({'not' if _not else ''} ( {self.change_values(self.params)} {self.set_like(like,True)} ))''')
		self.params = f'''{'NOT' if _not else ''} ({self.params} {self.set_like(like)})'''

	def __str__(self):
		"""
		 Returns a string representation of the parameter set. This is useful for debugging and to avoid having to re - generate the string every time it is called.
		 
		 
		 @return A string representation of the parameter set in the format used by L { __str__ }
		"""
		return self.params

	def set_like(self,like,value=False) -> str:
		"""
		 Set the like function. This is used to make an expression like a parameter or a function
		 
		 @param like - The expression to set the like function
		 @param value - If True return the value. If False return the parameters
		 
		 @return A string that can be used as a parameter or
		"""
		# Returns the function or params if value is not None.
		if like is not None:
			return like.func if value else like.params
		return ''

	def convert_operators(self,input_string,pattern):
		"""
		 Converts operators in a string to the correct format. This is used to convert operator in SQL statements that are not supported by Python's built - in operators such as < > = etc.
		 
		 @param input_string - The string to convert operators in.
		 @param pattern - The pattern to use for converting operators. See documentation for this class for details.
		 
		 @return The converted string with operators converted to the correct format
		"""
		regex = re.compile(r"('[^']*'|\"[^\"]*\")|({})".format(pattern))
		def replace(match):
			"""
			 Replace operators in a match. This is a callback for re. sub that takes a match and returns the replacement text
			 
			 @param match - The match to be replaced
			 
			 @return The replacement text for the match or the original if no replacement
			"""
			# Returns the operator of the match.
			if match.group(1):
				return match.group(1)
			else:
				operator = match.group(2)
				# Convert the operator to a string.
				if operator == "==":
					return "="
				elif operator == "!=":
					return "<>"
				elif operator == "=":
					return "=="
				elif operator == "<>":
					return "!="
				else:
					return operator
		return regex.sub(replace, input_string)

	def change_words(self, query) -> str:
		"""
		 Changes words in query to upper case. This is useful for converting queries that are intended to be in a more human readable form e. g.
		 
		 @param query - query to be converted to upper case. It is assumed that query is a string of SQL statements.
		 
		 @return query with words converted to upper case. The words are returned as a
		"""
		pattern = r'\b(?:\w+|\d+)\b(?=(?:[^"\']*"[^"\']*"[^"\']*)*(?:[^"\']|$))'
		def replace_word(match):
			"""
			 Replace words in query. This is a callback for re. sub to replace the word that was matched with a query
			 
			 @param match - The match object returned by re. sub
			 
			 @return The word that was matched with the query or NULL if none
			"""
			word = match.group(0)
			# Returns the lowercase version of the word.
			if word.lower() in ['and', 'or', 'not', 'in', 'is', 'true', 'false']:
				return word.upper()
			elif word.lower() == 'none':
				return 'NULL'
			return word
		return self.convert_operators(self.replace_brackets(re.sub(pattern, replace_word, query)),r"(==|!=|<=|>=|<|>)")

	def replace_brackets(self,text) -> str:
		"""
		 Replace brackets with parentheses. This is used to make sure a user doesn't accidentally get confused with an unusual syntax.
		 
		 @param text - The text to process. Should be a string of any length.
		 
		 @return The text with brackets replaced with parentheses. >>> text ='This is my text
		"""
		return re.sub(r'"[^"]*"|\'[^\']*\'|\[([^\[\]]*)\]', lambda x: '(' + x.group(1) + ')' if x.group(1) else x.group(0), text)

	def change_func(self,query) -> str:
		"""
		 Replace function names in query with column names. This is useful for functions that need to be defined in the column's definition e. g.
		 
		 @param query - query to search for functions in. The function names are separated by spaces and each word is a function name followed by a list of parameters.
		 
		 @return query with functions replaced with column names. If there is no function with the same name in the column it is returned as is
		"""
		pattern = r'\b(\w+)\(([^)]*)\)'
		def replace_word(match):
			"""
			 Replace a word in the col. This is a callback for re. sub.
			 
			 @param match - The match to replace. Must be of the form.
			 
			 @return The replacement for the word or the original if no replacement was made
			"""
			word = match.group(0)
			matches = re.findall(pattern, word)
			result = []
			# Returns a list of function names and parameters for each match.
			for match in matches:
				function_name = match[0]
				parameters = list(filter(None,map(check_number_type ,match[1].split(','))))
				result = [function_name] + parameters
			# Returns the value of the column.
			if len(result)>1:
				# Returns the value of the column in the result string.
				if self.col.column == result[1]:
					return str(self.col.get(result[0].lower(),*result[2:]))
			return word
		return re.sub(pattern, replace_word, query)

	def change_values(self, query) -> str:
		"""
		 Convert values to key / value pairs. This is used to convert SQL queries that are intended to be used as part of a WHERE clause.
		 
		 @param query - The query to convert. It can be a string or a list of strings.
		 
		 @return The query converted to string or list of strings depending on the type of query
		"""
		pattern = r'\b(?:\w+\.?\w*|\d+)\b(?=(?:[^"\']*"[^"\']*"[^"\']*)*(?:[^"\']|$))'
		def replace_word(match):
			"""
			 Replace words in column keys. This is used to replace the key part of the column key with the word that should be used for the search.
			 
			 @param match - The match object from re. search (... )
			 
			 @return The word to search for in the column key or
			"""
			word = match.group(0)
			# Returns the lowercase version of the word.
			if word.lower() == self.col.column.lower():
				return self.key
			elif word.lower() in ['in', 'is','and','or','not']:
				return word.lower()
			elif word.lower() in ['true', 'false']:
				return word.title()
			elif word.lower() == 'null':
				return 'None'
			return word
		return re.sub(pattern, replace_word, self.change_func(self.convert_operators(query,r"(=|<>|<=|>=|<|>)")))

class Where(WhereHaving):

	def __init__(self, *args, **kwargs):
		"""
		 Initialize the where object. This is the method that will be called by the database
		"""
		super(Where,self).__init__(*args, **kwargs)

class GroupBy:
	def __init__(self, *columns):
		"""
		 Initialize query by columns. Args : columns ( list ) : List of columns
		"""
		# Raise QueryException if columns is empty.
		if len(columns)==0:
			raise QueryException('Columns must not be empty')
		self.columns = list(set(map(lambda x: x if type(x) is str else x.key, columns)))
		self.params = f'''GROUP BY {','.join(set(map(lambda x: x if type(x) is str else x.column, columns)))}'''.strip()
		self.func = lambda x: x.constructor(x.groupby(self.columns).first().reset_index()[self.columns])
		# self.func = lambda x: [dict(b) for b in [set(tuple(filter(lambda x: x[0] in self.columns, d.items()))) for d in x]]
	def __str__(self):
		"""
		 Returns a string representation of the parameter set. This is useful for debugging and to avoid having to re - generate the string every time it is called.
		 
		 
		 @return A string representation of the parameter set in the format used by L { __str__ }
		"""
		return self.params

class Having(WhereHaving):
	def __init__(self, *args, **kwargs):
		"""
		 Overridden to ensure that Having is not called in __init__. This is necessary because the SQLAlchemy dialect doesn't support SQLAlchemy
		"""
		super(Having,self).__init__(*args, **kwargs)

class OrderBy:
	def __init__(self, column:Union[str,list], reverse:bool=False):
		"""
		 Initialize query by column. This method is used by : meth : ` Query. __init__ ` and
		 
		 @param column - Column name or Series to sort by
		 @param reverse - Sort ascending or
		"""
		# Raise QueryException if column data type is str or Columns
		if not isinstance(column,(str,pd.Series)):
			raise QueryException(f'column data type must be "str" or "Columns"')
		# Convert a pandas Series to a column name.
		if isinstance(column,pd.Series):
			col = f'{column.table}.{column.key}'
			column = column.key
		elif isinstance(column,str):
			col = column
		self.params = f'''ORDER BY {col} {'DESC' if reverse else ''}'''.strip()
		self.func = lambda x: x.constructor(x.sort_values(column, ascending=reverse))
	def __str__(self):
		"""
		 Returns a string representation of the parameter set. This is useful for debugging and to avoid having to re - generate the string every time it is called.
		 
		 
		 @return A string representation of the parameter set in the format used by L { __str__ }
		"""
		return self.params

class LimitOffset:
	def __init__(self, limit:int=0, offset:int=0):
		"""
		 Creates a query to fetch records. This is the method that will be called by __init__ and should not be called directly
		 
		 @param limit - number of records to fetch
		 @param offset - number of records to skip before returning the rest
		"""
		limit = abs(int(limit))
		offset = abs(int(offset))
		self.params = ''
		# limit is the number of results to return
		if limit>0:					self.params += f'''LIMIT {limit} '''
		# offset is the offset of the current offset
		if offset>0:				self.params += f'''OFFSET {offset} '''
		self.params =				self.params.strip()
		# limit and offset are the number of items in the range 0 limit offset limit
		if limit>0 and offset>=0:	self.func = lambda x: x.constructor(x[offset:offset+limit])
		elif limit==0 and offset>0:	self.func = lambda x: x.constructor(x[offset:])
		else:						self.func = lambda x: x
	def __str__(self):
		"""
		 Returns a string representation of the parameter set. This is useful for debugging and to avoid having to re - generate the string every time it is called.
		 
		 
		 @return A string representation of the parameter set in the format used by L { __str__ }
		"""
		return self.params

class Join:

	__hows__ = {
		'inner':'INNER JOIN',
		'right':'RIGHT JOIN',
		'left':'LEFT JOIN',
		'outer':'FULL OUTER JOIN',
	}
	
	def __init__(self, table1, table2, column:str, how:str, NaN:bool=False):
		"""
		 Initializes the class with the information to be used in the select statement. This is the method that will be called by the class when it is created
		 
		 @param table1 - The table in which the column is to be selected
		 @param table2 - The table in which the column is to be selected
		 @param column - The column in the table that is to be selected
		 @param how - The how of the selection ( select_ * )
		 @param NaN - A boolean indicating if the column is a NaN
		"""
		# Raise a QueryException if the how attribute does not exist.
		if how not in self.__hows__.keys():
			raise QueryException(f'The "how" attribute does not exist. Select something you need from this list {list(self.hows.keys())}')
		# Raise a QueryException if tables are not in the database.
		if not isinstance(table1,pd.DataFrame) and not isinstance(table2,pd.DataFrame):
			raise QueryException(f'Tables must exist in the database')
		# Raise exception if column is missing in table1. table
		if not table1.is_column(column):
			raise QueryException(f'Column "{column}" is missing in table "{table1.table}"')
		# Raise a QueryException if column is missing in table2. table
		if not table2.is_column(column):
			raise QueryException(f'Column "{column}" is missing in table "{table2.table}"')
		self.table1 = table1
		self.table2 = table2
		self.column = column
		self.how = how
		self.NaN = NaN
		self.params = f'''{self.__hows__[self.how]} {self.table2.table} ON {self.table1.get_column(column).column}={self.table2.get_column(column).column}'''

	def func(self,*args) -> dict:
		"""
		 Merge two DataFrames and return a dict. This is a wrapper for DataFrame. merge that does not handle NaNs.
		 
		 
		 @return A dict with the result of the merge. Keys are the column names
		"""
		merge = pd.merge(self.table1.values(), self.table2.values(), on=self.column, how=self.how)
		# Replace NaN with NoneValue.
		if self.NaN:
			merge = merge.fillna('NaN')
			merge = merge.replace('NaN', NoneValue)
		return self.table1.constructor(merge)


class Condition:

	def __init__(self,table=None,*tables):
		"""
		 Initialize the class with a table to search. This is a helper for : meth : ` __init__ `
		 
		 @param table - The table to search
		"""
		self.parameters = ''
		self.functions = []
		self.course = []
		self.course_join = []
		self.correct_course = Where,GroupBy,Having,OrderBy
		self.table = table
		self.tables = tables

	def __str__(self):
		"""
		 Returns a string representation of the parameter. This is useful for debugging and to avoid having to re - generate the string every time it is called.
		 
		 
		 @return A string representation of the parameter ( s ) that were
		"""
		return self.parameters

	def check(self):
		"""
		 Check the order of the conditions is correct Raises QueryException If the order of the conditions is
		"""
		self.course_join.extend(self.correct_course)
		isk_rem = filter(lambda x: any([isinstance(curs,x) for curs in self.course]),self.course_join)
		# Raise a QueryException if the order of the conditions is not correct.
		for curs in zip(self.course,isk_rem):
			# Raise a QueryException if the order of the conditions is not correct.
			if not isinstance(curs[0],(curs[1], LimitOffset)):
				raise QueryException('The order of the conditions is not correct')

	def where(self,items:dict,options:list=[]):
		"""
		 Build the where part of the query. It is used to get the data from the database
		 
		 @param items - The dictionary with the items
		 @param options - The options for the
		"""
		# Raise a QueryException if the number of parameters does not match the number of parameters.
		if len(items)-1 != len(options):
			raise QueryException('The number of parameters does not match the values')
		self.parameters += ' WHERE '
		func_where = 'lambda x: x.constructor(x.query( '
		# Add a where clause to the query.
		for key, value in items.items():
			# Raise a QueryException if the key column does not exist.
			if self.table is not None:
				# Raise a QueryException if the column with the given key does not exist.
				if not self.table.is_column(key):
					raise QueryException(f'The "{key}" column does not exist')
				key = self.table.get_column(key)
			val = value(key,sql=True)
			# Raise a QueryException if the data type is not Where.
			if not isinstance(val,Where):
				raise QueryException(f'The data type must be "{type(Where)}"')
			self.parameters += val.params
			func_where += val.func
			# Add a query string to the query string.
			if len(options)>0:
				self.parameters += ' '+options[0].upper()+' '
				func_where += ' '+('&' if options[0].lower().strip()=='and' else '|')+' '
				options.pop(0)
		func_where += ' ))'
		self.functions.append(eval(func_where))
		self.course.append(val)
		self.check()

	def having(self,items:dict,options:list=[]):
		"""
		 Add HAVING clause to the query. It is possible to specify conditions by using a dictionary where clause.
		 
		 @param items - dictionary where keys are column names and values are functions that take a column as input and return a boolean
		 @param options - list of options for
		"""
		# Raise a QueryException if the number of parameters does not match the number of parameters.
		if len(items)-1 != len(options):
			raise QueryException('The number of parameters does not match the values')
		self.parameters += ' HAVING '
		func_having = 'lambda x: x.constructor(x.query( '
		# Add a where clause to the query.
		for key, value in items.items():
			# Raise a QueryException if the key column does not exist.
			if self.table is not None:
				# Raise a QueryException if the column with the given key does not exist.
				if not self.table.is_column(key):
					raise QueryException(f'The "{key}" column does not exist')
				key = self.table.get_column(key)
			val = value(key,sql=True)
			# Raise a QueryException if the data type is not Where.
			if not isinstance(val,Where):
				raise QueryException(f'The data type must be "{type(Where)}"')
			self.parameters += val.params
			func_where += val.func
			# Add a query string to the query string.
			if len(options)>0:
				self.parameters += ' '+options[0].upper()+' '
				func_where += ' '+('&' if options[0].lower().strip()=='and' else '|')+' '
				options.pop(0)
		func_having += ' ))'
		self.functions.append(eval(func_having))
		self.course.append(val)
		self.check()

	def orderBy(self,ob:OrderBy):
		"""
		 Order the results by a given object. It is possible to use this method multiple times in a course but the order may be different
		 
		 @param ob - The object to order
		"""
		# Raise an exception if the data type is not order by.
		if not isinstance(ob,OrderBy):
			raise QueryException(f'The data type must be "{type(OrderBy)}"')
		self.parameters += ' '+ob.params
		self.functions.append(ob.func)
		self.course.append(ob)
		self.check()

	def limit_offset(self,lo:LimitOffset):
		"""
		 Add a LimitOffset to the query. This is a convenience method for adding an object to the query.
		 
		 @param lo - The LimitOffset to add to the query. It must be of type Limit
		"""
		# Raise an exception if the data type is not LimitOffset.
		if not isinstance(lo,LimitOffset):
			raise QueryException(f'The data type must be "{type(LimitOffset)}"')
		self.parameters += ' '+lo.params
		self.functions.append(lo.func)
		self.course.append(lo)
		self.check()

	def groupBy(self,gb:GroupBy):
		"""
		 Add a group by to the query. It is assumed that the data is grouped by an instance of
		 
		 @param gb - The group by to
		"""
		# Raise a QueryException if the data type is not GroupBy.
		if not isinstance(gb,GroupBy):
			raise QueryException(f'The data type must be "{type(GroupBy)}"')
		self.parameters += ' '+gb.params
		self.functions.append(gb.func)
		self.course.append(gb)
		self.check()

	def join(self,jo:Join):
		"""
		 Add a join to the query. It is possible to have multiple joins in a Course
		 
		 @param jo - The data to add
		"""
		# Raise an exception if the data type is not Join.
		if not isinstance(jo,Join):
			raise QueryException(f'The data type must be "{type(Join)}"')
		self.parameters += ' '+jo.params
		self.functions.append(jo.func)
		self.course_join.append(Join)
		self.course.append(jo)
		self.check()