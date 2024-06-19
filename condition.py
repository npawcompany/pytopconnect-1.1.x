import re
import random
from datetime import *
import types
from typing import Union
from functools import reduce, partial
import numpy as np
import pandas as pd

def check_number_type(input_str):
	if input_str.isdigit():
		return int(input_str)
	parts = list(filter(None,input_str.split('.')))
	if len(parts) == 2 and all(part.isdigit() for part in parts):
		return float(input_str)
	return input_str

class QueryException(Exception):

	def __init__(self,message):
		self.message = message

	def __str__(self):
		return self.message

class NoneValue:
	def __init__(self):
		pass

class Like:
	def __init__(self, column:str, params:str, operator:str='', _not:bool=False):
		operator = operator.lower().strip()
		if operator not in ['','and', 'or']:
			raise QueryException("The operator must be the empty string or contain 'and' or 'or'")
		self.params = f'''{operator.upper()} {'NOT' if _not else ''} {column} LIKE '{params}' '''.strip()
		pattern = re.sub(r'(?<!\\)[_%]', lambda x: '.' if x.group(0) == '_' else '.*', params)
		if not pattern.startswith('^'):		pattern = '^' + pattern
		if not pattern.endswith('$'):		pattern = pattern + '$'
		self.func = f'''{operator} {'not' if _not else ''} {column}.str.contains({pattern})'''.strip()

	def __str__(self):
		return self.params

class WhereHaving:

	def __init__(self, col, params:types.LambdaType, like=None, _not:bool=False, **kwargs):
		if not isinstance(like,(type(None),Like)):
			raise QueryException(f'The "like" data type must be {type(Like)} or None')

		self.col = col
		self.table = col.table
		self.key = col.key
		self.params = f'''{self.change_words(params)}'''
		self.func = repr(f'''({'not' if _not else ''} ( {self.change_values(self.params)} {self.set_like(like,True)} ))''')
		self.params = f'''{'NOT' if _not else ''} ({self.params} {self.set_like(like)})'''

	def __str__(self):
		return self.params

	def set_like(self,like,value=False) -> str:
		if like is not None:
			return like.func if value else like.params
		return ''

	def convert_operators(self,input_string,pattern):
		regex = re.compile(r"('[^']*'|\"[^\"]*\")|({})".format(pattern))
		def replace(match):
			if match.group(1):
				return match.group(1)
			else:
				operator = match.group(2)
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
		pattern = r'\b(?:\w+|\d+)\b(?=(?:[^"\']*"[^"\']*"[^"\']*)*(?:[^"\']|$))'
		def replace_word(match):
			word = match.group(0)
			if word.lower() in ['and', 'or', 'not', 'in', 'is', 'true', 'false']:
				return word.upper()
			elif word.lower() == 'none':
				return 'NULL'
			return word
		return self.convert_operators(self.replace_brackets(re.sub(pattern, replace_word, query)),r"(==|!=|<=|>=|<|>)")

	def replace_brackets(self,text) -> str:
		return re.sub(r'"[^"]*"|\'[^\']*\'|\[([^\[\]]*)\]', lambda x: '(' + x.group(1) + ')' if x.group(1) else x.group(0), text)

	def change_func(self,query) -> str:
		pattern = r'\b(\w+)\(([^)]*)\)'
		def replace_word(match):
			word = match.group(0)
			matches = re.findall(pattern, word)
			result = []
			for match in matches:
				function_name = match[0]
				parameters = list(filter(None,map(check_number_type ,match[1].split(','))))
				result = [function_name] + parameters
			if len(result)>1:
				if self.col.column == result[1]:
					return str(self.col.get(result[0].lower(),*result[2:]))
			return word
		return re.sub(pattern, replace_word, query)

	def change_values(self, query) -> str:
		pattern = r'\b(?:\w+\.?\w*|\d+)\b(?=(?:[^"\']*"[^"\']*"[^"\']*)*(?:[^"\']|$))'
		def replace_word(match):
			word = match.group(0)
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
		super(Where,self).__init__(*args, **kwargs)

class GroupBy:
	def __init__(self, *columns):
		if len(columns)==0:
			raise QueryException('Columns must not be empty')
		self.columns = list(set(map(lambda x: x if type(x) is str else x.key, columns)))
		self.params = f'''GROUP BY {','.join(set(map(lambda x: x if type(x) is str else x.column, columns)))}'''.strip()
		self.func = lambda x: x.constructor(x.groupby(self.columns).first().reset_index()[self.columns])
		# self.func = lambda x: [dict(b) for b in [set(tuple(filter(lambda x: x[0] in self.columns, d.items()))) for d in x]]
	def __str__(self):
		return self.params

class Having(WhereHaving):
	def __init__(self, *args, **kwargs):
		super(Having,self).__init__(*args, **kwargs)

class OrderBy:
	def __init__(self, column:Union[str,list], reverse:bool=False):
		if not isinstance(column,(str,pd.Series)):
			raise QueryException(f'column data type must be "str" or "Columns"')
		if isinstance(column,pd.Series):
			col = f'{column.table}.{column.key}'
			column = column.key
		elif isinstance(column,str):
			col = column
		self.params = f'''ORDER BY {col} {'DESC' if reverse else ''}'''.strip()
		self.func = lambda x: x.constructor(x.sort_values(column, ascending=reverse))
	def __str__(self):
		return self.params

class LimitOffset:
	def __init__(self, limit:int=0, offset:int=0):
		limit = abs(int(limit))
		offset = abs(int(offset))
		self.params = ''
		if limit>0:					self.params += f'''LIMIT {limit} '''
		if offset>0:				self.params += f'''OFFSET {offset} '''
		self.params =				self.params.strip()
		if limit>0 and offset>=0:	self.func = lambda x: x.constructor(x[offset:offset+limit])
		elif limit==0 and offset>0:	self.func = lambda x: x.constructor(x[offset:])
		else:						self.func = lambda x: x
	def __str__(self):
		return self.params

class Join:

	__hows__ = {
		'inner':'INNER JOIN',
		'right':'RIGHT JOIN',
		'left':'LEFT JOIN',
		'outer':'FULL OUTER JOIN',
	}
	
	def __init__(self, table1, table2, column:str, how:str, NaN:bool=False):
		if how not in self.__hows__.keys():
			raise QueryException(f'The "how" attribute does not exist. Select something you need from this list {list(self.hows.keys())}')
		if not isinstance(table1,pd.DataFrame) and not isinstance(table2,pd.DataFrame):
			raise QueryException(f'Tables must exist in the database')
		if not table1.is_column(column):
			raise QueryException(f'Column "{column}" is missing in table "{table1.table}"')
		if not table2.is_column(column):
			raise QueryException(f'Column "{column}" is missing in table "{table2.table}"')
		self.table1 = table1
		self.table2 = table2
		self.column = column
		self.how = how
		self.NaN = NaN
		self.params = f'''{self.__hows__[self.how]} {self.table2.table} ON {self.table1.get_column(column).column}={self.table2.get_column(column).column}'''

	def func(self,*args) -> dict:
		merge = pd.merge(self.table1.values(), self.table2.values(), on=self.column, how=self.how)
		if self.NaN:
			merge = merge.fillna('NaN')
			merge = merge.replace('NaN', NoneValue)
		return self.table1.constructor(merge)


class Condition:

	def __init__(self,table=None,*tables):
		self.parameters = ''
		self.functions = []
		self.course = []
		self.course_join = []
		self.correct_course = Where,GroupBy,Having,OrderBy
		self.table = table
		self.tables = tables

	def __str__(self):
		return self.parameters

	def check(self):
		self.course_join.extend(self.correct_course)
		isk_rem = filter(lambda x: any([isinstance(curs,x) for curs in self.course]),self.course_join)
		for curs in zip(self.course,isk_rem):
			if not isinstance(curs[0],(curs[1], LimitOffset)):
				raise QueryException('The order of the conditions is not correct')

	def where(self,items:dict,options:list=[]):
		if len(items)-1 != len(options):
			raise QueryException('The number of parameters does not match the values')
		self.parameters += ' WHERE '
		func_where = 'lambda x: x.constructor(x.query( '
		for key, value in items.items():
			if self.table is not None:
				if not self.table.is_column(key):
					raise QueryException(f'The "{key}" column does not exist')
				key = self.table.get_column(key)
			val = value(key,sql=True)
			if not isinstance(val,Where):
				raise QueryException(f'The data type must be "{type(Where)}"')
			self.parameters += val.params
			func_where += val.func
			if len(options)>0:
				self.parameters += ' '+options[0].upper()+' '
				func_where += ' '+('&' if options[0].lower().strip()=='and' else '|')+' '
				options.pop(0)
		func_where += ' ))'
		self.functions.append(eval(func_where))
		self.course.append(val)
		self.check()

	def having(self,items:dict,options:list=[]):
		if len(items)-1 != len(options):
			raise QueryException('The number of parameters does not match the values')
		self.parameters += ' HAVING '
		func_having = 'lambda x: x.constructor(x.query( '
		for key, value in items.items():
			if self.table is not None:
				if not self.table.is_column(key):
					raise QueryException(f'The "{key}" column does not exist')
				key = self.table.get_column(key)
			val = value(key,sql=True)
			if not isinstance(val,Where):
				raise QueryException(f'The data type must be "{type(Where)}"')
			self.parameters += val.params
			func_where += val.func
			if len(options)>0:
				self.parameters += ' '+options[0].upper()+' '
				func_where += ' '+('&' if options[0].lower().strip()=='and' else '|')+' '
				options.pop(0)
		func_having += ' ))'
		self.functions.append(eval(func_having))
		self.course.append(val)
		self.check()

	def orderBy(self,ob:OrderBy):
		if not isinstance(ob,OrderBy):
			raise QueryException(f'The data type must be "{type(OrderBy)}"')
		self.parameters += ' '+ob.params
		self.functions.append(ob.func)
		self.course.append(ob)
		self.check()

	def limit_offset(self,lo:LimitOffset):
		if not isinstance(lo,LimitOffset):
			raise QueryException(f'The data type must be "{type(LimitOffset)}"')
		self.parameters += ' '+lo.params
		self.functions.append(lo.func)
		self.course.append(lo)
		self.check()

	def groupBy(self,gb:GroupBy):
		if not isinstance(gb,GroupBy):
			raise QueryException(f'The data type must be "{type(GroupBy)}"')
		self.parameters += ' '+gb.params
		self.functions.append(gb.func)
		self.course.append(gb)
		self.check()

	def join(self,jo:Join):
		if not isinstance(jo,Join):
			raise QueryException(f'The data type must be "{type(Join)}"')
		self.parameters += ' '+jo.params
		self.functions.append(jo.func)
		self.course_join.append(Join)
		self.course.append(jo)
		self.check()