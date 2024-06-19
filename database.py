from .condition import *
from .datatypes import DataTypes, Index
from .storage import Procedure, Function, Trigger
from pandas import Series, DataFrame
from datetime import *
from decimal import Decimal
from dateutil import parser as dps
import inspect

import types
from typing import Union
import json

def __default_values__(key,args,n,t=False):
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

def __default_to_value__(key,val):
	val = None if val=="NULL" or val is None else str(val)
	if val is not None:
		if bool(re.match(r'^-?\d+(\.\d+)?$', val)):
			return float(val)
		elif val == 'CURRENT_TIMESTAMP':
			return datetime.now()
		elif val in ['TRUE','FALSE']:
			return True if val=='TRUE' else False
		try:
			return dps.parse(val[1:-1])
		except:
			try:
				return json.loads(val[1:-1])
			except:
				return val[1:-1]
	return val

def __check_variable_name__(text):
	pattern = r'^[a-zA-Z_][a-zA-Z0-9_]*$'
	return re.match(pattern, text)

class DataBase:

	def __init__(self,method:str,data:types.GeneratorType,*args,**kwargs):
		super(DataBase, self).__init__()
		if not isinstance(data,Series):
			raise QueryException(f"Data types do not match")
		self.names = []
		self.method = method
		for name, tables in data.items():
			self.names.append(name)
			if not __check_variable_name__(f'tc_{name}'):
				raise QueryException(f"The '{name}' data base name must follow the variable creation rules")
			setattr(self, f'tc_{name}', Tables(method,name,tables))

	def create_index(self,table):
		name_table = getattr(table,'_tc_name_table',table.__class__.__name__)
		if hasattr(table,'_tc_indexs_'):
			indexs = getattr(table,'_tc_indexs_')
			if not isinstance(indexs, (types.MethodType,Index,tuple,list)):
				raise QueryException(f"'_tc_indexs_' must be a method or list or tuple or Index")
			indexs = indexs() if isinstance(indexs,types.MethodType) else indexs
			if isinstance(indexs,(tuple,list)):
				for index in indexs:
					if not self._query_('CREATE_INDEX',{name_table:index}):
						return 
			else:
				if not self._query_('CREATE_INDEX',{name_table:indexs}):
					return

	def create_foreign(self,table):
		name_table = getattr(table,'_tc_name_table',table.__class__.__name__)
		if hasattr(table,'_tc_foreigns_'):
			foreigns = getattr(table,'_tc_foreigns_')
			if not isinstance(foreigns, (types.MethodType,Index,tuple,list)):
				raise QueryException(f"'_tc_foreigns_' must be a method or list or tuple or Forein")
			foreigns = foreigns() if isinstance(foreigns,types.MethodType) else foreigns
			if isinstance(foreigns,(tuple,list)):
				for forein in foreigns:
					if not self._query_('CREATE_FOREIGN',name_table,forein):
						return 
			else:
				if not self._query_('CREATE_FOREIGN',name_table,foreigns):
					return

	def create(self,obj=None,*args,**kwargs):
		def append(func):
			if not isinstance(func, type):
				raise QueryException(f"An object must be passed")
			table = func(self.method,*args, **kwargs)
			name_table = getattr(table,'_tc_name_table',table.__class__.__name__)
			r = getattr(table,'_tc_options_',{})
			r = r() if isinstance(r,types.MethodType) else r
			prefix = getattr(table,'_tc_prefix_','').strip()
			prefix = (prefix if __check_variable_name__(prefix.strip()) else '') if isinstance(prefix,str) else ''
			if len(prefix)>0:
				for key, val in table.__dict__.items():
					table.__dict__[f'{prefix}_{key}'] = val
					del table.__dict__[key]
			if not isinstance(r,dict):
				raise QueryException("'_tc_options_' must be a dictionary")
			if self.is_table(name_table):
				return self.get(name_table)
			if not self._query_('CREATE',{name_table:table.__dict__},r):
				return
			self.create_index(table)
			self.create_foreign(table)
			values = getattr(table, '_tc_values_', {})
			if not isinstance(values,(dict,types.MethodType)):
				raise QueryException(f"'_tc_values_' must be a method or dictionary")
			values = values() if isinstance(values,types.MethodType) else values
			if isinstance(values,dict):
				if not self._query_('INSERT',{name_table:{
					'columns':values.keys(),
					'values':map(lambda x: [str(i) if isinstance(i,(int,tuple)) else f" '{i}'  " for i in x],zip(*values.values()))
				}}):
					return
			ns = [len(v) for v in values.values()]
			n = int(sum(ns)/len(ns))
			if not all([n==i for i in ns]):
				raise QueryException(f"Number of values do not match")
			self[name_table] = [{
				'_query_': self._query_,
				'_upgraded_': self._upgraded_
			}]
			for key,val in table.__dict__.items():
				if key not in ['__TC_DATANAME__']:
					if not any([ik in table.__dict__.keys() for ik in values.keys()]):
						self[name_table][0][key] = NoneValue()
						self[name_table][0]['_query_'] = self._query_
						self[name_table][0]['_upgraded_'] = self._upgraded_
					else:
						i = 0
						for value in values.get(key,__default_values__(key,val,n, key in values.keys())):
							try:
								self[name_table][i][key] = value
							except IndexError:
								self[name_table].append({key:value})
							self[name_table][i]['_query_'] = self._query_
							self[name_table][i]['_upgraded_'] = self._upgraded_
							i += 1
			self.enjoin()
			self._upgraded_()
			return self.get(name_table)
		return append if obj is None else partial(append,obj)

class Tables(Series, DataBase):

	def __init__(self,method:str,name:str,items:Series,*args,**kwargs):
		super(Tables, self).__init__(items)
		self.name = name
		self.method = method
		self.parent = kwargs.get('parent',None)
		self.dataTypes = DataTypes(self.method)
		self.enjoin()
		self.version = self._connection_().version()
		self.db_name = self._connection_().DB_NAME_ORG
		if self.method in ['mysql','postgresql']:
			procedures = self._query_('SHOW_PROCEDURE',self.db_name)
			functions = self._query_('SHOW_FUNCTION',self.db_name)
			for procedure, value in procedures.items():
				proc = Procedure(self)
				proc.name = procedure
				proc.list_paramets = list(filter(None,value))
				setattr(self, f'ptc_{procedure}', proc)
			for function, value in functions.items():
				func = Function(self)
				func.name = function
				func.list_paramets = list(filter(None,value))
				setattr(self, f'ftc_{function}', func)

	def enjoin(self):
		self.ALL_TABLES = self.get_tables()
		for key,val in self.items():
			if not __check_variable_name__(f'tc_{key}'):
				raise QueryException(f"The '{key}' table name must follow the variable creation rules")
			if not isinstance(val, (types.LambdaType,types.FunctionType,types.MethodType,partial)):
				if not self.is_table(key):
					setattr(self,f'tc_{key}',Items(key,val,parent=self))
			else:
				setattr(self,key,val)

	def open(self) -> bool:
		if self._connection_().is_active():
			return False
		self._connection_().open()
		return True

	def close(self) -> bool:
		if not self._connection_().is_active():
			return False
		self._connection_().close()
		return True

	def get_procedure(self,procedure:str,default=None):
		return getattr(self, f'ptc_{procedure}', default)

	def get_procedures(self):
		return tuple(map(lambda y: self.get_procedure(y.replace('ptc_','',1)),filter(lambda x: x.startswith('ptc_') ,self.__dict__.keys())))

	def get_function(self,function:str,default=None):
		return getattr(self, f'ftc_{function}', default)

	def get_functions(self):
		return tuple(map(lambda y: self.get_function(y.replace('ftc_','',1)),filter(lambda x: x.startswith('ftc_') ,self.__dict__.keys())))

	def is_table(self,*tables) -> bool:
		return all([hasattr(self,f'tc_{table}') for table in tables])

	def get(self,table:str,default=None):
		return getattr(self, f'tc_{table}', default)

	def get_tables(self,*tables) -> list:
		if len(tables)==0:
			return list(self.keys())
		return [self.get(table) for table in tables if self.is_table(table)]

	def remove(self,table:str) -> bool:
		try:
			if not self.is_table(table):
				raise QueryException(f"Table '{table}' does not exist")
			if not self._query_('DROP', table ):
				return False
			self.drop(table, inplace=True)
			delattr(self, f'tc_{table}')
			self.enjoin()
			return True
		except BaseException as e:
			raise e

	def rename_tables(self,old_table:str,new_table:str) -> bool:
		try:
			if not self.is_table(old_table):
				raise QueryException(f"Table '{old_table}' does not exist")
			if self.is_table(new_table):
				raise QueryException(f'"{new_table}" name already exists')
			if not __check_variable_name__(new_table):
				raise QueryException(f'"{new_table}" name does not match the creation rules')
			if not self._query_('RENAME_TABLE', { old_table : new_table } ):
				return False
			table = self.get(old_table)
			table.table = new_table
			setattr(self, f'tc_{new_table}', table)
			delattr(self, f'tc_{old_table}')
			self.rename(index={old_table:new_table}, inplace=True)
			self.enjoin()
			self._upgraded_()
			return True
		except BaseException as e:
			raise e

class Items(DataFrame):

	def __init__(self,table:str,data:DataFrame,*args,**kwargs):
		super(Items, self).__init__(data)
		self.table = table
		self.parent = kwargs.get('parent',None)
		self.enjoin()
		self.LENGTH = self.get_count_row()
		self.dataTypes = self.parent.dataTypes
		if self.parent.method in ['mysql','postgresql']:
			triggers = self._query_('SHOW_TRIGGER',self.table)
			for trigger, val in triggers.items():
				trig = Trigger(self,val.get('time'),val.get('event'))
				trig.name = trigger
				setattr(self, f'ttc_{trigger}', trig)

	def constructor(self,data):
		data = data.copy()
		data.loc[:, '_query_'] = [self._query_]*max(len(data),1)
		data.loc[:, '_upgraded_'] = [self._upgraded_]*max(len(data),1)
		data.loc[:, '_connection_'] = [self._connection_]*max(len(data),1)
		return Items(self.table,data,parent=self.parent)

	def __setattr__(self, key, value):
		self.__dict__[key] = value

	def enjoin(self):
		def is_noneValue(value):
			return isinstance(value,NoneValue)
		self.ALL_COLUMNS = self.getColumns()
		self.LENGTH = self.get_count_row()
		for key,val in self.to_dict(orient='list').items():
			if len(val)>0:
				if not all(isinstance(v, (types.LambdaType,types.FunctionType,types.MethodType,partial)) for v in val):
					if not __check_variable_name__(f'tc_{key}'):
						raise QueryException(f"The '{key}' column name must follow the variable creation rules")
						# list(filter(lambda x: not isinstance(x,NoneValue),val))
					setattr(self, f'tc_{key}', Column(self.table,key,val,parent=self))
				else:
					setattr(self,key,val[0])
					del self[key]
		mask = self.apply(lambda col: col.apply(is_noneValue))
		indexs = list(self[mask.all(axis=1)].index)
		if len(indexs)>0:
			self.drop(indexs, inplace=True)

	def to_str(self, x) -> str:
		if isinstance(x,str):
			return repr(x)
		elif isinstance(x,bool):
			return f"{int(x)}"
		elif isinstance(x,(int, float)):
			return f"{x}"
		elif isinstance(x,Decimal):
			return f"{float(x)}"
		elif isinstance(x,(list, tuple, dict)):
			return f"'{json.dumps(x, ensure_ascii=False)}'"
		elif isinstance(x,(datetime, date, time)):
			return f"'{x}'"
		else:
			return "NULL"

	def is_empty(self) -> bool:
		return len(self)==0

	def is_column(self,*columns) -> bool:
		return all([hasattr(self,f'tc_{column}') for column in columns])

	def get_column(self,column:str):
		return getattr(self,f'tc_{column}',None)

	def getColumns(self,*columns) -> list:
		if len(columns)==0:
			try:
				return list(filter(lambda x: x not in ['_query_','_upgraded_','_connection_'],self.keys()))
			except IndexError:
				return getattr(self,'ALL_COLUMNS',[])
		if not isinstance(columns, tuple):
			raise QueryException("Columns must be of the following data types: 'tuple' and must contain a string")
		columns = filter(self.is_column,columns)
		return DataFrame(dict(zip(columns,map(self.get_column,columns))))

	def get_count_columns(self) -> int:
		if not self.is_empty():
			return len(list(self.ALL_COLUMNS))

	def get_row(self,i:int) -> dict:
		if not self.is_empty():
			return self[i]

	def get_count_row(self) -> int:
		return len(self)

	def is_required(self, column) -> bool:
		return column in self.required_columns()

	def required_columns(self) -> list:
		return [key for key, val in self.types().items() if val.get('required')]

	def get_trigger(self,trigger:str,default=None):
		return getattr(self, f'ttc_{trigger}', default)

	def get_triggers(self):
		return tuple(map(lambda y: self.get_trigger(y.replace('ttc_','',1)),filter(lambda x: x.startswith('ttc_') ,self.__dict__.keys())))

	def commit(self):
		self._connection_().commit()

	def rollback(self):
		self._connection_().rollback()

	def types(self,column:str='') -> dict:
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

	def added(self,values:list,columns:list) -> list:
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
						value[k] = val.max()+1 if len(val)>0 else 1
					else:
						value[k] = v.get('default')
			values[i] = value
		return values

	def get(self,columns:list=None,condition:Condition=Condition(),distinct:bool=False,sql:bool=False) -> DataFrame:
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

	def add(self,values:list,columns:list)->bool:
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

	def update(self,items:dict,condition:Condition=Condition())->bool:
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

	def delete(self,condition:Condition=Condition())->bool:
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

	def get_foreigns(self)->list:
		foreigns = self._query_('SHOW_FOREIGN',self.table,self.parent.db_name)
		self._upgraded_()
		return foreigns

	def get_index(self) -> list:
		try:
			indexs = self._query_('SHOW_INDEX',self.table,self.parent.db_name)
			self._upgraded_()
			return indexs
		except BaseException as e:
			raise e

	def set_index(self,index) -> bool:
		try:
			if not self._query_('CREATE_INDEX',{self.table:index}):
				return False
			self._upgraded_()
			return True
		except BaseException as e:
			raise e

	def del_index(self, name) -> bool:
		if not isinstance(name,str):
			raise QueryException(f"Data types do not match")
		try:
			if not self._query_('DROP_INDEX',{self.table:name}):
				return False
			self._upgraded_()
			return True
		except BaseException as e:
			raise e

	def rename_table(self,new_name:str) -> bool:
		if not self.parent.rename_tables(self.table,new_name):
			return False
		return True

	def remove(self)->bool:
		if not self.parent.remove(self.table):
			return False
		return True

	def add_column(self,column:str,values:list=[],*args)->bool:
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

	def edit_column(self,column:str,**kwargs)-> bool:
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

	def rename_column(self,old_column:str,new_column:str)->bool:
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

	def remove_column(self,column:str)->bool:
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

	def __init__(self,table:str,key:str,values:list,*args,**kwargs):
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

	def constructor(self,data):
		return Column(self.table,self.key,data,parent=self.parent)

	def __setattr__(self, key, value):
		self.__dict__[key] = value

	def get_foreign(self)->list:
		data = {}
		for i in filter(lambda a: a['from']==self.key, self.parent.get_foreigns()):
			table = self.parent.parent.get(i.get('table'))
			data[i['name']]=self.parent.merge(table, left_on=i['from'], right_on=i['to'], how='inner')
		return Series(data)

	def set_foreign(self,name:str,references:list)->bool:
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

	def set_default(self,value:Union[str,bool,int,float,list,tuple,dict,datetime,date,time])->bool:
		return self.parent.edit_column(self.key, default=lambda this: DataTypes.DEFAULT(this,value) )

	def set_type(self,data_type:Union[str,types.LambdaType,types.MethodType,types.FunctionType,partial])->bool:
		return self.parent.edit_column(self.key, data_type=data_type )

	def set_required(self,on:bool)->bool:
		return self.parent.edit_column(self.key, null=lambda this: DataTypes.NULL(this,on) )

	def set_primary(self,on:bool)->bool:
		return self.parent.edit_column(self.key, primary_key=lambda this: DataTypes.PRIMARY(this,on) )

	def rename(self,new_name:str) -> bool:
		return self.parent.rename_column(self.key,new_name)

	def edit(self,**kwargs) -> bool:
		return self.parent.edit_column(self.key,**kwargs)

	def remove(self) -> bool:
		return self.parent.remove_column(self.key)

	def clearing(self) -> bool:
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
		self.clear()
		self.extend(self.__copy_data__)

	def get(self,attr,*args,**kwargs):
		if attr == 'count':
			attr = 'length'
		if hasattr(self,attr):
			return getattr(self,attr)(*args,**kwargs)
		raise QueryException(f'Attribute "{attr}" does not exist')

	def __run_function__(self,fun,condition) -> list:
		if not isinstance(condition,(types.LambdaType, type(None))):
			raise QueryException(f"Data types do not match")
		if condition is not None:
			return fun(list(filter(condition, self)))
		return fun(self)

	def max(self,condition:types.LambdaType=None,sql:bool=False) -> Union[int, str]:
		return f'''MAX({self.column})''' if sql else self.__run_function__(max,condition)

	def min(self,condition:types.LambdaType=None,sql:bool=False) -> Union[int, str]:
		return f'''MIN({self.column})''' if sql else self.__run_function__(min,condition)

	def length(self,condition:types.LambdaType=None,sql:bool=False) -> Union[int, str]:
		return f'''COUNT({self.column})''' if sql else self.__run_function__(len,condition)

	def sum(self,condition:types.LambdaType=None,sql:bool=False) -> Union[int, float, str]:
		return f'''SUM({self.column})''' if sql else self.__run_function__(sum,condition)

	def avg(self,condition:types.LambdaType=None,sql:bool=False) -> Union[int, float, str]:
		return f'''AVG({self.column})''' if sql else self.__run_function__(lambda arr:sum(arr)/len(arr),condition)

	def round(self,k:int,condition:types.LambdaType=None,sql:bool=False) -> Union[list, str]:
		return f'''ROUND({self.column},{k})''' if sql else self.__run_function__(lambda arr:list(map(lambda x: round(x,k), arr)),condition)

	def random(self,k:int,condition:types.LambdaType=None) -> list:
		return self.__run_function__(lambda arr: random.choices(arr, k=k), condition)

	def random_one(self,condition:types.LambdaType=None) -> list:
		return self.__run_function__(lambda arr: random.choice(arr), condition)

	def shuffle(self,n:int=1,condition:types.LambdaType=None) -> list:
		data = self.__run_function__(lambda arr: arr, condition)
		n = abs(int(n))
		for _ in range(n):
			random.shuffle(data)
		return data

	def mult(self,condition:types.LambdaType=None) -> Union[int, float]:
		return self.__run_function__(lambda arr: reduce(lambda x, y: x * y, arr), condition)

	def diff(self,condition:types.LambdaType=None) -> Union[int, float]:
		return self.__run_function__(lambda arr: reduce(lambda x, y: x - y, arr), condition)

	def quot(self,condition:types.LambdaType=None) -> Union[int, float]:
		return self.__run_function__(lambda arr: reduce(lambda x, y: x / y, arr), condition)

	def filter(self,func:types.FunctionType=None) -> list:
		return self.__run_function__(lambda arr: filter(func, arr) , None)

	def map(self,func:types.FunctionType,condition:types.LambdaType=None) -> list:
		return self.__run_function__(lambda arr: map(func, arr) , condition)

	def enumerate(self,key:types.FunctionType=None,reverse:bool=False,condition:types.LambdaType=None) -> list:
		return self.__run_function__(enumerate, condition)

	def mirror(self,condition:types.LambdaType=None) -> list:
		return self.__run_function__(lambda arr: arr[::-1], condition)

	def power(self,x:Union[int, float]=2,condition:types.LambdaType=None) -> Union[int, float]:
		return self.__run_function__(lambda arr: arr**x, condition)

	def join(self,t:str='',condition:types.LambdaType=None) -> str:
		return self.__run_function__(lambda arr: t.join(arr), condition)

