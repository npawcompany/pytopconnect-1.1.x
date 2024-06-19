import re
import types
import inspect

class Trigger:

	def __init__(self,table,time:str,event:str,definer:str=None,delitmiter:str=None,**kwargs):
		self.name = ''
		self.time = time.strip().upper()
		self.event = event.strip().upper()
		self.paramets = kwargs
		self.definer = definer
		self.delitmiter = delitmiter
		self.table = table
		self.db = self.table.parent
		if self.db is None:
			raise Exception('The database must be')
		if self.db.method not in ['mysql','sqlserver','postgresql']:
			raise Exception(f'The trigger does not exist in this DBMS "{self.db.method}"')
		if self.time not in ['BEFORE','AFTER']:
			raise Exception('"time" must contain "before" or "after"')
		if self.event not in ['INSERT','UPDATE','DELETE']:
			raise Exception(f'"event" must contain "insert", "update" or "after"')

	def __str__(self):
		return f'Trigger ({self.db.db_name}|{self.db.method}) => {self.name}'

	def get_definer(self):
		return f'DEFINER {self.definer}' if self.definer is not None else ''

	def __call__(self,func:types.FunctionType,*args,**kwargs):
		self.name = func.__name__
		self.body = func()
		if not isinstance(self.body,str):
			raise Exception('The trigger body must be a string')
		if len(self.body.strip())==0:
			raise Exception('The trigger body must not be empty')
		if not self.db._query_('CREATE_TRIGGER',self):
			return 
		setattr(self.table,f'ttc_{self.name}',self)
		return self

	def remove(self,if_exists=True) -> bool:
		if not self.db._query_('DROP_TRIGGER',self.name,{'if_exists':if_exists}):
			return False
		delattr(self.table,f'ttc_{self.name}')
		del self
		return True

class Function:

	def __init__(self,db,definer:str=None,delitmiter:str=None,**kwargs):
		self.name = ''
		self.list_paramets = []
		self.count = lambda : len(self.list_paramets)
		self.params = kwargs
		self.definer = definer
		self.delitmiter = delitmiter
		self.db = db
		if self.db is None:
			raise Exception('The database must be')
		if self.db.method not in ['mysql','sqlserver','postgresql']:
			raise Exception(f'The function does not exist in this DBMS "{self.db.method}"')

	def __str__(self):
		return f'Function ({self.db.db_name}|{self.db.method}) => {self.name}'

	def get_definer(self):
		return f'DEFINER {self.definer}' if self.definer is not None else ''

	def __call__(self,func:types.FunctionType):
		self.name = func.__name__
		signature = inspect.signature(func)
		self.parameters = signature.parameters
		self.list_paramets.extend(self.parameters)
		self.return_type = signature.return_annotation
		self.body = func(*([None]*len(self.parameters)))
		if not isinstance(self.body,str):
			raise Exception('The function body must be a string')
		if len(self.body.strip())==0:
			raise Exception('The function body must not be empty')
		if not isinstance(self.return_type,types.FunctionType):
			raise Exception(f'The data type must be present from the DataTypes list')
		if not self.db._query_('CREATE_FUNCTION',self):
			return 
		setattr(self.db,f'ftc_{self.name}',self)
		return self

	def to_str(self, x) -> str:
		if isinstance(x,str):
			return repr(x)
		elif isinstance(x,bool):
			return f"{int(x)}"
		elif isinstance(x,(int, float)):
			return f"{x}"
		elif isinstance(x,(list, tuple, dict)):
			return f"'{json.dumps(x, ensure_ascii=False)}'"
		elif isinstance(x,(datetime, date, time)):
			return f"'{x}'"
		else:
			return "NULL"

	def run(self,*args,**kwargs):
		if len(args)!=self.count():
			raise Exception('Parameter lengths do not match')
		result = self.db._query_('RUN_FUNCTION',{
			'function':self.name,
			'parameters':list(map(self.to_str,args))
		},self.list_paramets)
		return result.get(self.name,None)

	def remove(self,if_exists=True) -> bool:
		if not self.db._query_('DROP_FUNCTION',self.name,{'if_exists':if_exists}):
			return False
		delattr(self.db,f'ftc_{self.name}')
		del self
		return True

class Procedure:
	
	def __init__(self,db,definer:str=None,delitmiter:str=None,**kwargs):
		self.name = ''
		self.list_paramets = []
		self.count = lambda : len(self.list_paramets)
		self.params = kwargs
		self.definer = definer
		self.delitmiter = delitmiter
		self.db = db
		if self.db is None:
			raise Exception('The database must be')
		if self.db.method not in ['mysql','sqlserver','postgresql']:
			raise Exception(f'The procedure does not exist in this DBMS "{self.db.method}"')

	def __str__(self):
		return f'Procedure ({self.db.db_name}|{self.db.method}) => {self.name}'

	def get_definer(self):
		return f'DEFINER {self.definer}' if self.definer is not None else ''
	
	def __call__(self,func:types.FunctionType):
		self.name = func.__name__
		signature = inspect.signature(func)
		self.parameters = signature.parameters
		self.list_paramets.extend(self.parameters)
		self.body = func(*([None]*len(self.parameters)))
		if not isinstance(self.body,str):
			raise Exception('The procedure body must be a string')
		if len(self.body.strip())==0:
			raise Exception('The procedure body must not be empty')
		if not self.db._query_('CREATE_PROCEDURE',self):
			return 
		setattr(self.db,f'ptc_{self.name}',self)
		return self

	def __params__(self,a:str,b:str,c:str=''):
		paramet = self.params.get(a,{}).get(b,c)
		return ' '.join(paramet) if isinstance(paramet,(list,tuple)) else paramet

	def to_str(self, x) -> str:
		if isinstance(x,str):
			return repr(x)
		elif isinstance(x,bool):
			return f"{int(x)}"
		elif isinstance(x,(int, float)):
			return f"{x}"
		elif isinstance(x,(list, tuple, dict)):
			return f"'{json.dumps(x, ensure_ascii=False)}'"
		elif isinstance(x,(datetime, date, time)):
			return f"'{x}'"
		else:
			return "NULL"

	def run(self,*args,**kwargs):
		if len(args)!=self.count():
			raise Exception('Parameter lengths do not match')
		result = self.db._query_('RUN_PROCEDURE',{
			'procedure':self.name,
			'parameters_org':args,
			'parameters':list(map(self.to_str,args)),
			'params': list(map(lambda x: x[1].annotation(self.db.dataTypes),self.parameters.items())),
			'call':kwargs.get('call',False)
		},self.list_paramets)
		return result

	def remove(self,if_exists=True) -> bool:
		if not self.db._query_('DROP_PROCEDURE',self.name,{'if_exists':if_exists}):
			return False
		delattr(self.db,f'ptc_{self.name}')
		del self
		return True

