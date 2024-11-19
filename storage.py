import re
import types
import inspect
import json
from datetime import *

class Trigger:
	"""
	A class representing a database trigger.

	This class encapsulates the properties and behaviors of a database trigger, allowing for 
	the definition of triggers that respond to specific events on a table. It provides methods 
	to create, call, and remove triggers, ensuring compatibility with supported database management systems.

	Args:
		table (str): The name of the table associated with the trigger.
		time (str): The timing of the trigger, either 'BEFORE' or 'AFTER'.
		event (str): The event that activates the trigger, such as 'INSERT', 'UPDATE', or 'DELETE'.
		definer (str, optional): The definer of the trigger. Defaults to None.
		delitmiter (str, optional): The delimiter for the trigger body. Defaults to None.
		**kwargs: Additional parameters for the trigger.

	Attributes:
		name (str): The name of the trigger.
		time (str): The timing of the trigger.
		event (str): The event that activates the trigger.
		paramets (dict): Additional parameters for the trigger.
		definer (str): The definer of the trigger.
		delitmiter (str): The delimiter for the trigger body.
		table (str): The table associated with the trigger.
		db: The database object associated with the table.
	"""

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
			raise ValueError('The database must be')
		if self.db.method not in ['mysql','sqlserver','postgresql']:
			raise ValueError(f'The trigger does not exist in this DBMS "{self.db.method}"')
		if self.time not in ['BEFORE','AFTER']:
			raise ValueError('"time" must contain "before" or "after"')
		if self.event not in ['INSERT','UPDATE','DELETE']:
			raise ValueError('"event" must contain "insert", "update" or "after"')

	def __str__(self):
		return f'Trigger ({self.db.db_name}|{self.db.method}) => {self.name}'

	def get_definer(self):
		return f'DEFINER {self.definer}' if self.definer is not None else ''

	def __call__(self, func: types.FunctionType, *args, **kwargs):
		"""
		Execute the trigger by calling the specified function.

		This method allows the trigger to be invoked as a callable, executing the provided function 
		and storing its name and body. It validates that the body is a non-empty string and attempts 
		to create the trigger in the database, associating it with the specified table.

		Args:
			func (types.FunctionType): The function to be executed as the trigger.
			*args: Additional positional arguments to pass to the function.
			**kwargs: Additional keyword arguments to pass to the function.

		Returns:
			self: The current instance of the trigger.

		Raises:
			Exception: If the trigger body is not a string or is empty, or if there is an error 
						during the trigger creation process.
		"""
		self.name = func.__name__
		self.body = func()
		if not isinstance(self.body,str):
			raise TypeError('The trigger body must be a string')
		if len(self.body.strip())==0:
			raise ValueError('The trigger body must not be empty')
		if not self.db._query_('CREATE_TRIGGER',self):
			return 
		setattr(self.table,f'ttc_{self.name}',self)
		return self

	def remove(self, if_exists: bool = True) -> bool:
		"""
		Remove the trigger from the database.

		This function attempts to delete the trigger associated with the current instance from the 
		database, optionally checking if the trigger exists before attempting removal. It returns 
		a boolean indicating the success of the operation.

		Args:
			if_exists (bool, optional): If True, the function will not raise an error if the trigger 
										does not exist. Defaults to True.

		Returns:
			bool: True if the trigger was successfully removed, False otherwise.
		"""
		if not self.db._query_('DROP_TRIGGER',self.name,{'if_exists':if_exists}):
			return False
		delattr(self.table,f'ttc_{self.name}')
		del self
		return True

class Function:
	"""
	A class representing a database function.

	This class encapsulates the properties and behaviors of a database function, allowing for 
	the definition, execution, and management of functions within a database. It provides methods 
	to create, call, and remove functions, ensuring compatibility with supported database management systems.

	Args:
		db: The database object associated with the function.
		definer (str, optional): The definer of the function. Defaults to None.
		delitmiter (str, optional): The delimiter for the function body. Defaults to None.
		**kwargs: Additional parameters for the function.

	Attributes:
		name (str): The name of the function.
		list_paramets (list): A list of parameters for the function.
		count (callable): A function to count the number of parameters.
		params (dict): Additional parameters for the function.
		definer (str): The definer of the function.
		delitmiter (str): The delimiter for the function body.
		db: The database object associated with the function.
	"""

	def __init__(self,db,definer:str=None,delitmiter:str=None,**kwargs):
		self.name = ''
		self.list_paramets = []
		self.count = lambda : len(self.list_paramets)
		self.params = kwargs
		self.definer = definer
		self.delitmiter = delitmiter
		self.db = db
		if self.db is None:
			raise ValueError('The database must be')
		if self.db.method not in ['mysql','sqlserver','postgresql']:
			raise ValueError(f'The function does not exist in this DBMS "{self.db.method}"')

	def __str__(self):
		return f'Function ({self.db.db_name}|{self.db.method}) => {self.name}'

	def get_definer(self):
		return f'DEFINER {self.definer}' if self.definer is not None else ''

	def __call__(self, func: types.FunctionType):
		"""
		Execute the function and register it as a database function.

		This method allows the function to be invoked as a callable, capturing its name, parameters, 
		return type, and body. It validates that the body is a non-empty string and attempts to create 
		the function in the database, associating it with the specified database object.

		Args:
			func (types.FunctionType): The function to be executed and registered as a database function.

		Returns:
			self: The current instance of the Function class.

		Raises:
			TypeError: If the function body is not a string or if the return type is not valid.
			ValueError: If the function body is empty.
		"""
		self.name = func.__name__
		signature = inspect.signature(func)
		self.parameters = signature.parameters
		self.list_paramets.extend(self.parameters)
		self.return_type = signature.return_annotation
		self.body = func(*([None]*len(self.parameters)))
		if not isinstance(self.body,str):
			raise TypeError('The function body must be a string')
		if len(self.body.strip())==0:
			raise ValueError('The function body must not be empty')
		if not isinstance(self.return_type,types.FunctionType):
			raise TypeError('The data type must be present from the DataTypes list')
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

	def run(self, *args, **kwargs):
		"""
		Execute the registered function with the provided arguments.

		This method runs the function associated with the current instance, passing the specified 
		arguments to it. It checks that the number of arguments matches the expected count and 
		retrieves the result from the database.

		Args:
			*args: Positional arguments to be passed to the function.
			**kwargs: Keyword arguments to be passed to the function.

		Returns:
			The result of the function execution, or None if the function does not return a value.

		Raises:
			Exception: If the number of provided arguments does not match the expected count.
		"""
		if len(args)!=self.count():
			raise ValueError('Parameter lengths do not match')
		result = self.db._query_('RUN_FUNCTION',{
			'function':self.name,
			'parameters':list(map(self.to_str,args))
		},self.list_paramets)
		return result.get(self.name,None)

	def remove(self, if_exists: bool = True) -> bool:
		"""
		Remove the registered function from the database.

		This method attempts to delete the function associated with the current instance from the 
		database, optionally checking if the function exists before attempting removal. It returns 
		a boolean indicating the success of the operation.

		Args:
			if_exists (bool, optional): If True, the function will not raise an error if the function 
										does not exist. Defaults to True.

		Returns:
			bool: True if the function was successfully removed, False otherwise.
		"""
		if not self.db._query_('DROP_FUNCTION',self.name,{'if_exists':if_exists}):
			return False
		delattr(self.db,f'ftc_{self.name}')
		del self
		return True

class Procedure:
	"""
	A class representing a database stored procedure.

	This class encapsulates the properties and behaviors of a stored procedure, allowing for 
	the definition, execution, and management of procedures within a database. It provides methods 
	to create, call, and remove procedures, ensuring compatibility with supported database management systems.

	Args:
		db: The database object associated with the procedure.
		definer (str, optional): The definer of the procedure. Defaults to None.
		delitmiter (str, optional): The delimiter for the procedure body. Defaults to None.
		**kwargs: Additional parameters for the procedure.

	Attributes:
		name (str): The name of the procedure.
		list_paramets (list): A list of parameters for the procedure.
		count (callable): A function to count the number of parameters.
		params (dict): Additional parameters for the procedure.
		definer (str): The definer of the procedure.
		delitmiter (str): The delimiter for the procedure body.
		db: The database object associated with the procedure.
	"""

	def __init__(self,db,definer:str=None,delitmiter:str=None,**kwargs):
		self.name = ''
		self.list_paramets = []
		self.count = lambda : len(self.list_paramets)
		self.params = kwargs
		self.definer = definer
		self.delitmiter = delitmiter
		self.db = db
		if self.db is None:
			raise ValueError('The database must be')
		if self.db.method not in ['mysql','sqlserver','postgresql']:
			raise ValueError(f'The procedure does not exist in this DBMS "{self.db.method}"')

	def __str__(self):
		return f'Procedure ({self.db.db_name}|{self.db.method}) => {self.name}'

	def get_definer(self):
		return f'DEFINER {self.definer}' if self.definer is not None else ''
	
	def __call__(self, func: types.FunctionType):
		"""
		Execute the specified function and register it as a stored procedure.

		This method allows the procedure to be invoked as a callable, capturing its name, parameters, 
		and body. It validates that the body is a non-empty string and attempts to create the procedure 
		in the database, associating it with the specified database object.

		Args:
			func (types.FunctionType): The function to be executed and registered as a stored procedure.

		Returns:
			self: The current instance of the Procedure class.

		Raises:
			TypeError: If the procedure body is not a string or is empty.
			ValueError: If the procedure body is empty.
		"""
		self.name = func.__name__
		signature = inspect.signature(func)
		self.parameters = signature.parameters
		self.list_paramets.extend(self.parameters)
		self.body = func(*([None]*len(self.parameters)))
		if not isinstance(self.body,str):
			raise TypeError('The procedure body must be a string')
		if len(self.body.strip())==0:
			raise ValueError('The procedure body must not be empty')
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

	def run(self, *args, **kwargs):
		"""
		Execute the registered stored procedure with the provided arguments.

		This method runs the stored procedure associated with the current instance, passing the specified 
		arguments to it. It checks that the number of arguments matches the expected count and retrieves 
		the result from the database.

		Args:
			*args: Positional arguments to be passed to the stored procedure.
			**kwargs: Keyword arguments to be passed to the stored procedure.

		Returns:
			The result of the stored procedure execution.

		Raises:
			ValueError: If the number of provided arguments does not match the expected count.
		"""
		if len(args)!=self.count():
			raise ValueError('Parameter lengths do not match')
		return self.db._query_('RUN_PROCEDURE',{
			'procedure':self.name,
			'parameters_org':args,
			'parameters':list(map(self.to_str,args)),
			'params': list(map(lambda x: x[1].annotation(self.db.dataTypes),self.parameters.items())),
			'call':kwargs.get('call',False)
		},self.list_paramets)

	def remove(self, if_exists: bool = True) -> bool:
		"""
		Remove the registered stored procedure from the database.

		This method attempts to delete the stored procedure associated with the current instance from 
		the database, optionally checking if the procedure exists before attempting removal. It returns 
		a boolean indicating the success of the operation.

		Args:
			if_exists (bool, optional): If True, the function will not raise an error if the procedure 
										does not exist. Defaults to True.

		Returns:
			bool: True if the procedure was successfully removed, False otherwise.
		"""
		if not self.db._query_('DROP_PROCEDURE',self.name,{'if_exists':if_exists}):
			return False
		delattr(self.db,f'ptc_{self.name}')
		del self
		return True

