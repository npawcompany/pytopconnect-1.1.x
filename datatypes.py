from .condition import *
from datetime import *
from decimal import Decimal
import json
import urllib.parse
import types
from typing import Union

def __check_variable_name__(text):
	"""
	Check if the given text is a valid Python variable name.

	This function uses a regular expression to verify if the input text follows the rules for a 
	valid Python variable name: it must start with a letter or underscore, followed by any 
	number of letters, numbers, or underscores. It returns a match object if the text is valid, 
	or None if it is not.

	Args:
		text (str): The string to be checked as a potential variable name.

	Returns:
		re.Match object or None: If the text is a valid variable name, a match object is returned; 
								 if not valid, None is returned.
	"""
	pattern = r'^[a-zA-Z_][a-zA-Z0-9_]*$'
	return re.match(pattern, text)

class DataTypes:
	"""
	A class to define and manage data types for database fields.

	This class provides methods to specify various data types and their attributes for use in 
	database schema definitions. It supports a wide range of data types, including numeric, 
	string, date, and binary types, and allows for the creation of constraints such as primary 
	keys and foreign keys.

	Args:
		dn: The name of the data type or database.

	Attributes:
		__TC_DATANAME__ (str): The name of the database type being used.
	"""

	def __init__(self,dn):
		self.__TC_DATANAME__ = dn

	# Строки запроса

	def NULL(self, x: bool = True) -> str:
		"""
		Generate a SQL representation of a NULL constraint.

		This function returns the appropriate SQL string for a NULL constraint based on the 
		provided boolean value. If the value is True, it returns 'NULL'; if False, it returns 
		'NOT NULL', indicating whether a column can accept null values.

		Args:
			x (bool, optional): A flag indicating whether the column should allow NULL values. 
								Defaults to True.

		Returns:
			str: 'NULL' if x is True, 'NOT NULL' if x is False, or None if x is None.
		"""
		if x is None:
			return None
		return 'NULL' if x else 'NOT NULL'

	def DEFAULT(self, x=None) -> str:
		"""
		Generate a SQL representation of a default value for a column.

		This function constructs the appropriate SQL syntax for setting a default value based on 
		the type of the provided value. It handles various data types, including strings, booleans, 
		numbers, and complex types like lists and dictionaries, returning the corresponding SQL 
		representation.

		Args:
			x: The value to be set as the default. Can be of various types including str, bool, 
			int, float, Decimal, list, tuple, dict, datetime, date, or time.

		Returns:
			str: The SQL representation of the default value.

		Raises:
			None: This function does not raise exceptions but may return a default representation 
				for unsupported types.
		"""
		if isinstance(x,str):
			defa = f"DEFAULT({repr(x)})"
		elif isinstance(x,bool):
			defa = f"DEFAULT({int(x)})"
		elif isinstance(x,(int, float)):
			defa = f"DEFAULT({x})"
		elif isinstance(x,Decimal):
			defa = f"DEFAULT({float(x)})"
		elif isinstance(x,(list, tuple, dict)):
			defa = f"DEFAULT('{json.dumps(x, ensure_ascii=False)}')"
		elif isinstance(x,(datetime, date, time)):
			defa = f"DEFAULT('{x}')"
		else:
			defa = "DEFAULT(NULL)"
		if self.__TC_DATANAME__.lower() in ['mysql','sqlserver','postgresql']:
			return re.sub(r"DEFAULT\(([^)]+)\)", r"DEFAULT \1", defa)
		return defa

	def PRIMARY(self, x: bool = True) -> Union[str, None]:
		"""
		Generate a SQL representation of a primary key constraint.

		This function returns the SQL syntax for defining a primary key based on the provided boolean value. 
		If the value is True, it returns 'PRIMARY KEY'; if False, it returns None, indicating that no primary key 
		constraint should be applied.

		Args:
			x (bool, optional): A flag indicating whether the column should be a primary key. Defaults to True.

		Returns:
			Union[str, None]: 'PRIMARY KEY' if x is True, None if x is False.
		"""
		return 'PRIMARY KEY' if x is not None else None

	def UNIQUE(self, x: bool = True) -> Union[str, None]:
		"""
		Generate a SQL representation of a unique constraint.

		This function returns the SQL syntax for defining a unique constraint based on the provided boolean value. 
		If the value is True, it returns 'UNIQUE'; if False, it returns None, indicating that no unique constraint 
		should be applied.

		Args:
			x (bool, optional): A flag indicating whether the column should have a unique constraint. Defaults to True.

		Returns:
			Union[str, None]: 'UNIQUE' if x is True, None if x is False.
		"""
		return 'UNIQUE' if x is not None else None

	def AUTO(self, x: bool = True) -> Union[str, None]:
		"""
		Generate a SQL representation of an auto-increment constraint for a column.

		This function returns the appropriate SQL syntax for defining an auto-increment behavior based 
		on the specified database type and the provided boolean value. It supports different SQL dialects, 
		returning the correct syntax for SQLite, MySQL, and SQL Server.

		Args:
			x (bool, optional): A flag indicating whether the column should be set to auto-increment. 
								Defaults to True.

		Returns:
			Union[str, None]: The SQL syntax for auto-increment if applicable, or None if not.
		"""
		if self.__TC_DATANAME__.lower() in ['sqlite']:
			return "AUTOINCREMENT" if x else None
		elif self.__TC_DATANAME__.lower() in ['mysql']:
			return "AUTO_INCREMENT" if x is not None else None
		elif self.__TC_DATANAME__.lower() in ['sqlserver']:
			return "IDENTITY(1,1)" if x is not None else None

	def COMMENT(self, x: str = None) -> str:
		"""
		Generate a SQL representation of a comment for a column.

		This function constructs the appropriate SQL syntax for adding a comment to a column based 
		on the provided string. It ensures compatibility with specific database types, modifying the 
		syntax as necessary for MySQL and SQL Server.

		Args:
			x (str, optional): The comment text to be added to the column. Defaults to None.

		Returns:
			str: The SQL syntax for the comment, or None if no comment is provided.
		"""
		comm = f"COMMENT('{x}')" if x is not None else None
		if self.__TC_DATANAME__.lower() in ['mysql','sqlserver']:
			return re.sub(r"COMMENT\(([^)]+)\)", r"COMMENT \1", comm)
		return comm

	def CHECK(self, column: str, operator: str, x=None) -> str:
		"""
		Generate a SQL CHECK constraint for a specified column.

		This function constructs the SQL syntax for a CHECK constraint based on the provided column, 
		operator, and value. It handles various data types, including strings, booleans, numbers, 
		and date/time objects, returning the appropriate SQL representation.

		Args:
			column (str): The name of the column to apply the CHECK constraint to.
			operator (str): The operator to use in the CHECK condition (e.g., '=', '>', '<').
			x: The value to compare against the column. Can be of various types including str, bool, 
			int, float, datetime, date, or time.

		Returns:
			str: The SQL syntax for the CHECK constraint.
		"""
		if type(x) is str:
			return f"CHECK({column} {operator} '{x}')"
		elif type(x) is bool:
			return f"CHECK({column} {operator} {int(x)})"
		elif isinstance(x,(int, float)):
			return f"CHECK({column} {operator} {x})"
		elif isinstance(x,(datetime, date, time)):
			return f"CHECK({column} {operator} '{x}')"
		else:
			return f"CHECK({column} {operator} NULL)"

	def FOREIGN(self, column: str, references: pd.DataFrame, name: str = '', table: pd.DataFrame = None) -> str:
		"""
		Generate a SQL representation of a foreign key constraint for a column.

		This function constructs the SQL syntax for defining a foreign key relationship based on the 
		specified column and references to another table. It ensures that the provided names and 
		structures are valid and returns the appropriate SQL representation if the database type 
		supports foreign keys.

		Args:
			column (str): The name of the column to which the foreign key constraint will be applied.
			references (pd.DataFrame): The DataFrame representing the referenced table.
			name (str, optional): The name of the foreign key constraint. Defaults to an empty string.
			table (pd.DataFrame, optional): The DataFrame representing the current table. Defaults to None.

		Returns:
			str: The SQL syntax for the foreign key constraint, or an empty string if not applicable.

		Raises:
			QueryException: If the database type does not support foreign keys, if the name is not a string, 
							if the column does not exist, or if the referenced table is missing a primary key.
		"""
		if self.__TC_DATANAME__.lower() not in ['mysql','sqlserver']:
			return ''
		prefix = getattr(self,'_tc_prefix_','').strip()
		prefix = (prefix if __check_variable_name__(prefix.strip()) else '') if isinstance(prefix,str) else ''
		if not isinstance(name,str):
			raise QueryException(f'"name" must be a string')
		if not __check_variable_name__(f'{name}') and len(name.strip())>0:
			raise QueryException(f"The '{name}' data base name must follow the variable creation rules")
		if table is None:
			if not hasattr(self,column):
				raise QueryException(f'Column "{column}" was not found or does not exist')
		else:
			if not table.is_column(column):
				raise QueryException(f'Column "{column}" was not found or does not exist')
		if not isinstance(references,pd.DataFrame):
			raise QueryException(f'Table was not found or does not exist')
		prime_key = tuple(dict(filter(lambda x: x[1].get('is_primary',False) ,references.types().items())).keys())
		if len(prime_key)==0:
			raise QueryException(f'Table "{references.table}" is missing a primary key')
		return f"""{name} FOREIGN KEY ({column}) REFERENCES {references.table} ({prime_key[0]})""".strip()

	def FOREIGN(self, column: str, references: pd.DataFrame, name: str = '', table: pd.DataFrame = None) -> str:
		"""
		Generate a SQL representation of a foreign key constraint for a column.

		This function constructs the SQL syntax for defining a foreign key relationship based on the 
		specified column and references to another table. It ensures that the provided names and 
		structures are valid and returns the appropriate SQL representation if the database type 
		supports foreign keys.

		Args:
			column (str): The name of the column to which the foreign key constraint will be applied.
			references (pd.DataFrame): The DataFrame representing the referenced table.
			name (str, optional): The name of the foreign key constraint. Defaults to an empty string.
			table (pd.DataFrame, optional): The DataFrame representing the current table. Defaults to None.

		Returns:
			str: The SQL syntax for the foreign key constraint, or an empty string if not applicable.

		Raises:
			QueryException: If the database type does not support foreign keys, if the name is not a string, 
							if the column does not exist, or if the referenced table is missing a primary key.
		"""
		if self.__TC_DATANAME__.lower() not in ['sqlite','postgresql']:
			return ''
		if not isinstance(references,pd.DataFrame):
			raise QueryException(f'Table was not found or does not exist')
		prime_key = tuple(dict(filter(lambda x: x[1].get('is_primary',False) ,references.types().items())).keys())
		if len(prime_key)==0:
			raise QueryException(f'Table "{references.table}" is missing a primary key')
		return f"""REFERENCES {references.table} ({prime_key[0]})"""

	def COLLATE(self, func: str = None):
		"""
		Generate a SQL COLLATE clause for string comparison.

		This function constructs the appropriate SQL syntax for a COLLATE clause based on the specified 
		collation method, ensuring compatibility with the database type. It validates the provided 
		collation method against a predefined list for SQLite and defaults to 'BINARY' if no method is specified.

		Args:
			func (str, optional): The collation method to be used. Defaults to None.

		Returns:
			str: The SQL COLLATE clause, or None if the database type does not support it.

		Raises:
			QueryException: If the specified collation method is not valid for the database type.
		"""
		if self.__TC_DATANAME__.lower() in ['sqlite']:
			if func is not None:
				if func.strip().upper() not in ['BINARY','NOCASE','RTRIM','LIKE','LOCALIZED']:
					raise QueryException(f'COLLATE list does not have this method "{func.strip().upper()}"')
			return f'COLLATE {func.strip().upper() if func else "BINARY"}'
		elif self.__TC_DATANAME__.lower() in ['mysql']:
			pass

	def ENGINES(self, x: str = None) -> Union[str, None]:
		"""
		Generate a SQL ENGINE clause for the specified database table.

		This function constructs the SQL syntax for defining the storage engine of a table based on 
		the provided engine type. It supports various engine types for MySQL and defaults to 'InnoDB' 
		if no valid engine type is specified.

		Args:
			x (str, optional): The name of the storage engine to be used. Defaults to None.

		Returns:
			Union[str, None]: The SQL ENGINE clause for the table, or None if the database type is not MySQL.

		Raises:
			None: This function does not raise exceptions but may return a default engine if no valid 
				engine type is provided.
		"""
		if self.__TC_DATANAME__.lower() in ['mysql']:
			if x is not None or x in ['InnoDB','MyISAM','Memory','CSV','Archive','Blackhole','NDB','Merge','Federated','Example']:
				return f'ENGINE = {x}'
			return 'ENGINE = InnoDB'

	def TRIGGER(self) -> Union[str, None]:
		"""
		Generate a SQL representation of a trigger for the specified database.

		This function returns the SQL syntax for defining a trigger if the database type is PostgreSQL. 
		If the database type is not supported, it returns None.

		Returns:
			Union[str, None]: The SQL representation of a trigger for PostgreSQL, or None if not applicable.
		"""
		if self.__TC_DATANAME__.lower() in ['postgresql']:
			return 'trigger'

	# Логические данные

	def BOOL(self) -> Union[str, None]:
		"""
		Generate a SQL representation of a boolean data type.

		This function returns the appropriate SQL syntax for a boolean data type based on the 
		specified database type. It supports different representations for MySQL, SQLite, PostgreSQL, 
		and SQL Server.

		Returns:
			Union[str, None]: The SQL representation of the boolean data type, or None if the database type is not supported.
		"""
		if self.__TC_DATANAME__.lower() in ['mysql']:
			return 'TINYINT(1)'
		elif self.__TC_DATANAME__.lower() in ['sqlite','postgresql']:
			return 'BOOLEAN'
		elif self.__TC_DATANAME__.lower() in ['sqlserver']:
			return 'BIT(1)'

	# Числовые данные

	def SERIAL(self) -> Union[str, None]:
		"""
		Generate a SQL representation of a serial data type.

		This function returns the SQL syntax for a serial data type, which is used for auto-incrementing 
		integer values in PostgreSQL. If the database type is not PostgreSQL, it returns None.

		Returns:
			Union[str, None]: The SQL representation of the serial data type for PostgreSQL, or None if not applicable.
		"""
		if self.__TC_DATANAME__.lower() in ['postgresql']:
			return 'SERIAL'

	def SMALLSERIAL(self) -> Union[str, None]:
		"""
		Generate a SQL representation of a small serial data type.

		This function returns the SQL syntax for a small serial data type, which is used for 
		auto-incrementing small integer values in PostgreSQL. If the database type is not PostgreSQL, 
		it returns None.

		Returns:
			Union[str, None]: The SQL representation of the small serial data type for PostgreSQL, 
							or None if not applicable.
		"""
		if self.__TC_DATANAME__.lower() in ['postgresql']:
			return 'SMALLSERIAL'

	def BIGSERIAL(self) -> Union[str, None]:
		"""
		Generate a SQL representation of a big serial data type.

		This function returns the SQL syntax for a big serial data type, which is used for 
		auto-incrementing large integer values in PostgreSQL. If the database type is not PostgreSQL, 
		it returns None.

		Returns:
			Union[str, None]: The SQL representation of the big serial data type for PostgreSQL, 
							or None if not applicable.
		"""
		if self.__TC_DATANAME__.lower() in ['postgresql']:
			return 'BIGSERIAL'

	def INTEGER(self, x: int = None) -> Union[str, None]:
		"""
		Generate a SQL representation of an integer data type.

		This function returns the SQL syntax for an integer data type, optionally specifying a size 
		constraint based on the provided integer value. It ensures that the value falls within the 
		valid range for integers and returns a default representation if no size is specified.

		Args:
			x (int, optional): The size constraint for the integer type. Defaults to None.

		Returns:
			Union[str, None]: The SQL representation of the integer data type, or None if the size 
							constraint is out of range.
		"""
		if self.__TC_DATANAME__.lower() in ['sqlite','postgresql']:
			return (f'INTEGER({x})' if x>-2147483648 and x <2147483648 else None) if x is not None else 'INTEGER'

	def INT(self, x: int = None) -> Union[str, None]:
		"""
		Generate a SQL representation of an integer data type.

		This function returns the SQL syntax for an integer data type, optionally specifying a size 
		constraint based on the provided integer value. It ensures that the value falls within the 
		valid range for integers and returns a default representation if no size is specified.

		Args:
			x (int, optional): The size constraint for the integer type. Defaults to None.

		Returns:
			Union[str, None]: The SQL representation of the integer data type, or None if the size 
							constraint is out of range.
		"""
		if self.__TC_DATANAME__.lower() in ['mysql','sqlite','sqlserver']:
			return (f'INT({x})' if x>-2147483648 and x <2147483648 else None) if x is not None else 'INT'

	def TINYINT(self, x: int = None) -> Union[str, None]:
		"""
		Generate a SQL representation of a tiny integer data type.

		This function returns the SQL syntax for a tiny integer data type, optionally specifying a size 
		constraint based on the provided integer value. It ensures that the value falls within the valid 
		range for tiny integers and returns a default representation if no size is specified.

		Args:
			x (int, optional): The size constraint for the tiny integer type. Defaults to None.

		Returns:
			Union[str, None]: The SQL representation of the tiny integer data type, or None if the size 
							constraint is out of range.
		"""
		if self.__TC_DATANAME__.lower() in ['mysql','sqlserver']:
			return (f'TINYINT({x})' if x>-128 and x <128 else None) if x is not None else 'TINYINT'

	def BIT(self, x: int = None) -> Union[str, None]:
		"""
		Generate a SQL representation of a bit data type.

		This function returns the SQL syntax for a bit data type, optionally specifying a size 
		constraint based on the provided integer value. It ensures that the value falls within the 
		valid range for bits and returns a default representation if no size is specified.

		Args:
			x (int, optional): The size constraint for the bit type. Defaults to None.

		Returns:
			Union[str, None]: The SQL representation of the bit data type, or None if the size 
							constraint is out of range.
		"""
		if self.__TC_DATANAME__.lower() in ['mysql']:
			return (f'BIT({x})' if x>-128 and x <128 else None) if x is not None else 'BIT'

	def SMALLINT(self, x: int = None) -> Union[str, None]:
		"""
		Generate a SQL representation of a small integer data type.

		This function returns the SQL syntax for a small integer data type, optionally specifying a size 
		constraint based on the provided integer value. It ensures that the value falls within the valid 
		range for small integers and returns a default representation if no size is specified.

		Args:
			x (int, optional): The size constraint for the small integer type. Defaults to None.

		Returns:
			Union[str, None]: The SQL representation of the small integer data type, or None if the size 
							constraint is out of range.
		"""
		if self.__TC_DATANAME__.lower() in ['mysql','sqlserver','postgresql']:
			return (f'SMALLINT({x})' if x>-32768 and x <32768 else None) if x is not None else 'SMALLINT'

	def BIGINT(self, x: int = None) -> Union[str, None]:
		"""
		Generate a SQL representation of a big integer data type.

		This function returns the SQL syntax for a big integer data type, optionally specifying a size 
		constraint based on the provided integer value. It ensures that the value falls within the valid 
		range for big integers and returns a default representation if no size is specified.

		Args:
			x (int, optional): The size constraint for the big integer type. Defaults to None.

		Returns:
			Union[str, None]: The SQL representation of the big integer data type, or None if the size 
							constraint is out of range.
		"""
		if self.__TC_DATANAME__.lower() in ['mysql','sqlserver','postgresql']:
			return (f'BIGINT({x})' if x>-9223372036854775808 and x <9223372036854775808 else None) if x is not None else 'BIGINT'

	def MEDIUMINT(self, x: int = None) -> Union[str, None]:
		"""
		Generate a SQL representation of a medium integer data type.

		This function returns the SQL syntax for a medium integer data type, optionally specifying a size 
		constraint based on the provided integer value. It ensures that the value falls within the valid 
		range for medium integers and returns a default representation if no size is specified.

		Args:
			x (int, optional): The size constraint for the medium integer type. Defaults to None.

		Returns:
			Union[str, None]: The SQL representation of the medium integer data type, or None if the size 
							constraint is out of range.
		"""
		if self.__TC_DATANAME__.lower() in ['mysql']:
			return (f'MEDIUMINT({x})' if x>-32768 and x <32768 else None) if x is not None else 'MEDIUMINT'

	def NUMERIC(self, x: int, d: int = 0) -> Union[str, None]:
		"""
		Generate a SQL representation of a numeric data type.

		This function returns the SQL syntax for a numeric data type, specifying both the precision 
		and scale based on the provided integer values. It is specifically designed for use with 
		PostgreSQL.

		Args:
			x (int): The precision of the numeric type.
			d (int, optional): The scale of the numeric type. Defaults to 0.

		Returns:
			Union[str, None]: The SQL representation of the numeric data type for PostgreSQL, or None if not applicable.
		"""
		if self.__TC_DATANAME__.lower() in ['postgresql']:
			return f'NUMERIC({x},{d})'

	def FLOAT(self, x: int = None) -> Union[str, None]:
		"""
		Generate a SQL representation of a floating-point data type.

		This function returns the SQL syntax for a floating-point data type, specifying an optional 
		size constraint based on the provided integer value. It supports different representations 
		for MySQL, SQLite, and PostgreSQL, returning the appropriate syntax based on the database type.

		Args:
			x (int, optional): The size constraint for the floating-point type. Defaults to None.

		Returns:
			Union[str, None]: The SQL representation of the floating-point data type, or None if the size 
							constraint is out of range.
		"""
		if self.__TC_DATANAME__.lower() in ['mysql']:
			return (f'FLOAT({x})' if x>-1.175494351*(10**(-39)) and x <1.175494351*(10**(-39)) else None) if x is not None else 'FLOAT'
		elif self.__TC_DATANAME__.lower() in ['sqlite','postgresql']:
			return (f'REAL({x})' if x>-1.175494351*(10**(-39)) and x <1.175494351*(10**(-39)) else None) if x is not None else 'REAL'

	def DOUBLE(self, x: int = None, d: int = 2) -> Union[str, None]:
		"""
		Generate a SQL representation of a double precision floating-point data type.

		This function returns the SQL syntax for a double data type, optionally specifying a size 
		constraint based on the provided integer value. It supports different representations for 
		MySQL, SQLite, and PostgreSQL, returning the appropriate syntax based on the database type.

		Args:
			x (int, optional): The size constraint for the double type. Defaults to None.
			d (int, optional): The number of decimal places. Defaults to 2.

		Returns:
			Union[str, None]: The SQL representation of the double data type, or None if the size 
							constraint is out of range for MySQL or SQLite.
		"""
		if self.__TC_DATANAME__.lower() in ['mysql','sqlite']:
			return (f'DOUBLE({x})' if x>-2.2250738585072015*(10**(-308)) and x <2.2250738585072015*(10**(-308)) else None) if x is not None else 'DOUBLE'
		elif self.__TC_DATANAME__.lower() in ['postgresql']:
			return f'DOUBLE PRECISION'

	def DECIMAL(self, x: int = None, d: int = 2) -> Union[str, None]:
		"""
		Generate a SQL representation of a decimal data type.

		This function returns the SQL syntax for a decimal data type, specifying both the precision 
		and scale based on the provided integer values. It ensures that the precision falls within 
		a valid range and returns a default representation if no size is specified.

		Args:
			x (int, optional): The precision of the decimal type. Defaults to None.
			d (int, optional): The scale of the decimal type. Defaults to 2.

		Returns:
			Union[str, None]: The SQL representation of the decimal data type, or None if the precision 
							constraint is out of range.
		"""
		if self.__TC_DATANAME__.lower() in ['mysql','sqlite','postgresql']:
			return (f'DECIMAL({x},{d})' if x>-99.99 and x <99.99 else None) if x is not None else 'DECIMAL'

	# Текстовые данные

	def STRING(self, n: int = None) -> Union[str, None]:
		"""
		Generate a SQL representation of a string data type.

		This function returns the SQL syntax for a string data type, optionally specifying a length 
		constraint based on the provided integer value. It is specifically designed for use with 
		SQLite, returning a default representation if no length is specified.

		Args:
			n (int, optional): The length constraint for the string type. Defaults to None.

		Returns:
			Union[str, None]: The SQL representation of the string data type, or None if not applicable.
		"""
		if self.__TC_DATANAME__.lower() in ['sqlite']:
			return (f'STRING({n})') if n != None else 'STRING'

	def TEXT(self) -> Union[str, None]:
		"""
		Generate a SQL representation of a text data type.

		This function returns the SQL syntax for a text data type, which is used for storing 
		large amounts of text data in the database. It is compatible with MySQL, SQLite, and 
		PostgreSQL, returning 'TEXT' if the database type is supported.

		Returns:
			Union[str, None]: The SQL representation of the text data type, or None if the database type is not supported.
		"""
		if self.__TC_DATANAME__.lower() in ['mysql','sqlite','postgresql']:
			return 'TEXT'

	def TINYTEXT(self) -> Union[str, None]:
		"""
		Generate a SQL representation of a tiny text data type.

		This function returns the SQL syntax for a tiny text data type, which is used for storing 
		small amounts of text data in the database. It is specifically designed for use with MySQL, 
		returning 'TINYTEXT' if the database type is supported.

		Returns:
			Union[str, None]: The SQL representation of the tiny text data type, or None if the database type is not supported.
		"""
		if self.__TC_DATANAME__.lower() in ['mysql']:
			return 'TINYTEXT'

	def MEDIUMTEXT(self) -> Union[str, None]:
		"""
		Generate a SQL representation of a medium text data type.

		This function returns the SQL syntax for a medium text data type, which is used for storing 
		larger amounts of text data in the database. It is specifically designed for use with MySQL, 
		returning 'MEDIUMTEXT' if the database type is supported.

		Returns:
			Union[str, None]: The SQL representation of the medium text data type, or None if the database type is not supported.
		"""
		if self.__TC_DATANAME__.lower() in ['mysql']:
			return 'MEDIUMTEXT'

	def LONGTEXT(self) -> Union[str, None]:
		"""
		Generate a SQL representation of a long text data type.

		This function returns the SQL syntax for a long text data type, which is used for storing 
		very large amounts of text data in the database. It is specifically designed for use with MySQL, 
		returning 'LONGTEXT' if the database type is supported.

		Returns:
			Union[str, None]: The SQL representation of the long text data type, or None if the database type is not supported.
		"""
		if self.__TC_DATANAME__.lower() in ['mysql']:
			return 'LONGTEXT'

	def CHAR(self, n: int) -> Union[str, None]:
		"""
		Generate a SQL representation of a character data type with a specified length.

		This function returns the SQL syntax for a character data type, specifying the length based 
		on the provided integer value. It supports different representations for MySQL, SQLite, and 
		PostgreSQL.

		Args:
			n (int): The length constraint for the character type.

		Returns:
			Union[str, None]: The SQL representation of the character data type with the specified length, 
							or None if the database type is not supported.
		"""
		if self.__TC_DATANAME__.lower() in ['mysql','sqlite']:
			return f'CHAR({n})' if type(n) is int else None
		elif self.__TC_DATANAME__.lower() in ['postgresql']:
			return f'CHARACTER({n})'

	def VARCHAR(self, n: int) -> Union[str, None]:
		"""
		Generate a SQL representation of a variable character data type with a specified length.

		This function returns the SQL syntax for a variable character data type, specifying the length 
		based on the provided integer value. It supports different representations for MySQL, SQLite, 
		and PostgreSQL.

		Args:
			n (int): The length constraint for the variable character type.

		Returns:
			Union[str, None]: The SQL representation of the variable character data type with the specified length, 
							or None if the database type is not supported.
		"""
		if self.__TC_DATANAME__.lower() in ['mysql','sqlite']:
			return f'VARCHAR({n})' if type(n) is int else None
		elif self.__TC_DATANAME__.lower() in ['postgresql']:
			return f'CHARACTER VARYING({n})'

	# Структурные данные

	def JSON(self) -> Union[str, None]:
		"""
		Generate a SQL representation of a JSON data type.

		This function returns the SQL syntax for a JSON data type, which is used for storing 
		structured data in a flexible format. It is compatible with MySQL and PostgreSQL, returning 
		'JSON' if the database type is supported.

		Returns:
			Union[str, None]: The SQL representation of the JSON data type, or None if the database type is not supported.
		"""
		if self.__TC_DATANAME__.lower() in ['mysql','postgresql']:
			return 'JSON'

	# Временые данные

	def DATE(self) -> Union[str, None]:
		"""
		Generate a SQL representation of a date data type.

		This function returns the SQL syntax for a date data type, which is used for storing 
		date values in the database. It is compatible with MySQL, SQLite, and PostgreSQL, returning 
		'DATE' if the database type is supported.

		Returns:
			Union[str, None]: The SQL representation of the date data type, or None if the database type is not supported.
		"""
		if self.__TC_DATANAME__.lower() in ['mysql','sqlite','postgresql']:
			return 'DATE'

	def TIME(self) -> Union[str, None]:
		"""
		Generate a SQL representation of a time data type.

		This function returns the SQL syntax for a time data type, which is used for storing 
		time values in the database. It is compatible with MySQL, SQLite, and PostgreSQL, returning 
		'TIME' if the database type is supported.

		Returns:
			Union[str, None]: The SQL representation of the time data type, or None if the database type is not supported.
		"""
		if self.__TC_DATANAME__.lower() in ['mysql','sqlite','postgresql']:
			return 'TIME'

	def DATETIME(self) -> Union[str, None]:
		"""
		Generate a SQL representation of a datetime data type.

		This function returns the SQL syntax for a datetime data type, which is used for storing 
		date and time values in the database. It is compatible with MySQL, SQLite, and PostgreSQL, returning 
		'DATETIME' if the database type is supported.

		Returns:
			Union[str, None]: The SQL representation of the datetime data type, or None if the database type is not supported.
		"""
		if self.__TC_DATANAME__.lower() in ['mysql','sqlite','postgresql']:
			return 'DATETIME'

	def TIMESTAMP(self) -> Union[str, None]:
		"""
		Generate a SQL representation of a timestamp data type.

		This function returns the SQL syntax for a timestamp data type, which is used for storing 
		both date and time values with time zone information in the database. It is compatible with 
		MySQL and PostgreSQL, returning 'TIMESTAMP' if the database type is supported.

		Returns:
			Union[str, None]: The SQL representation of the timestamp data type, or None if the database type is not supported.
		"""
		if self.__TC_DATANAME__.lower() in ['mysql','postgresql']:
			return 'TIMESTAMP'

	def YEAR(self) -> Union[str, None]:
		"""
		Generate a SQL representation of a year data type.

		This function returns the SQL syntax for a year data type, which is used for storing 
		year values in the database. It is specifically designed for use with MySQL, returning 
		'YEAR' if the database type is supported.

		Returns:
			Union[str, None]: The SQL representation of the year data type, or None if the database type is not supported.
		"""
		if self.__TC_DATANAME__.lower() in ['mysql']:
			return 'YEAR'

	def INTERVAL(self) -> Union[str, None]:
		"""
		Generate a SQL representation of an interval data type.

		This function returns the SQL syntax for an interval data type, which is used for storing 
		time intervals in the database. It is specifically designed for use with PostgreSQL, returning 
		'INTERVAL' if the database type is supported.

		Returns:
			Union[str, None]: The SQL representation of the interval data type, or None if the database type is not supported.
		"""
		if self.__TC_DATANAME__.lower() in ['postgresql']:
			return 'INTERVAL'

	# Бинарные типы данных

	def TINYBLOB(self) -> Union[str, None]:
		"""
		Generate a SQL representation of a tiny blob data type.

		This function returns the SQL syntax for a tiny blob data type, which is used for storing 
		small amounts of binary data in the database. It is specifically designed for use with MySQL 
		and SQLite, returning 'TINYBLOB' if the database type is supported.

		Returns:
			Union[str, None]: The SQL representation of the tiny blob data type, or None if the database type is not supported.
		"""
		if self.__TC_DATANAME__.lower() in ['mysql','sqlite']:
			return 'TINYBLOB'

	def BLOB(self) -> Union[str, None]:
		"""
		Generate a SQL representation of a blob data type.

		This function returns the SQL syntax for a blob data type, which is used for storing 
		large amounts of binary data in the database. It is specifically designed for use with 
		MySQL and SQLite, returning 'BLOB' if the database type is supported.

		Returns:
			Union[str, None]: The SQL representation of the blob data type, or None if the database type is not supported.
		"""
		if self.__TC_DATANAME__.lower() in ['mysql','sqlite']:
			return 'BLOB'

	def MEDIUMBLOB(self) -> Union[str, None]:
		"""
		Generate a SQL representation of a medium blob data type.

		This function returns the SQL syntax for a medium blob data type, which is used for storing 
		medium-sized amounts of binary data in the database. It is specifically designed for use with 
		MySQL and SQLite, returning 'MEDIUMBLOB' if the database type is supported.

		Returns:
			Union[str, None]: The SQL representation of the medium blob data type, or None if the database type is not supported.
		"""
		if self.__TC_DATANAME__.lower() in ['mysql','sqlite']:
			return 'MEDIUMBLOB'

	def LONGBLOB(self) -> Union[str, None]:
		"""
		Generate a SQL representation of a long blob data type.

		This function returns the SQL syntax for a long blob data type, which is used for storing 
		large amounts of binary data in the database. It is specifically designed for use with 
		MySQL and SQLite, returning 'LONGBLOB' if the database type is supported.

		Returns:
			Union[str, None]: The SQL representation of the long blob data type, or None if the database type is not supported.
		"""
		if self.__TC_DATANAME__.lower() in ['mysql','sqlite']:
			return 'LONGBLOB'

	def LONGBLOB(self) -> Union[str, None]:
		"""
		Generate a SQL representation of a long blob data type.

		This function returns the SQL syntax for a long blob data type, which is used for storing 
		large amounts of binary data in the database. It is specifically designed for use with 
		MySQL and SQLite, returning 'LONGBLOB' if the database type is supported.

		Returns:
			Union[str, None]: The SQL representation of the long blob data type, or None if the database type is not supported.
		"""
		if self.__TC_DATANAME__.lower() in ['mysql','sqlite']:
			return 'LARGEBLOB'

	def BYTEA(self) -> Union[str, None]:
		"""
		Generate a SQL representation of a bytea data type.

		This function returns the SQL syntax for a bytea data type, which is used for storing 
		binary data in PostgreSQL. It returns 'BYTEA' if the database type is supported.

		Returns:
			Union[str, None]: The SQL representation of the bytea data type, or None if the database type is not supported.
		"""
		if self.__TC_DATANAME__.lower() in ['postgresql']:
			return 'BYTEA'

	# Составные типы

	def ENUM(self) -> Union[str, None]:
		"""
		Generate a SQL representation of an enum data type.

		This function returns the SQL syntax for an enum data type, which is used for defining 
		a column that can hold a predefined set of values in MySQL. It returns 'ENUM' if the 
		database type is supported.

		Returns:
			Union[str, None]: The SQL representation of the enum data type, or None if the database type is not supported.
		"""
		if self.__TC_DATANAME__.lower() in ['mysql']:
			return 'ENUM'

	def SET(self) -> Union[str, None]:
		"""
		Generate a SQL representation of a set data type.

		This function returns the SQL syntax for a set data type, which allows a column to hold 
		multiple predefined values in MySQL. It returns 'SET' if the database type is supported.

		Returns:
			Union[str, None]: The SQL representation of the set data type, or None if the database type is not supported.
		"""
		if self.__TC_DATANAME__.lower() in ['mysql']:
			return 'SET'

class CollationMappings:
	"""
	A class to define and manage collation mappings for various database character sets.

	This class provides a collection of predefined collation mappings for different character sets 
	used in databases. It includes methods to retrieve available collations and check for specific 
	collation support based on the database type.

	Attributes:
		big5 (list): Collations for the Big5 character set.
		dec8 (list): Collations for the DEC8 character set.
		cp850 (list): Collations for the CP850 character set.
		hp8 (list): Collations for the HP8 character set.
		koi8r (list): Collations for the KOI8-R character set.
		latin1 (list): Collations for the Latin1 character set.
		latin2 (list): Collations for the Latin2 character set.
		swe7 (list): Collations for the SWE7 character set.
		ascii (list): Collations for the ASCII character set.
		ujis (list): Collations for the UJIS character set.
		sjis (list): Collations for the SJIS character set.
		hebrew (list): Collations for the Hebrew character set.
		tis620 (list): Collations for the TIS620 character set.
		euckr (list): Collations for the EUCKR character set.
		koi8u (list): Collations for the KOI8-U character set.
		gb2312 (list): Collations for the GB2312 character set.
		greek (list): Collations for the Greek character set.
		cp1250 (list): Collations for the CP1250 character set.
		gbk (list): Collations for the GBK character set.
		latin5 (list): Collations for the Latin5 character set.
		armscii8 (list): Collations for the ARMSCII-8 character set.
		utf8 (list): Collations for the UTF-8 character set.
		ucs2 (list): Collations for the UCS2 character set.
		cp866 (list): Collations for the CP866 character set.
		keybcs2 (list): Collations for the KEYBCS2 character set.
		macce (list): Collations for the MACCE character set.
		macroman (list): Collations for the MACROMAN character set.
		cp852 (list): Collations for the CP852 character set.
		latin7 (list): Collations for the Latin7 character set.
		utf8mb4 (list): Collations for the UTF8MB4 character set.
		cp1251 (list): Collations for the CP1251 character set.
		utf16 (list): Collations for the UTF16 character set.
		utf16le (list): Collations for the UTF16LE character set.
		cp1256 (list): Collations for the CP1256 character set.
		cp1257 (list): Collations for the CP1257 character set.
		utf32 (list): Collations for the UTF32 character set.
	"""

	big5 = ["big5_chinese_ci", "big5_bin"]
	dec8 = ["dec8_swedish_ci", "dec8_bin"]
	cp850 = ["cp850_general_ci", "cp850_bin"]
	hp8 = ["hp8_english_ci", "hp8_bin"]
	koi8r = ["koi8r_general_ci", "koi8r_bin"]
	latin1 = ["latin1_german1_ci", "latin1_swedish_ci", "latin1_danish_ci", "latin1_german2_ci",
			  "latin1_bin", "latin1_general_ci", "latin1_general_cs", "latin1_spanish_ci"]
	latin2 = ["latin2_czech_cs", "latin2_general_ci", "latin2_hungarian_ci", "latin2_croatian_ci", "latin2_bin"]
	swe7 = ["swe7_swedish_ci", "swe7_bin"]
	ascii = ["ascii_general_ci", "ascii_bin"]
	ujis = ["ujis_japanese_ci", "ujis_bin"]
	sjis = ["sjis_japanese_ci", "sjis_bin"]
	hebrew = ["hebrew_general_ci", "hebrew_bin"]
	tis620 = ["tis620_thai_ci", "tis620_bin"]
	euckr = ["euckr_korean_ci", "euckr_bin"]
	koi8u = ["koi8u_general_ci", "koi8u_bin"]
	gb2312 = ["gb2312_chinese_ci", "gb2312_bin"]
	greek = ["greek_general_ci", "greek_bin"]
	cp1250 = ["cp1250_general_ci", "cp1250_czech_cs", "cp1250_croatian_ci", "cp1250_bin", "cp1250_polish_ci"]
	gbk = ["gbk_chinese_ci", "gbk_bin"]
	latin5 = ["latin5_turkish_ci", "latin5_bin"]
	armscii8 = ["armscii8_general_ci", "armscii8_bin"]
	utf8 = ["utf8_general_ci", "utf8_bin", "utf8_unicode_ci", "utf8_icelandic_ci", "utf8_latvian_ci", 
			"utf8_romanian_ci", "utf8_slovenian_ci", "utf8_polish_ci", "utf8_estonian_ci", "utf8_spanish_ci", 
			"utf8_swedish_ci", "utf8_turkish_ci", "utf8_czech_ci", "utf8_danish_ci", "utf8_lithuanian_ci", 
			"utf8_slovak_ci", "utf8_spanish2_ci", "utf8_roman_ci", "utf8_persian_ci", "utf8_esperanto_ci", 
			"utf8_hungarian_ci", "utf8_sinhala_ci", "utf8_german2_ci", "utf8_croatian_ci", "utf8_unicode_520_ci", 
			"utf8_vietnamese_ci", "utf8_general_mysql500_ci"]
	ucs2 = ["ucs2_general_ci", "ucs2_bin", "ucs2_unicode_ci", "ucs2_icelandic_ci", "ucs2_latvian_ci", 
			"ucs2_romanian_ci", "ucs2_slovenian_ci", "ucs2_polish_ci", "ucs2_estonian_ci", "ucs2_spanish_ci", 
			"ucs2_swedish_ci", "ucs2_turkish_ci", "ucs2_czech_ci", "ucs2_danish_ci", "ucs2_lithuanian_ci", 
			"ucs2_slovak_ci", "ucs2_spanish2_ci", "ucs2_roman_ci", "ucs2_persian_ci", "ucs2_esperanto_ci", 
			"ucs2_hungarian_ci", "ucs2_sinhala_ci", "ucs2_german2_ci", "ucs2_croatian_ci", "ucs2_unicode_520_ci", 
			"ucs2_general_mysql500_ci"]
	cp866 = ["cp866_general_ci", "cp866_bin"]
	keybcs2 = ["keybcs2_general_ci", "keybcs2_bin"]
	macce = ["macce_general_ci", "macce_bin"]
	macroman = ["macroman_general_ci", "macroman_bin"]
	cp852 = ["cp852_general_ci", "cp852_bin"]
	latin7 = ["latin7_general_ci", "latin7_estonian_cs", "latin7_general_cs", "latin7_bin"]
	utf8mb4 = ["utf8mb4_general_ci", "utf8mb4_bin", "utf8mb4_unicode_ci", "utf8mb4_icelandic_ci", "utf8mb4_latvian_ci", 
			   "utf8mb4_romanian_ci", "utf8mb4_slovenian_ci", "utf8mb4_polish_ci", "utf8mb4_estonian_ci", "utf8mb4_spanish_ci", 
			   "utf8mb4_swedish_ci", "utf8mb4_turkish_ci", "utf8mb4_czech_ci", "utf8mb4_danish_ci", "utf8mb4_lithuanian_ci", 
			   "utf8mb4_slovak_ci", "utf8mb4_spanish2_ci", "utf8mb4_roman_ci", "utf8mb4_persian_ci", "utf8mb4_esperanto_ci", 
			   "utf8mb4_hungarian_ci", "utf8mb4_sinhala_ci", "utf8mb4_german2_ci", "utf8mb4_croatian_ci", "utf8mb4_unicode_520_ci", 
			   "utf8mb4_vietnamese_ci"]
	cp1251 = ["cp1251_bulgarian_ci", "cp1251_ukrainian_ci", "cp1251_bin", "cp1251_general_ci", "cp1251_general_cs"]
	utf16 = ["utf16_general_ci", "utf16_bin", "utf16_unicode_ci", "utf16_icelandic_ci", "utf16_latvian_ci", 
			 "utf16_romanian_ci", "utf16_slovenian_ci", "utf16_polish_ci", "utf16_estonian_ci", "utf16_spanish_ci", 
			 "utf16_swedish_ci", "utf16_turkish_ci", "utf16_czech_ci", "utf16_danish_ci", "utf16_lithuanian_ci", 
			 "utf16_slovak_ci", "utf16_spanish2_ci", "utf16_roman_ci", "utf16_persian_ci", "utf16_esperanto_ci", 
			 "utf16_hungarian_ci", "utf16_sinhala_ci", "utf16_german2_ci", "utf16_croatian_ci", "utf16_unicode_520_ci"]
	utf16le = ["utf16le_general_ci", "utf16le_bin"]
	cp1256 = ["cp1256_general_ci", "cp1256_bin"]
	cp1257 = ["cp1257_lithuanian_ci", "cp1257_bin", "cp1257_general_ci"]
	utf32 = ["utf32_general_ci", "utf32_bin", "utf32_unicode_ci", "utf32_icelandic_ci", "utf32_latvian_ci", 
			 "utf32_romanian_ci", "utf32_slovenian_ci", "utf32_polish_ci", "utf32_estonian_ci", "utf32_spanish_ci", 
			 "utf32_swedish_ci", "utf32_turkish_ci", "utf32_czech_ci", "utf32_danish_ci", "utf32_lithuanian_ci", 
			 "utf32_slovak_ci", "utf32_spanish2_ci", "utf32_roman_ci", "utf32_persian_ci", "utf32_esperanto_ci", 
			 "utf32_hungarian_ci", "utf32_sinhala_ci", "utf32_german2_ci", "utf32_croatian_ci", "utf32_unicode_520_ci"]
	binary = ["binary_general_ci", "binary_bin"]
	geostd8 = ["geostd8_general_ci", "geostd8_bin"]
	cp932 = ["cp932_japanese_ci", "cp932_bin"]
	eucjpms = ["eucjpms_japanese_ci", "eucjpms_bin"]
	gb18030 = ["gb18030_chinese_ci", "gb18030_bin", "gb18030_unicode_520_ci"]

	def __iter__(self):
		return self

	@classmethod
	def get_collations(cls, key: str):
		"""
		Retrieve the collation mappings associated with a specified key.

		This class method returns the list of collations defined for the given key in the 
		CollationMappings class. If the key does not exist, it returns an empty list.

		Args:
			key (str): The key for which to retrieve the collation mappings.

		Returns:
			list: A list of collation mappings associated with the specified key.
		"""
		return getattr(cls, key, [])

	@classmethod
	def get_collation(cls, key: str):
		"""
		Retrieve a specific collation mapping based on the provided key.

		This class method checks if the specified key exists in the list of collations associated 
		with the first part of the key (before the underscore). It returns the key if it is found; 
		otherwise, it returns None.

		Args:
			key (str): The key for which to retrieve the collation mapping.

		Returns:
			str or None: The specified key if it exists in the collation mappings, or None if it does not.
		"""
		coll = cls.get_collations(key.split('_')[0])
		return key if key in coll else None

	@classmethod
	def get(cls, key: str = 'utf8mb4'):
		"""
		Retrieve the specified key if it exists in the class attributes.

		This class method checks if the provided key is an attribute of the CollationMappings class. 
		If the key exists, it returns the key; otherwise, it returns the default value 'utf8mb4'.

		Args:
			key (str, optional): The key to retrieve. Defaults to 'utf8mb4'.

		Returns:
			str: The specified key if it exists, or 'utf8mb4' if it does not.
		"""
		return key if hasattr(cls,key) else 'utf8mb4'

class Index:
	"""
	A class representing an index for a database table.

	This class encapsulates the properties and behaviors of a database index, allowing for 
	the creation of various types of indexes with specific attributes. It provides methods to 
	define the index characteristics and generate the corresponding SQL syntax for different 
	database systems.

	Args:
		name (str): The name of the index.
		dbname (str): The name of the database.
		NOT_EXISTS (bool, optional): If True, adds 'IF NOT EXISTS' to the index creation statement. Defaults to False.
		UNIQUE (bool, optional): If True, defines the index as unique. Defaults to False.
		CLUSTERED (bool, optional): If True, defines the index as clustered. Defaults to False.
		FULLTEXT (bool, optional): If True, defines the index as a full-text index. Defaults to False.
		SPATIAL (bool, optional): If True, defines the index as a spatial index. Defaults to False.
		USING (Union[tuple, list, str], optional): The method used for the index (e.g., 'BTREE'). Defaults to 'BTREE'.
		INCLUDES (Union[tuple, list], optional): Additional columns to include in the index. Defaults to an empty list.
		WHERE (str, optional): A condition for the index. Defaults to an empty string.
		**columns: The columns to be indexed, provided as keyword arguments.

	Attributes:
		__TC_DATANAME__ (str): The name of the database type.
		name (str): The name of the index.
		using (Union[tuple, list, str]): The method used for the index.
		exists (str): SQL syntax for 'IF NOT EXISTS' if applicable.
		unique (str): SQL syntax for 'UNIQUE' if applicable.
		clustered (str): SQL syntax for 'CLUSTERED' if applicable.
		fulltext (str): SQL syntax for 'FULLTEXT' if applicable.
		spatial (str): SQL syntax for 'SPATIAL' if applicable.
		includes (Union[tuple, list]): Additional columns included in the index.
		where (str): The condition for the index.
		columns (map): The columns to be indexed formatted for SQL.
	"""

	def __init__(self, name: str, dbname: str,
			 NOT_EXISTS: bool = False,
			 UNIQUE: bool = False, CLUSTERED: bool = False, FULLTEXT: bool = False, SPATIAL: bool = False,
			 USING: Union[tuple, list, str] = 'BTREE',
			 INCLUDES: Union[tuple, list] = [],
			 WHERE: str = '',
			 **columns):
		"""
		Initialize an Index object representing a database index.

		This constructor sets up the index with its name, associated database, and various attributes 
		that define its behavior, such as uniqueness and clustering. It also validates the provided 
		parameters and prepares the index for use in SQL statements.

		Args:
			name (str): The name of the index.
			dbname (str): The name of the database.
			NOT_EXISTS (bool, optional): If True, adds 'IF NOT EXISTS' to the index creation statement. Defaults to False.
			UNIQUE (bool, optional): If True, defines the index as unique. Defaults to False.
			CLUSTERED (bool, optional): If True, defines the index as clustered. Defaults to False.
			FULLTEXT (bool, optional): If True, defines the index as a full-text index. Defaults to False.
			SPATIAL (bool, optional): If True, defines the index as a spatial index. Defaults to False.
			USING (Union[tuple, list, str], optional): The method used for the index (e.g., 'BTREE'). Defaults to 'BTREE'.
			INCLUDES (Union[tuple, list], optional): Additional columns to include in the index. Defaults to an empty list.
			WHERE (str, optional): A condition for the index. Defaults to an empty string.
			**columns: The columns to be indexed, provided as keyword arguments.

		Raises:
			QueryException: If FULLTEXT and SPATIAL are both set to True, or if any provided parameters are invalid.
		"""
		self.__TC_DATANAME__ = dbname
		self.name = name
		self.using = USING
		self.exists = 'IF NOT EXISTS' if NOT_EXISTS else ''
		self.unique = 'UNIQUE' if UNIQUE else ''
		self.clustered = 'CLUSTERED' if CLUSTERED else ''
		if FULLTEXT and SPATIAL:
			raise QueryException('You can use either FULLTEXT or SPATIAL')
		self.fulltext = 'FULLTEXT' if FULLTEXT else ''
		self.spatial = 'SPATIAL' if SPATIAL else ''

		self.includes = INCLUDES
		self.where = WHERE
		red = lambda x: map(str,filter(None,x)) if isinstance(x,(tuple,list)) else (x if isinstance(x,str) else '')
		self.columns = map(lambda kv: f'{kv[0]} {" ".join(red(kv[1])) if isinstance(kv[1],(tuple,list)) else red(kv[1])}',columns.items())

	def __str__(self):
		"""
		Generate the SQL statement for creating the index.

		This method constructs and returns the SQL syntax for creating an index based on the 
		attributes of the Index object, including its name, uniqueness, and any specified 
		conditions. It adapts the SQL syntax according to the database type, ensuring compatibility 
		with SQLite, MySQL, SQL Server, and PostgreSQL.

		Returns:
			str: The SQL statement for creating the index, or an empty string if the database type is not supported.
		"""
		if self.__TC_DATANAME__ in ['sqlite']:
			return f'''CREATE {self.unique} {self.clustered} INDEX {self.exists} {self.name} ON {"{table}"} ({','.join(self.columns)}) {self.where}'''
		elif self.__TC_DATANAME__ in ['mysql']:
			return f'''CREATE {self.unique} {self.fulltext}{self.spatial} INDEX {self.name} ON {"{table}"} ({','.join(self.columns)}) USING {self.using} {self.where}'''
		elif self.__TC_DATANAME__ in ['sqlserver','postgresql']:
			return f'''CREATE {self.unique} {self.clustered} INDEX {self.exists} {self.name} ON {"{table}"} ({','.join(self.columns)}) {f'INCLUDE ({",".join(self.includes)})'if len(self.includes)>0 else ""} {self.where}'''
		return ''