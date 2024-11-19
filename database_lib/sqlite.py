import sqlite3
import sys
import os
import re

class queryPY(sqlite3.Connection):
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

	Methods:
		__call__(func: types.FunctionType):
			Execute the specified function and register it as a stored procedure.
		remove(if_exists: bool = True) -> bool:
			Remove the registered stored procedure from the database.
		run(*args, **kwargs):
			Execute the registered stored procedure with the provided arguments.
		get_definer() -> str:
			Retrieve the definer of the procedure.
		__str__() -> str:
			Return a string representation of the procedure.
	"""
	def __init__(self, data, paramets):
		"""
		Initialize a connection to a SQLite database with specified parameters.

		This constructor sets up the connection using the provided data and parameters, including 
		handling reconnection attempts. It ensures that the maximum number of reconnection attempts 
		is not exceeded and prepares the connection for use.

		Args:
			data: A dictionary containing connection parameters such as database file path.
			paramets: A dictionary of additional parameters for the connection, including attempts 
					and maximum attempts.

		Raises:
			ValueError: If the number of reconnection attempts exceeds the allowed maximum.
			Exception: If there is an error during the initialization of the database connection.
		"""
		self.paramets = paramets
		self.paramets['attempts'] = self.paramets.get('attempts',0)
		if self.paramets['attempts']>=self.paramets.get('max_attempts',5):
			raise ValueError('Attempts to reconnect exceeded the norm')
		self.DATA_CONNECT = data
		self.DB_NAME_ORG = ".".join(data["dbFile"].split(os.sep)[-1].split('.')[:-1])
		self.DB_NAME = "".join("_".join(data["dbFile"].split(os.sep)[-2:]).replace(" ","").split(".")[:-1])
		try:
			super(queryPY,self).__init__(data["dbFile"])
			self.version = lambda : self.query_f("VERSION")
		except BaseException as e:
			self.close()
			raise e
	def __str__(self):
		return f'''{self.res}'''
	def close_cur(self):
		"""
		Close the current database cursor if it exists.

		This function checks if a cursor attribute is present and, if so, closes it to free up 
		resources. It also removes the cursor attribute from the instance to ensure it is no longer accessible.

		Returns:
			None
		"""
		if hasattr(self,'cur'):
			self.cur.close()
			delattr(self,'cur')
	def open(self):
		"""
		Reinitialize the database connection.

		This function first closes the current cursor, if it exists, and then reinitializes the 
		database connection using the stored connection data and parameters. It effectively 
		resets the connection state.

		Returns:
			None
		"""
		self.close_cur()
		self.__init__(self.DATA_CONNECT,self.paramets)
	def is_active(self):
		"""
		Check if the database connection is active.

		This function attempts to execute a simple query to verify the status of the database connection. 
		If the query executes successfully, it returns True; otherwise, it returns False.

		Returns:
			bool: True if the database connection is active, False otherwise.
		"""
		try:
			self.close_cur()
			self.cur = self.cursor()
			self.cur.execute('SELECT 1')
			self.close_cur()
			return True
		except sqlite3.Error:
			return False
	def query_f(self,method,que={},req={}):
		"""
		Execute a database query using the specified method.

		This function performs a query on the database based on the provided method, query parameters, 
		and request options. It handles automatic commits if specified and manages reconnection attempts 
		in case of connection errors.

		Args:
			method (str): The method to be executed (e.g., 'SELECT', 'INSERT').
			que (dict, optional): The query parameters to be used in the execution. Defaults to an empty dictionary.
			req (dict, optional): Additional request options for the query execution. Defaults to an empty dictionary.

		Returns:
			The result of the query execution.

		Raises:
			pymysql.Error: If there is an error during the query execution.
			BaseException: If any other exception occurs during the process.
		"""
		try:
			self.cur = self.cursor()
			self.res = self.functinon_list(method)(que,req)
			if self.paramets.get('auto_commit',False): self.commit()
		except BaseException as e:
			raise e
		finally:
			self.close_cur()
		return self.res
	def functinon_list(self,m):
		"""
		Retrieve the corresponding function for a specified database operation.

		This method maps a given operation name to its associated function, allowing for dynamic 
		execution of various database methods based on the operation specified. It returns the 
		function reference from a predefined mapping of operation names to methods.

		Args:
			m (str): The name of the database operation for which to retrieve the corresponding function.

		Returns:
			The function associated with the specified operation name.

		Raises:
			KeyError: If the specified operation name does not exist in the mapping.
		"""
		MET_FUNC = {
			"VERSION":self.version_f,
			"SELECT":self.selection_f,
			"SELECT_DISTINCT":self.selection_distinct_f,
			"INSERT":self.insert_f,
			"UPDATE":self.update_f,
			"DELETE":self.delete_f,
			"DROP":self.drop_f,
			"SHOW_TABLE":self.show_table_f,
			"SHOW_COLUMNS":self.show_coll_f,
			"DDL":self.ddl_f,
			"FIELDS":self.show_field_f,
			"CREATE":self.create_f,
			"RENAME_TABLE":self.rename_table_f,
			"ALTER_COLUMN":self.alter_column_f,
			"ADD_COLUMN":self.add_column_f,
			"DROP_COLUMN":self.drop_column_f,
			"RENAME_COLUMN":self.rename_column_f,
			"CREATE_INDEX":self.create_index_f,
			"DROP_INDEX":self.drop_index_f,
			"SHOW_INDEX":self.show_index_f,
			"SHOW_FOREIGN":self.show_foreign_f,
			"CREATE_FOREIGN":self.create_foreign_f,
		}
		return MET_FUNC[m]
	def version_f(self,q,r):
		"""
		Retrieve the version of the database.

		This function executes a SQL query to obtain the current version of the database being used. 
		It returns the version information as a string.

		Args:
			q: The first parameter for the query, not used in this function.
			r: The second parameter for the query, not used in this function.

		Returns:
			str: The version of the database as a string.
		"""
		self.cur.execute("SELECT sqlite_version();")
		return self.cur.fetchone()[0]
	def selection_f(self,q,r):
		"""
		Execute a SELECT query on the specified tables and retrieve the results.

		This function takes a dictionary of tables and their corresponding columns, constructs 
		a SELECT SQL query for each table, and executes it. It returns the results of the queries 
		in the original dictionary format.

		Args:
			q (dict): A dictionary where keys are table names and values are lists of columns to select.
			r: Additional SQL clauses to append to the query (e.g., WHERE conditions).

		Returns:
			dict: A dictionary containing the results of the SELECT queries for each table.
		
		Raises:
			Exception: If the provided query parameter is not a dictionary.
		"""
		if not isinstance(q,dict):
			raise ValueError('Data type must be dictionary')
		def get_columns(tab,cols):
			if len(cols)==0:
				return f'{tab}.*'
			return ','.join(map(lambda x: f'{tab}.{x}', cols))
		for tab,cols in q.items():
			select = f"""SELECT {get_columns(tab,cols)} FROM {tab} {r}"""
			self.cur.execute(select)
			q[tab] = self.cur.fetchall()
		return q
	def selection_distinct_f(self,q,r):
		"""
		Execute a SELECT DISTINCT query on the specified tables and retrieve the unique results.

		This function takes a dictionary of tables and their corresponding columns, constructs 
		a SELECT DISTINCT SQL query for each table, and executes it. It returns the unique results 
		of the queries in the original dictionary format.

		Args:
			q (dict): A dictionary where keys are table names and values are lists of columns to select.
			r: Additional SQL clauses to append to the query (e.g., WHERE conditions).

		Returns:
			dict: A dictionary containing the unique results of the SELECT DISTINCT queries for each table.

		Raises:
			Exception: If the provided query parameter is not a dictionary.
		"""
		if not isinstance(q,dict):
			raise ValueError('Data type must be dictionary')
		def get_columns(tab,cols):
			if len(cols)==0:
				return f'{tab}.*'
			return ','.join(map(lambda x: f'{tab}.{x}', cols))
		for tab,cols in q.items():
			select = f"""SELECT DISTINCT {get_columns(tab,cols)} FROM {tab} {r}"""
			self.cur.execute(select)
			q[tab] = self.cur.fetchall()
		return q
	def insert_f(self,q,r):
		"""
		Execute an INSERT query to add new records to the specified tables.

		This function constructs and executes SQL INSERT statements based on the provided 
		dictionary of tables and their corresponding column values. It returns a boolean indicating 
		the success of the insert operation.

		Args:
			q (dict): A dictionary where keys are table names and values are dictionaries containing 
					'columns' and 'values' for the insert operation.
			r: Additional SQL clauses to append to the query (e.g., conditions).

		Returns:
			bool: True if the records were successfully inserted, False if no inserts were attempted.

		Raises:
			Exception: If an error occurs during the execution of the insert statements.
		"""
		innserts = []
		for table, cols in q.items():
			innserts.append(f"""INSERT INTO {table} ({','.join(cols.get('columns',[]))}) VALUES {','.join([f"({','.join(val)})" for val in cols.get('values',[])])}""")
		if len(innserts)==0:
			return False
		try:
			self.cur.execute(';'.join(innserts))
			return True
		except BaseException as e:
			raise e
	def update_f(self,q,r):
		"""
		Execute an UPDATE query to modify existing records in the specified tables.

		This function constructs and executes SQL UPDATE statements based on the provided 
		dictionary of tables and their corresponding column values. It returns a boolean indicating 
		the success of the update operation.

		Args:
			q (dict): A dictionary where keys are table names and values are dictionaries containing 
					column-value pairs for the update operation.
			r: Additional SQL clauses to append to the query (e.g., conditions).

		Returns:
			bool: True if the records were successfully updated, False if no updates were attempted.

		Raises:
			Exception: If an error occurs during the execution of the update statements.
		"""
		updates = []
		for tab, val in q.items():
			updates.append(f"""UPDATE {tab} SET {','.join([f'{c}={v}' for c,v in val.items()]) } {r}""")
		try:
			self.cur.execute(';'.join(updates))
			return True
		except BaseException as e:
			raise e
	def delete_f(self,q,r):
		"""
		Execute a DELETE query to remove records from the specified tables.

		This function constructs and executes SQL DELETE statements based on the provided 
		list of tables. It returns a boolean indicating the success of the delete operation.

		Args:
			q (list): A list of table names from which to delete records.
			r: Additional SQL clauses to append to the query (e.g., conditions).

		Returns:
			bool: True if the records were successfully deleted.

		Raises:
			Exception: If an error occurs during the execution of the delete statements.
		"""
		deletes = []
		for tab in q:
			deletes.append(f"""DELETE FROM {tab} {r}""")
		try:
			self.cur.execute(';'.join(deletes))
		except BaseException as e:
			raise e
		return True
	def drop_f(self,q,r):
		"""
		Execute a DROP TABLE query to remove a specified table from the database.

		This function constructs and executes a SQL statement to drop the specified table from 
		the database. It returns a boolean indicating the success of the drop operation.

		Args:
			q (str): The name of the table to be dropped.
			r: Additional SQL clauses to append to the query (e.g., conditions).

		Returns:
			bool: True if the table was successfully dropped.

		Raises:
			Exception: If an error occurs during the execution of the drop statement.
		"""
		try:
			self.cur.execute(f"""DROP TABLE {q};""")
		except BaseException as e:
			raise e
		return True
	def show_table_f(self,q,r):
		"""
		Retrieve a list of tables from the specified database.

		This function executes a SQL query to show all tables in the database associated with the 
		current instance. It returns a tuple of the table names, filtering out any empty results.

		Args:
			q: The first parameter for the query, not used in this function.
			r: The second parameter for the query, not used in this function.

		Returns:
			tuple: A tuple containing the names of the tables in the database.
		"""
		show = "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
		self.cur.execute(show)
		return tuple(zip(*self.cur.fetchall()+[('sqlite_master',)]))[0]
	def show_field_f(self,q,r):
		"""
		Retrieve metadata about the fields of a specified table.

		This function executes a SQL query to obtain information about the columns in the specified 
		table, including their names, types, nullability, default values, and keys. It processes the 
		results to provide a structured representation of the column metadata.

		Args:
			q: A list or tuple containing the name of the table to query.
			r: Additional parameters for the query, not used in this function.

		Returns:
			list: A list of lists containing metadata for each column in the specified table.

		Raises:
			Exception: If the provided table name is invalid or if there is an error during the query execution.
		"""
		ex = list(map(str,q))
		show = f"PRAGMA table_info({ex[0]})"
		self.cur.execute(show)
		return self.cur.fetchall()
	def show_coll_f(self,q,r):
		"""
		Retrieve the names of columns for the specified tables.

		This function executes a SQL query to obtain the column names for each table listed in the 
		provided input. It returns a dictionary mapping each table name to a tuple of its column names.

		Args:
			q: A list or tuple containing the names of the tables to query.
			r: Additional parameters for the query, not used in this function.

		Returns:
			dict: A dictionary where keys are table names and values are tuples of column names.

		Raises:
			Exception: If the provided table names are invalid or if there is an error during the query execution.
		"""
		ex = list(map(str,q))
		names = {}
		for i in ex:
			show = f"SELECT * FROM {i}"
			self.cur.execute(show)
			names[i] = tuple(description[0] for description in self.cur.description)
		return names
	def ddl_f(self, q, r):
		"""
		Retrieve the Data Definition Language (DDL) statement for a specified table.

		This function executes a SQL query to obtain the DDL statement from the SQLite master table 
		for the specified table name. It returns the DDL statement if the table exists; otherwise, 
		it returns None.

		Args:
			q (str): The name of the table for which to retrieve the DDL statement.
			r: Additional parameters for the query, not used in this function.

		Returns:
			str or None: The DDL statement for the specified table, or None if the table does not exist.

		Raises:
			Exception: If an error occurs during the execution of the query.
		"""
		ddl = f"SELECT sql FROM sqlite_master WHERE name='{q}' AND type='table'"
		self.cur.execute(ddl)
		ddl = self.cur.fetchone()
		return ddl[0] if len(ddl)>0 else None
	def create_f(self,q,r):
		"""
		Execute a CREATE TABLE query to define new tables in the database.

		This function constructs and executes SQL CREATE TABLE statements based on the provided 
		dictionary of tables and their corresponding column definitions. It supports options for 
		checking if the table should be created only if it does not already exist, as well as 
		specifying the storage engine and character set.

		Args:
			q (dict): A dictionary where keys are table names and values are dictionaries containing 
					column definitions.
			r: Additional SQL options, including 'not_exists', 'engine', and 'charset'.

		Returns:
			bool: True if the tables were successfully created.

		Raises:
			Exception: If an error occurs during the execution of the create statements.
		"""
		creates = []
		for tab, cols in q.items():
			creates.append(f'''CREATE TABLE {tab} ({f', '.join([
				key+' '+' '.join(filter(None,val)) if isinstance(val,(tuple,list)) else key+' '+val
				for key,val in cols.items() if key not in ['__DATANAME__']
			])})''')
		try:
			self.cur.execute(';'.join(creates))
		except BaseException as e:
			raise e
		return True
	def add_column_f(self,q,r):
		"""
		Execute an ALTER TABLE query to add new columns to specified tables.

		This function constructs and executes SQL ALTER TABLE statements to add columns based on 
		the provided dictionary of tables and their corresponding column definitions. It returns 
		a boolean indicating the success of the operation.

		Args:
			q (dict): A dictionary where keys are table names and values are dictionaries containing 
					column definitions to be added.
			r: Additional SQL options, not used in this function.

		Returns:
			bool: True if the columns were successfully added.

		Raises:
			Exception: If an error occurs during the execution of the alter statements.
		"""
		alters = []
		for tab, vals in q.items():
			alter = f'''ALTER TABLE {tab} ADD COLUMN'''
			for col, val in vals.items():
				alters.append(f'''{alter} {col} {' '.join(val)}''')
		try:
			self.cur.execute(';'.join(alters))
		except BaseException as e:
			raise e
		return True
	def alter_column_f(self, q, r):
		"""
		Execute an ALTER TABLE query to modify existing columns in specified tables.

		This function constructs and executes SQL ALTER TABLE statements to change the definitions 
		of columns based on the provided dictionary of tables and their corresponding new definitions. 
		It returns a boolean indicating the success of the operation.

		Args:
			q (dict): A dictionary where keys are table names and values are dictionaries containing 
					column modifications.
			r: Additional SQL options, not used in this function.

		Returns:
			bool: True if the columns were successfully altered.

		Raises:
			Exception: If an error occurs during the execution of the alter statements.
		"""
		alters = []
		def replace_match(match,t):
			if t=='NOT NULL':
				return 'NOT NULL' if 'NULL'==match.group().upper() else t
			elif t=='NULL':
				return 'NULL' if 'NOT NULL'==match.group().upper() else t
			else:
				return match.group().upper()
		def update_option(ops,op):
			if len(ops)==0:
				return op
			for o in ops:
				_type = o.get('type',None)
				value = o.get('value',None)
				pattern1 = fr"""(?<![\(\[\"'])\b(NOT\s+NULL|NULL|{_type})\b(?![\)\]\"'])"""
				pattern2 = fr"""{_type}\s*\([^()]*('[^']*'|"[^"]*"|[^()]+)[^()]*\)"""
				if value is not None:
					if re.search(pattern1,op) is not None:
						op = re.sub(pattern1, lambda x: replace_match(x,value), op)
					elif re.search(pattern2,op) is not None:
						op = re.sub(pattern2, value, op)
					else:
						op = op+f' {value}'
				else:
					if re.search(fr"""(?<![\(\[\"'])\b({_type})\b(?![\)\]\"'])""",op) is not None:
						op = re.sub(fr"""(?<![\(\[\"'])\b({_type})\b(?![\)\]\"'])""", '' , op)
			return op.strip()
		for tab, vals in q.items():
			ddl = self.ddl_f(tab,[])
			pattern = r"CREATE TABLE (\w+) \((.*)\)"
			matches = re.search(pattern, ddl)
			if matches is None:
				return False
			dataType = lambda y: list(filter(lambda x: x.get('type',None)=='DATA TYPE' ,vals.get(y,[])))
			options = lambda y: list(filter(lambda x: x.get('type',None)!='DATA TYPE' ,vals.get(y,[])))
			table_name = matches.group(1)
			columns = re.split(r",(?=(?:[^']*'[^']*')*[^']*$)(?![^()]*\))", matches.group(2))
			columns = list(map(lambda x: re.split(r"\s(?=(?:[^']*'[^']*')*[^']*$)(?![^()]*\))", x.strip()) ,columns))
			columns = {
				col[0]:{
					'data_type': (col[1] if len(dataType(col[0]))==0 else dataType(col[0])[0].get('value',None)),
					'option': update_option(options(col[0]),' '.join(col[2:]))
				}
			for col in columns }
			indexs = '; '.join(map(lambda x: x.get('ddl','').strip(), self.show_index_f(tab,[])))
			alters.append(f"""
				PRAGMA foreign_keys = 0;
				CREATE TABLE temp_table_{tab} AS SELECT * FROM {tab};
				DROP TABLE {tab};
				CREATE TABLE {tab} ( {','.join([k+' '+v['data_type']+' '+v['option'] for k,v in columns.items()])});
				INSERT INTO {tab} ({','.join(columns.keys())}) SELECT {','.join(columns.keys())} FROM temp_table_{tab};
				DROP TABLE temp_table_{tab};
				{indexs+';' if len(indexs.strip())>0 else ''}
				PRAGMA foreign_keys = 1;
			""")
		try:
			self.cur.executescript('\n'.join(alters))
		except BaseException as e:
			raise e
		return True
	def drop_column_f(self,q,r):
		"""
		Execute an ALTER TABLE query to remove specified columns from tables.

		This function constructs and executes SQL ALTER TABLE statements to drop columns based on 
		the provided dictionary of tables and their corresponding columns to be removed. It returns 
		a boolean indicating the success of the operation.

		Args:
			q (dict): A dictionary where keys are table names and values are the columns to be dropped.
			r: Additional SQL options, not used in this function.

		Returns:
			bool: True if the columns were successfully dropped.

		Raises:
			Exception: If an error occurs during the execution of the drop statements.
		"""
		alters = []
		for tab, vals in q.items():
			alters.append(f'''ALTER TABLE {tab} DROP COLUMN {vals}''')
		try:
			self.cur.execute(';'.join(alters))
		except BaseException as e:
			raise e
		return True
	def rename_table_f(self,q,r):
		"""
		Execute an ALTER TABLE query to rename specified tables.

		This function constructs and executes SQL ALTER TABLE statements to rename tables based on 
		the provided dictionary of current and new table names. It returns a boolean indicating the 
		success of the operation.

		Args:
			q (dict): A dictionary where keys are current table names and values are the new names.
			r: Additional SQL options, not used in this function.

		Returns:
			bool: True if the tables were successfully renamed.

		Raises:
			Exception: If an error occurs during the execution of the rename statements.
		"""
		alters = []
		for tab, vals in q.items():
			alters.append(f'''ALTER TABLE {tab} RENAME TO {vals}''')
		try:
			self.cur.execute(';'.join(alters))
		except BaseException as e:
			raise e
		return True
	def rename_column_f(self,q,r):
		"""
		Execute an ALTER TABLE query to rename specified columns in tables.

		This function constructs and executes SQL ALTER TABLE statements to rename columns based on 
		the provided dictionary of tables and their corresponding old and new column names. It returns 
		a boolean indicating the success of the operation.

		Args:
			q (dict): A dictionary where keys are table names and values are dictionaries containing 
					'old_name' and 'new_name' for the columns to be renamed.
			r: Additional SQL options, not used in this function.

		Returns:
			bool: True if the columns were successfully renamed.

		Raises:
			Exception: If an error occurs during the execution of the rename statements.
		"""
		alters = []
		for tab, vals in q.items():
			alters.append(f'''ALTER TABLE {tab} RENAME COLUMN {vals.get('old_name','')} TO {vals.get('new_name','')}''')
		try:
			self.cur.execute(';'.join(alters))
		except BaseException as e:
			raise e
		return True
	def create_index_f(self, q, r):
		"""
		Execute a SQL query to create indexes on specified tables.

		This function constructs and executes SQL CREATE INDEX statements based on the provided 
		dictionary of tables and their corresponding index definitions. It returns a boolean indicating 
		the success of the operation.

		Args:
			q (dict): A dictionary where keys are table names and values are the index definitions.
			r: Additional SQL options, not used in this function.

		Returns:
			bool: True if the indexes were successfully created.

		Raises:
			Exception: If an error occurs during the execution of the create index statements.
		"""
		indexs = []
		for tab, val in q.items():
			indexs.append(str(val).format(table=tab))
		try:
			self.cur.execute(';'.join(indexs))
		except BaseException as e:
			raise e
	def drop_index_f(self, q, r):
		"""
		Execute a SQL query to drop a specified index from a table.

		This function constructs and executes a SQL DROP INDEX statement to remove the specified 
		index from the database. It returns a boolean indicating the success of the operation.

		Args:
			q (str): The name of the index to be dropped.
			r: Additional SQL options, not used in this function.

		Returns:
			bool: True if the index was successfully dropped.

		Raises:
			Exception: If an error occurs during the execution of the drop index statement.
		"""
		try:
			self.cur.execute(f'''DROP INDEX {q}''')
		except BaseException as e:
			raise e
	def show_index_f(self, q, r):
		"""
		Retrieve information about indexes for a specified table.

		This function executes a SQL query to obtain details about the indexes associated with 
		the specified table, including their names, uniqueness, and sorting order. It returns 
		a structured list containing the index information.

		Args:
			q (str): The name of the table for which to retrieve index information.
			r: Additional parameters for the query, not used in this function.

		Returns:
			list: A list of dictionaries containing details about each index, including 
				the index name, DDL, uniqueness, clustering, and associated columns.

		Raises:
			Exception: If an error occurs during the execution of the query.
		"""
		try:
			self.cur.execute(f'''SELECT name, sql FROM sqlite_master WHERE type='index' AND tbl_name='{q}' ''')
			result = self.cur.fetchall()
			for i,data in enumerate(result):
				columns = list(map(lambda x: x.strip().split(),filter(None,re.search(fr'ON {q} \((.*?)\)', data[1]).group(1).strip().split(','))))
				result[i] = {
					'name' : data[0],
					'ddl':data[1],
					'is_unique' : 'UNIQUE' in data[1],
					'is_clustered' : 'CLUSTERED' in data[1],
					'columns': [{
						'name': column[0],
						'sort': 'ASC' if 'ASC' in column[1:] else ('DESC' if 'DESC' in column[1:] else None),
						'collate': re.search(r'COLLATE\s(.*?)\s', " ".join(column)).group(1) if re.search(r'COLLATE\s(.*?)\s', " ".join(column)) else None
					} for column in columns]
				}
			return result
		except BaseException as e:
			raise e
	def show_foreign_f(self, q, r):
		"""
		Retrieve information about foreign key constraints for a specified table.

		This function executes a SQL query to obtain details about the foreign keys associated 
		with the specified table, including the referenced table and columns, as well as the 
		rules for updates and deletions. It returns a structured list containing the foreign key information.

		Args:
			q (str): The name of the table for which to retrieve foreign key information.
			r: Additional parameters for the query, not used in this function.

		Returns:
			list: A list of dictionaries containing details about each foreign key, including 
				the referenced table, columns involved, and rules for updates and deletions.

		Raises:
			Exception: If an error occurs during the execution of the query.
		"""
		try:
			self.cur.execute(f'''PRAGMA foreign_key_list('{q}') ''')
			result = self.cur.fetchall()
			return list(map(lambda x: {
				'table':x[2],
				'from':x[3],
				'to':x[4],
				'on_update':x[5],
				'on_delete':x[6],
				'match':x[7]
			}, result))
		except BaseException as e:
			raise e
	def create_foreign_f(self, q, r):
		"""
		Execute an ALTER TABLE query to add a foreign key constraint.

		This function constructs and executes a SQL statement to add a foreign key constraint 
		to the specified table. It returns a boolean indicating the success of the operation.

		Args:
			q (str): The name of the table to which the foreign key constraint will be added.
			r (str): The definition of the foreign key constraint.

		Returns:
			bool: True if the foreign key constraint was successfully created.

		Raises:
			Exception: If an error occurs during the execution of the ALTER TABLE statement.
		"""
		try:
			self.cur.execute(f'''ALTER TABLE {q} ADD CONSTRAINT {r};''')
			return True
		except BaseException as e:
			raise e
