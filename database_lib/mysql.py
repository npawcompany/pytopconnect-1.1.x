import pymysql
import sys
import os
import re

class queryPY(pymysql.connections.Connection):
	"""
	A class representing a MySQL connection.

	This class inherits from the pymysql.connections.Connection class and provides additional functionality.
	It provides methods for executing SQL queries, managing transactions, and handling connection failures.
	It also provides a method to execute SQL queries and handle connection failures using a retry mechanism.
	Args:
		data (dict): A dictionary containing the connection details (host, user, password, database).
		paramets (dict, optional): A dictionary containing additional parameters for the connection. Defaults to an empty dictionary.
		max_attempts (int, optional): The maximum number of attempts to reconnect. Defaults to 5.
		auto_commit (bool, optional): If True, commits the transaction after executing each query. Defaults to False.
		functinon_list (dict, optional): A dictionary containing the methods for executing SQL queries. Defaults to a predefined list.
		"""

	def __init__(self, data, paramets):
		"""
		Initialize a database connection with specified parameters.

		This constructor sets up the connection using the provided data and parameters, including 
		handling reconnection attempts. It ensures that the maximum number of reconnection attempts 
		is not exceeded and prepares the connection for use.

		Args:
			data: A dictionary containing connection parameters such as database name and credentials.
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
		self.DB_NAME_ORG = data["database"]
		self.DB_NAME = data["database"].replace(' ','_')
		try:
			super(queryPY,self).__init__(**data)
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
			with self.cursor() as cursor:
				cursor.execute('SELECT 1')
				cursor.close()
				return True
		except pymysql.Error:
			return False
	def query_f(self, method: str, que: dict = {}, req: dict = {}):
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
		except pymysql.Error as e:
			if e.args[0] == 2006:
				self.paramets['attempts'] += 1
				self.open()
				return self.query_f(method,que,req)
			raise e
		except BaseException as e:
			raise e
		finally:
			self.close_cur()
		return self.res
	def functinon_list(self, m):
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
			"SHOW_TABLE":self.show_table_f,
			"SHOW_COLUMNS":self.show_coll_f,
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

			"SHOW_PROCEDURE":self.show_procedure_f,
			"CREATE_PROCEDURE":self.create_procedure_f,
			"RUN_PROCEDURE":self.run_procedure_f,
			"DROP_PROCEDURE":self.drop_procedure_f,
			"SHOW_FUNCTION":self.show_function_f,
			"CREATE_FUNCTION":self.create_function_f,
			"RUN_FUNCTION":self.run_function_f,
			"DROP_FUNCTION":self.drop_function_f,
			"SHOW_TRIGGER":self.show_trigger_f,
			"CREATE_TRIGGER":self.create_trigger_f,
			"DROP_TRIGGER":self.drop_trigger_f,
		}
		return MET_FUNC[m]
	def version_f(self, q, r):
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
		self.cur.execute("SELECT VERSION()")
		return self.cur.fetchone()[0]
	def selection_f(self, q, r):
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
	def selection_distinct_f(self, q, r):
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
	def insert_f(self, q, r):
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
	def update_f(self, q, r):
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
	def delete_f(self, q, r):
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
	def drop_f(self, q, r):
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
	def show_table_f(self, q, r):
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
		show = f"SHOW TABLES FROM `{self.DB_NAME}`"
		self.cur.execute(show)
		return tuple(filter(None,zip(*self.cur.fetchall())))
	def show_field_f(self, q, r):
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
		show = f"SELECT `COLUMN_NAME`,`COLUMN_TYPE`,`IS_NULLABLE`,`COLUMN_DEFAULT`,`COLUMN_KEY` FROM INFORMATION_SCHEMA.COLUMNS WHERE `TABLE_NAME` = '{ex[0]}'"
		self.cur.execute(show)
		result = list(self.cur.fetchall())
		for i, elm in enumerate(result):
			elm = list(elm)
			elm[1] = elm[1].upper()
			elm[2] = elm[2] == 'NO'
			elm[4] = elm[4] == 'PRI'
			elm.insert(0,i+1)
			result[i] = elm
		return result
	def show_coll_f(self, q, r):
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
		names = {}
		if len(q)==0:
			return {}
		q = q[0]
		for i in q:
			show = f"SELECT * FROM `{i}`"
			self.cur.execute(show)
			names[i] = tuple(description[0] for description in self.cur.description)
		return names
	def create_f(self, q, r):
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
		not_exists = 'IF NOT EXISTS' if r.get('not_exists', False) else ''
		engine = r.get('engine','ENGINE = InnoDB')
		charset = r.get('charset','utf8mb4')
		for tab, cols in q.items():
			creates.append(f'''CREATE TABLE {tab} {not_exists} ({f', '.join([
				key+' '+' '.join(filter(None,val)) if isinstance(val,(tuple,list)) else key+' '+val
				for key,val in cols.items() if key not in ['__TC_DATANAME__']
			])}) {engine} CHARSET = {charset}''')
		try:
			self.cur.execute(';'.join(creates))
		except BaseException as e:
			raise e
		return True
	def add_column_f(self, q, r):
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
			alters.exctend([f'''ALTER TABLE {tab} ADD COLUMN {col} {' '.join(val)}''' for col, val in vals.items()])
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
		for tab, vals in q.items():
			alters.exctend([f'''ALTER TABLE {tab} ALERT COLUMN {col} {' '.join(val)}''' for col, val in vals.items()])
		try:
			self.cur.execute(';'.join(alters))
		except BaseException as e:
			raise e
		return True
	def drop_column_f(self, q, r):
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
		alters = [f'''ALTER TABLE {tab} DROP COLUMN {vals}''' for tab, vals in q.items()]
		try:
			self.cur.execute(';'.join(alters))
		except BaseException as e:
			raise e
		return True
	def rename_table_f(self, q, r):
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
	def rename_column_f(self, q, r):
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
		Execute an SQL query to create indexes on specified tables.

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

		This function constructs and executes a SQL DROP INDEX statement based on the provided index name. 
		It returns a boolean indicating the success of the operation.

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
		the specified table, including their names, uniqueness, and columns. It returns a 
		structured dictionary containing the index information.

		Args:
			q: The first parameter for the query, not used in this function.
			r: The schema name of the table for which to retrieve index information.

		Returns:
			dict: A dictionary containing index details, where each key corresponds to an index 
				and its value includes properties such as name, uniqueness, and associated columns.

		Raises:
			Exception: If an error occurs during the execution of the query.
		"""
		try:
			self.cur.execute(f'''SELECT index_name, non_unique, column_name, index_type FROM information_schema.statistics WHERE table_schema = '{r}' ; ''')
			result = list(self.cur.fetchall())
			obj = {}
			for i,data in enumerate(result):
				if i not in obj.keys():
					obj[i] = {
						'name' : data[0],
						'is_unique' : data[1]==1,
						'using': data[3],
						'columns': [data[2]]
					}
				else:
					obj[i]['columns'].append(data[2])
			return obj
		except BaseException as e:
			raise e
	def show_foreign_f(self, q, r):
		"""
		Retrieve information about foreign key constraints for a specified table.

		This function executes a SQL query to obtain details about the foreign keys associated 
		with the specified table, including their names, referenced tables, and update/delete rules. 
		It returns a structured list containing the foreign key information.

		Args:
			q: The name of the table for which to retrieve foreign key information.
			r: The schema name of the table.

		Returns:
			list: A list of dictionaries containing details about each foreign key, including 
				the foreign key name, referenced table, columns involved, and rules for updates 
				and deletions.

		Raises:
			Exception: If an error occurs during the execution of the query.
		"""
		try:
			self.cur.execute(f'''SELECT RC.CONSTRAINT_NAME AS FOREIGN_KEY_NAME,KCU.REFERENCED_TABLE_NAME, KCU.COLUMN_NAME, KCU.REFERENCED_COLUMN_NAME, RC.UPDATE_RULE, RC.DELETE_RULE, RC.MATCH_OPTION FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS RC JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU ON RC.CONSTRAINT_NAME = KCU.CONSTRAINT_NAME WHERE RC.CONSTRAINT_SCHEMA = '{r}' AND RC.TABLE_NAME = '{q}';''')
			result = self.cur.fetchall()
			return list(map(lambda x: {
				'name':x[0],
				'table':x[1],
				'from':x[2],
				'to':x[3],
				'on_update':x[4],
				'on_delete':x[5],
				'match':x[6]
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

	def show_procedure_f(self, q, r):
		"""
		Retrieve information about stored procedures in the specified schema.

		This function executes a SQL query to obtain the names and parameters of stored procedures 
		within the given schema. It returns a dictionary mapping each procedure name to a list of 
		its parameter names.

		Args:
			q (str): The name of the schema to query for stored procedures.
			r: Additional parameters for the query, not used in this function.

		Returns:
			dict: A dictionary where keys are procedure names and values are lists of parameter names.

		Raises:
			Exception: If an error occurs during the execution of the query.
		"""
		try:
			self.cur.execute(f"""SELECT `SPECIFIC_NAME`, `PARAMETER_NAME` FROM `INFORMATION_SCHEMA`.`PARAMETERS` WHERE `SPECIFIC_SCHEMA`='{q}' AND `ROUTINE_TYPE`='PROCEDURE';""")
			result = self.cur.fetchall()
			data = {}
			for res in result:
				if res[0] in data.keys():
					data[res[0]].append(res[1])
					continue
				data[res[0]] = [res[1]]
			return data
		except BaseException as e:
			raise e
	def create_procedure_f(self, q, r):
		"""
		Execute a SQL query to create a stored procedure in the database.

		This function constructs and executes the necessary SQL statements to define a new stored 
		procedure based on the provided parameters and body. It handles the dropping of any existing 
		procedure with the same name and ensures that the procedure is created with the specified 
		characteristics.

		Args:
			q: An object containing the procedure's name, parameters, body, and other attributes.
			r: Additional SQL options, not used in this function.

		Returns:
			bool: True if the procedure was successfully created.

		Raises:
			Exception: If an error occurs during the execution of the create procedure statements.
		"""
		try:
			proc = [
				f'DROP PROCEDURE IF EXISTS {q.name};',
				(f'DELIMITER {q.delitmiter}' if q.delitmiter is not None else ''),
				f'CREATE {q.get_definer()} PROCEDURE {q.name}',
				(f'DELIMITER ;' if q.delitmiter is not None else '')
			]
			proc[2] += f'''({','.join([f'{q.__params__(param_name,"direction","IN").upper()} {param_name} {param.annotation(q.db.dataTypes)} {q.__params__(param_name,"parameter")}' for param_name, param in q.parameters.items()])})'''
			proc[2] += f'''\nBEGIN\n\t{q.body}\nEND{q.delitmiter if q.delitmiter is not None else ';'}'''
			for i in proc:
				if len(i.strip())>0:
					self.cur.execute(i)
			return True
		except BaseException as e:
			raise e
	def run_procedure_f(self, q, r):
		"""
		Execute a stored procedure with the specified parameters.

		This function runs the stored procedure defined in the input dictionary, passing the specified 
		parameters to it. It handles both calling the procedure directly and retrieving results if the 
		procedure is expected to return values.

		Args:
			q (dict): A dictionary containing the procedure name and parameters.
			r: A list of return column names for the result set.

		Returns:
			Union[bool, list]: True if the procedure executed successfully without returning values, 
							or a list of dictionaries containing the results if 'is_return' is True.

		Raises:
			Exception: If the procedure name is missing, or if an error occurs during the execution of the procedure.
		"""
		try:
			procedure = q.get('procedure',None)
			parameters = q.get('parameters',[])
			if procedure is None:
				raise ValueError('You are missing a procedure')
			if q.get('call',False):
				self.cur.callproc(procedure,parameters)
				return self.cur.fetchall()
			pars = [f'SET @p{i} = {v};' for i,v in enumerate(parameters)] if isinstance(parameters,(list,tuple)) else [f'SET @p0 = {parameters};']
			parel = 'SELECT '+(','.join([f'@p{i} AS {r[i]}' for i,v in enumerate(parameters)]) if isinstance(parameters,(list,tuple)) else f'@p0 AS {r[0]}')
			parfu = f'CALL `{procedure}` ('+(','.join([f'@p{i}' for i,v in enumerate(parameters)]) if isinstance(parameters,(list,tuple)) else '@p0')+')'
			for i in pars:
				self.cur.execute(i)
			self.cur.execute(parfu)
			if q.get('is_return',False):
				self.cur.execute(parel)
				return tuple(map(lambda x: dict(zip(r,x)),self.cur.fetchall()))
			return True
		except BaseException as e:
			raise e
	def drop_procedure_f(self, q, r):
		"""
		Execute a SQL query to drop a specified stored procedure.

		This function constructs and executes a SQL DROP PROCEDURE statement to remove the 
		specified procedure from the database. It returns a boolean indicating the success of the operation.

		Args:
			q (str): The name of the procedure to be dropped.
			r: Additional SQL options, including 'if_exists' to avoid errors if the procedure does not exist.

		Returns:
			bool: True if the procedure was successfully dropped.

		Raises:
			Exception: If an error occurs during the execution of the drop procedure statement.
		"""
		try:
			self.cur.execute(f'''DROP PROCEDURE {'IF EXISTS' if r.get('if_exists',False) else ''} {q}''')
			return True
		except BaseException as e:
			raise e
	def show_function_f(self, q, r):
		"""
		Retrieve information about stored functions in the specified schema.

		This function executes a SQL query to obtain the names and parameters of functions 
		within the given schema. It returns a dictionary mapping each function name to a list of 
		its parameter names.

		Args:
			q (str): The name of the schema to query for stored functions.
			r: Additional parameters for the query, not used in this function.

		Returns:
			dict: A dictionary where keys are function names and values are lists of parameter names.

		Raises:
			Exception: If an error occurs during the execution of the query.
		"""
		try:
			self.cur.execute(f"""SELECT `SPECIFIC_NAME`, `PARAMETER_NAME` FROM `INFORMATION_SCHEMA`.`PARAMETERS` WHERE `SPECIFIC_SCHEMA`='{q}' AND `ROUTINE_TYPE`='FUNCTION';""")
			result = self.cur.fetchall()
			data = {}
			for res in result:
				if res[0] in data:
					data[res[0]].append(res[1])
					continue
				data[res[0]] = [res[1]]
			return data
		except BaseException as e:
			raise e
	def create_function_f(self, q, r):
		"""
		Execute a SQL query to create a stored function in the database.

		This function constructs and executes the necessary SQL statements to define a new stored 
		function based on the provided parameters and body. It handles the dropping of any existing 
		function with the same name and ensures that the function is created with the specified 
		characteristics.

		Args:
			q: An object containing the function's name, parameters, body, and other attributes.
			r: Additional SQL options, not used in this function.

		Returns:
			bool: True if the function was successfully created.

		Raises:
			Exception: If an error occurs during the execution of the create function statements.
		"""
		try:
			funct = [
				f'DROP FUNCTION IF EXISTS {q.name};',
				(f'DELIMITER {q.delitmiter}' if q.delitmiter is not None else ''),
				f'CREATE {q.get_definer()} FUNCTION {q.name}',
				(f'DELIMITER ;' if q.delitmiter is not None else '')
			]
			funct[2] += f'''({','.join([f'{param_name} {param.annotation(q.db.dataTypes)}' for param_name, param in q.parameters.items()])}) RETURNS {q.return_type(q.db.dataTypes)}'''
			funct[2] += f'''\nBEGIN\n\t{q.body}\nEND{q.delitmiter if q.delitmiter is not None else ';'}'''
			for i in funct:
				if len(i.strip())>0:
					self.cur.execute(i)
			return True
		except BaseException as e:
			raise e
	def run_function_f(self, q, r):
		"""
		Execute a stored function with the specified parameters.

		This function runs the stored function defined in the input dictionary, passing the specified 
		parameters to it. It retrieves the result of the function execution and returns it as a dictionary.

		Args:
			q (dict): A dictionary containing the function name and parameters.
			r: A list of return column names for the result set.

		Returns:
			dict: A dictionary containing the result of the function execution, with the function name as the key.

		Raises:
			ValueError: If the function name is missing from the input dictionary.
			Exception: If an error occurs during the execution of the function.
		"""
		try:
			function = q.get('function',None)
			parameters = q.get('parameters',[])
			if function is None:
				raise ValueError('You are missing a function')
			pars = [f'SET @p{i} = {v};' for i,v in enumerate(parameters)] if isinstance(parameters,(list,tuple)) else [f'SET @p0 = {parameters};']
			parel = f'''SELECT `{function}`('''+(','.join([f'@p{i}' for i in range(len(parameters))]) if isinstance(parameters,(list,tuple)) else '@p0')+f''') AS {function}'''
			for i in pars:
				self.cur.execute(i)
			self.cur.execute(parel)
			return dict(map(lambda x: (function,x),self.cur.fetchone()))
		except BaseException as e:
			raise e
	def drop_function_f(self, q, r):
		"""
		Execute a SQL query to drop a specified stored function.

		This function constructs and executes a SQL DROP FUNCTION statement to remove the 
		specified function from the database. It returns a boolean indicating the success of the operation.

		Args:
			q (str): The name of the function to be dropped.
			r: Additional SQL options, including 'if_exists' to avoid errors if the function does not exist.

		Returns:
			bool: True if the function was successfully dropped.

		Raises:
			Exception: If an error occurs during the execution of the drop function statement.
		"""
		try:
			self.cur.execute(f'''DROP FUNCTION {'IF EXISTS' if r.get('if_exists',False) else ''} {q}''')
			return True
		except BaseException as e:
			raise e
	def show_trigger_f(self, q, r):
		"""
		Retrieve information about triggers in the database.

		This function executes a SQL query to list all triggers in the database and filters the results 
		based on the specified table name. It returns a dictionary containing details about each trigger, 
		including its name, timing, and event type.

		Args:
			q (str): The name of the table for which to retrieve trigger information.
			r: Additional parameters for the query, not used in this function.

		Returns:
			dict: A dictionary where keys are trigger names and values are dictionaries containing 
				details such as timing and event type.

		Raises:
			Exception: If an error occurs during the execution of the query.
		"""
		try:
			self.cur.execute("SHOW TRIGGERS;")
			result = self.cur.fetchall()
			data = {}
			for res in result:
				if res[2]==q:
					data[res[0]] = {
						'time':res[4],
						'event':res[1]
					}
			return data
		except BaseException as e:
			raise e
	def create_trigger_f(self, q, r):
		"""
		Execute a SQL query to create a trigger in the database.

		This function constructs and executes the necessary SQL statements to define a new trigger 
		based on the provided parameters and body. It handles the dropping of any existing trigger 
		with the same name and ensures that the trigger is created with the specified characteristics.

		Args:
			q: An object containing the trigger's name, timing, event, body, and other attributes.
			r: Additional SQL options, not used in this function.

		Returns:
			bool: True if the trigger was successfully created.

		Raises:
			Exception: If an error occurs during the execution of the create trigger statements.
		"""
		try:
			trig = [
				f'DROP TRIGGER IF EXISTS {q.name};',
				(f'DELIMITER {q.delitmiter}' if q.delitmiter is not None else ''),
				f'CREATE {q.get_definer()} TRIGGER {q.name} {q.time} {q.event} ON {q.db.db_name}.{q.table.table}',
				(f'DELIMITER ;' if q.delitmiter is not None else '')
			]
			trig[2] += f'''\nFOR EACH ROW BEGIN\n\t{q.body}\nEND{q.delitmiter if q.delitmiter is not None else ';'}'''
			for i in q:
				if len(i.strip())>0:
					self.cur.execute(i)
			return True
		except BaseException as e:
			raise e
	def drop_trigger_f(self, q, r):
		"""
		Execute a SQL query to drop a specified trigger from the database.

		This function constructs and executes a SQL DROP TRIGGER statement to remove the specified 
		trigger from the database. It returns a boolean indicating the success of the operation.

		Args:
			q (str): The name of the trigger to be dropped.
			r: Additional SQL options, including 'if_exists' to avoid errors if the trigger does not exist.

		Returns:
			bool: True if the trigger was successfully dropped.

		Raises:
			Exception: If an error occurs during the execution of the drop trigger statement.
		"""
		try:
			self.cur.execute(f'''DROP TRIGGER {'IF EXISTS' if r.get('if_exists',False) else ''} {q}''')
			return True
		except BaseException as e:
			raise e
