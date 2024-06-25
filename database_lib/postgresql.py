import psycopg2
import sys
import os
import re

class queryPY(psycopg2.extensions.connection):

	def __init__(self,data,paramets):
		self.paramets = paramets
		self.paramets['attempts'] = self.paramets.get('attempts',0)
		if self.paramets['attempts']>=self.paramets.get('max_attempts',5):
			raise Exception('Attempts to reconnect exceeded the norm')
		self.DATA_CONNECT = data
		self.DB_NAME_ORG = data["database"]+'.'+data['schema']
		self.DB_NAME = (data["database"]+'_'+data['schema']).replace(' ','_')
		try:
			conn_string = f"postgres://{data['user']}:{data['password']}@{data.get('host', 'localhost')}:{data.get('port', 5432)}/{data['database']}"
			super(queryPY,self).__init__(conn_string)
			self.version = lambda : self.query_f("VERSION")
		except BaseException as e:
			self.close()
			raise e
	def __str__(self):
		return f'''{self.res}'''
	def close_cur(self):
		if hasattr(self,'cur'):
			self.cur.close()
			delattr(self,'cur')
	def open(self):
		self.close_cur()
		self.__init__(self.DATA_CONNECT)
	def is_active(self):
		try:
			self.close_cur()
			self.cur = self.cursor()
			self.cur.execute('SELECT 1')
			self.close_cur()
			return True
		except psycopg2.Error:
			return False
	def query_f(self,method:str,que={},req={}):
		try:
			self.cur = self.cursor()
			self.res = self.functinon_list(method)(que,req)
			if self.paramets.get('auto_commit',False): self.commit()
		except psycopg2.Error as e:
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
	def functinon_list(self,m):
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
	def version_f(self,q,r):
		self.cur.execute("SELECT VERSION()")
		return self.cur.fetchone()[0]
	def selection_f(self,q,r):
		if not isinstance(q,dict):
			raise Exception('Data type must be dictionary')
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
		if not isinstance(q,dict):
			raise Exception('Data type must be dictionary')
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
		updates = []
		for tab, val in q.items():
			updates.append(f"""UPDATE {tab} SET {','.join([f'{c}={v}' for c,v in val.items()]) } {r}""")
		try:
			self.cur.execute(';'.join(updates))
			return True
		except BaseException as e:
			raise e
	def delete_f(self,q,r):
		deletes = []
		for tab in q:
			deletes.append(f"""DELETE FROM {tab} {r}""")
		try:
			self.cur.execute(';'.join(deletes))
		except BaseException as e:
			raise e
		return True
	def drop_f(self,q,r):
		try:
			self.cur.execute(f"""DROP TABLE {q};""")
		except BaseException as e:
			raise e
		return True
	def show_table_f(self,q,r):
		show = f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{self.DATA_CONNECT['schema']}' AND table_catalog = '{self.DATA_CONNECT['database']}' AND table_type = 'BASE TABLE'"
		self.cur.execute(show)
		return tuple(filter(None,zip(*self.cur.fetchall())))
	def show_field_f(self,q,r):
		ex = list(map(str,q))
		show = f"""SELECT c.column_name, c.data_type AS column_type, c.is_nullable, c.column_default, k.constraint_name AS column_key 
			FROM information_schema.columns c
				LEFT JOIN information_schema.key_column_usage k
					ON c.column_name = k.column_name AND c.table_name = k.table_name
			WHERE c.table_name = '{ex[0]}';"""
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
	def show_coll_f(self,q,r):
		names = {}
		if len(q)==0:
			return {}
		q = q[0]
		for i in q:
			show = f"SELECT * FROM {i}"
			self.cur.execute(show)
			names[i] = tuple(description[0] for description in self.cur.description)
		return names
	def create_f(self,q,r):
		creates = []
		not_exists = 'IF NOT EXISTS' if r.get('not_exists', False) else ''
		for tab, cols in q.items():
			creates.append(f'''CREATE TABLE {tab} {not_exists} ({f', '.join([
				key+' '+' '.join(filter(None,val)) if isinstance(val,(tuple,list)) else key+' '+val
				for key,val in cols.items() if key not in ['__TC_DATANAME__']
			])})''')
		try:
			self.cur.execute(';'.join(creates))
		except BaseException as e:
			raise e
		return True
	def add_column_f(self,q,r):
		alters = []
		for tab, vals in q.items():
			alters.exctend([f'''ALTER TABLE {tab} ADD COLUMN {col} {' '.join(val)}''' for col, val in vals.items()])
		try:
			self.cur.execute(';'.join(alters))
		except BaseException as e:
			raise e
		return True
	def alter_column_f(self,q,r):
		alters = []
		for tab, vals in q.items():
			alters.exctend([f'''ALTER TABLE {tab} ALERT COLUMN {col} SET {' '.join(val)}''' for col, val in vals.items()])
		try:
			self.cur.execute(';'.join(alters))
		except BaseException as e:
			raise e
		return True
	def drop_column_f(self,q,r):
		alters = [f'''ALTER TABLE {tab} DROP COLUMN {vals}''' for tab, vals in q.items()]
		try:
			self.cur.execute(';'.join(alters))
		except BaseException as e:
			raise e
		return True
	def rename_table_f(self,q,r):
		alters = []
		for tab, vals in q.items():
			alters.append(f'''ALTER TABLE {tab} RENAME TO {vals}''')
		try:
			self.cur.execute(';'.join(alters))
		except BaseException as e:
			raise e
		return True
	def rename_column_f(self,q,r):
		alters = []
		for tab, vals in q.items():
			alters.append(f'''ALTER TABLE {tab} RENAME COLUMN {vals.get('old_name','')} TO {vals.get('new_name','')}''')
		try:
			self.cur.execute(';'.join(alters))
		except BaseException as e:
			raise e
		return True
	def create_index_f(self,q,r):
		indexs = []
		for tab, val in q.items():
			indexs.append(str(val).format(table=tab))
		try:
			self.cur.execute(';'.join(indexs))
		except BaseException as e:
			raise e
	def drop_index_f(self,q,r):
		try:
			self.cur.execute(f'''DROP INDEX {q}''')
		except BaseException as e:
			raise e
	def show_index_f(self,q,r):
		try:
			self.cur.execute(f'''SELECT
				indexname AS index_name,
				NOT indisunique AS non_unique,
				attname AS column_name,
				amname AS index_type
			FROM pg_indexes
			JOIN pg_class ON indexname = pg_class.relname
			JOIN pg_index ON pg_class.oid = pg_index.indexrelid
			JOIN pg_attribute ON pg_attribute.attrelid = pg_class.oid AND pg_attribute.attnum = ANY(pg_index.indkey)
			JOIN pg_am ON pg_am.oid = pg_class.relam
			WHERE tablename = '{q}';''')
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
	def show_foreign_f(self,q,r):
		try:
			self.cur.execute(f'''SELECT
	TC.CONSTRAINT_NAME AS FOREIGN_KEY_NAME,
	CCU.TABLE_NAME AS REFERENCED_COLUMN_TABLE_NAME,
	KCU.COLUMN_NAME,
	CCU.COLUMN_NAME AS REFERENCED_COLUMN_NAME,
	RC.UPDATE_RULE,
	RC.DELETE_RULE,
	RC.MATCH_OPTION
FROM
	INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC
JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KCU
	ON TC.CONSTRAINT_NAME = KCU.CONSTRAINT_NAME
JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE AS CCU
	ON CCU.CONSTRAINT_NAME = TC.CONSTRAINT_NAME
JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS AS RC
	ON TC.CONSTRAINT_NAME = RC.CONSTRAINT_NAME
WHERE
	TC.CONSTRAINT_TYPE = 'FOREIGN KEY'
	AND TC.TABLE_SCHEMA = '{self.DATA_CONNECT['schema']}'
	AND TC.TABLE_NAME = '{q}';''')
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
	def create_foreign_f(self,q,r):
		try:
			self.cur.execute(f'''ALTER TABLE {q} ADD CONSTRAINT {r};''')
			return True
		except BaseException as e:
			raise e

	def show_procedure_f(self,q,r):
		try:
			self.cur.execute(f"""SELECT  p.proname AS function_name, pg_get_function_arguments(p.oid) AS arguments FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid WHERE n.nspname = '{self.DATA_CONNECT['schema']}' AND prokind = 'p';""")
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
	def create_procedure_f(self,q,r):
		try:
			proc = [
				f'CREATE OR REPLACE PROCEDURE {q.name}',
			]
			proc[0] += f'''({','.join([f'{q.__params__(param_name,"direction","IN").upper()} {param_name} {param.annotation(q.db.dataTypes)} {q.__params__(param_name,"parameter")}' for param_name, param in q.parameters.items()])})'''
			proc[0] += f'''\nLANGUAGE plpgsql\nAS $$\n\t{q.body}\n$$;'''
			for i in proc:
				if len(i.strip())>0:
					self.cur.execute(i)
			return True
		except BaseException as e:
			raise e
	def run_procedure_f(self,q,r):
		try:
			procedure = q.get('procedure',None)
			parameters = q.get('parameters',[])
			params = q.get('params',[])
			if len(params) != len(parameters):
				raise Exception('Parameter lengths do not match')
			if procedure is None:
				raise Exception('You are missing a procedure')
			if q.get('call',False):
				self.cur.execute(f"""CALL {procedure} ("""+ ','.join(parameters) +f""");""")
				return self.cur.fetchall()
			declare = f"""DO $$
				DECLARE\n """+ ";".join([f'p{i} {v}' for i,v in enumerate(params)])+""";
				BEGIN
					"""+ "\n".join([f'p{i} := {v};' for i,v in enumerate(parameters)])+f"""
					CALL {procedure} ("""+ ','.join([f'p{i}' for i,v in enumerate(parameters)]) +f""");
				END;
			$$; """
			self.cur.execute(declare)
			return True
		except BaseException as e:
			raise e
	def drop_procedure_f(self,q,r):
		try:
			self.cur.execute(f'''DROP PROCEDURE {'IF EXISTS' if r.get('if_exists',False) else ''} {q}''')
			return True
		except BaseException as e:
			raise e
	def show_function_f(self,q,r):
		try:
			self.cur.execute(f"""SELECT  p.proname AS function_name, pg_get_function_arguments(p.oid) AS arguments FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid WHERE n.nspname = '{self.DATA_CONNECT['schema']}' AND prokind = 'f';""")
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
	def create_function_f(self,q,r):
		try:
			funct = f"""CREATE OR REPLACE FUNCTION {q.name}"""
			funct += f'''({','.join([f'{param_name} {param.annotation(q.db.dataTypes)}' for param_name, param in q.parameters.items()])})\nRETURNS {q.return_type(q.db.dataTypes)} AS $$'''
			funct += f'''\n\t{q.body}\n$$ LANGUAGE plpgsql;'''
			self.cur.execute(funct)
			return True
		except BaseException as e:
			raise e
	def run_function_f(self,q,r):
		try:
			function = q.get('function',None)
			parameters = q.get('parameters',[])
			if function is None:
				raise Exception('You are missing a function')
			parel = f'''SELECT {function}('''+','.join(parameters)+f''') AS {function}'''
			self.cur.execute(parel)
			result = dict(map(lambda x: (function,x),self.cur.fetchone()))
			return result
		except BaseException as e:
			raise e
	def drop_function_f(self,q,r):
		try:
			self.cur.execute(f'''DROP FUNCTION {'IF EXISTS' if r.get('if_exists',False) else ''} {q}''')
			return True
		except BaseException as e:
			raise e
	def show_trigger_f(self,q,r):
		try:
			self.cur.execute(f"""SELECT * FROM information_schema.triggers WHERE trigger_schema='{self.DATA_CONNECT['schema']}';""")
			result = self.cur.fetchall()
			data = {}
			for res in result:
				if res[6]==q:
					data[res[2]] = {
						'time':res[11],
						'event':res[3]
					}
			return data
		except BaseException as e:
			raise e
	def create_trigger_f(self,q,r):
		try:
			trig = f'CREATE OR REPLACE TRIGGER {q.name} {q.time} {q.event} ON {q.db.db_name}.{q.table.table}'
			trig += f'''\n\t{q.body}'''
			self.cur.execute(trig)
			return True
		except BaseException as e:
			raise e
	def drop_trigger_f(self,q,r):
		try:
			self.cur.execute(f'''DROP TRIGGER {'IF EXISTS' if r.get('if_exists',False) else ''} {q}''')
			return True
		except BaseException as e:
			raise e
