import pymysql
import sys
import os
import re

class queryPY(pymysql.connections.Connection):

	def __init__(self,data,paramets):
		self.paramets = paramets
		self.paramets['attempts'] = self.paramets.get('attempts',0)
		if self.paramets['attempts']>=self.paramets.get('max_attempts',5):
			raise Exception('Attempts to reconnect exceeded the norm')
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
	def open(self):
		if hasattr(self,'cur'): self.cur.close()
		self.__init__(self.DATA_CONNECT,self.paramets)
	def is_active(self):
		try:
			if hasattr(self,'cur'): self.cur.close()
			with self.cursor() as cursor:
				cursor.execute('SELECT 1')
				cursor.close()
				return True
		except pymysql.Error:
			return False
	def query_f(self,method:str,que={},req={}):
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
			if hasattr(self,'cur'): self.cur.close()
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
		show = "SHOW TABLES FROM `"+self.DB_NAME+"`"
		self.cur.execute(show)
		return tuple(filter(None,zip(*self.cur.fetchall())))
	def show_field_f(self,q,r):
		ex = list(map(str,q))
		show = "SELECT `COLUMN_NAME`,`COLUMN_TYPE`,`IS_NULLABLE`,`COLUMN_DEFAULT`,`COLUMN_KEY` FROM INFORMATION_SCHEMA.COLUMNS WHERE `TABLE_NAME` = '"+ex[0]+"'"
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
			show = f"SELECT * FROM `{i}`"
			self.cur.execute(show)
			names[i] = tuple(description[0] for description in self.cur.description)
		return names
	def create_f(self,q,r):
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
			alters.exctend([f'''ALTER TABLE {tab} ALERT COLUMN {col} {' '.join(val)}''' for col, val in vals.items()])
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
	def show_foreign_f(self,q,r):
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
	def create_foreign_f(self,q,r):
		try:
			self.cur.execute(f'''ALTER TABLE {q} ADD CONSTRAINT {r};''')
			return True
		except BaseException as e:
			raise e

	def show_procedure_f(self,q,r):
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
	def create_procedure_f(self,q,r):
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
	def run_procedure_f(self,q,r):
		try:
			procedure = q.get('procedure',None)
			parameters = q.get('parameters',[])
			if procedure is None:
				raise Exception('You are missing a procedure')
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
				result = tuple(map(lambda x: dict(zip(r,x)),self.cur.fetchall()))
				return result
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
			self.cur.execute(f"""SELECT `SPECIFIC_NAME`, `PARAMETER_NAME` FROM `INFORMATION_SCHEMA`.`PARAMETERS` WHERE `SPECIFIC_SCHEMA`='{q}' AND `ROUTINE_TYPE`='FUNCTION';""")
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
	def run_function_f(self,q,r):
		try:
			function = q.get('function',None)
			parameters = q.get('parameters',[])
			if function is None:
				raise Exception('You are missing a function')
			pars = [f'SET @p{i} = {v};' for i,v in enumerate(parameters)] if isinstance(parameters,(list,tuple)) else [f'SET @p0 = {parameters};']
			parel = f'''SELECT `{function}`('''+(','.join([f'@p{i}' for i in range(len(parameters))]) if isinstance(parameters,(list,tuple)) else '@p0')+f''') AS {function}'''
			for i in pars:
				self.cur.execute(i)
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
			self.cur.execute(f"""SHOW TRIGGERS;""")
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
	def create_trigger_f(self,q,r):
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
	def drop_trigger_f(self,q,r):
		try:
			self.cur.execute(f'''DROP TRIGGER {'IF EXISTS' if r.get('if_exists',False) else ''} {q}''')
			return True
		except BaseException as e:
			raise e
