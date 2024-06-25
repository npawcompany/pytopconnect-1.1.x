import sqlite3
import sys
import os
import re

class queryPY(sqlite3.Connection):

	def __init__(self,data,paramets):
		self.paramets = paramets
		self.paramets['attempts'] = self.paramets.get('attempts',0)
		if self.paramets['attempts']>=self.paramets.get('max_attempts',5):
			raise Exception('Attempts to reconnect exceeded the norm')
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
		if hasattr(self,'cur'):
			self.cur.close()
			delattr(self,'cur')
	def open(self):
		self.close_cur()
		self.__init__(self.DATA_CONNECT,self.paramets)
	def is_active(self):
		try:
			self.close_cur()
			self.cur = self.cursor()
			self.cur.execute('SELECT 1')
			self.close_cur()
			return True
		except sqlite3.Error:
			return False
	def query_f(self,method,que={},req={}):
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
		self.cur.execute("SELECT sqlite_version();")
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
		show = "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
		self.cur.execute(show)
		return tuple(zip(*self.cur.fetchall()+[('sqlite_master',)]))[0]
	def show_field_f(self,q,r):
		ex = list(map(str,q))
		show = f"PRAGMA table_info({ex[0]})"
		self.cur.execute(show)
		return self.cur.fetchall()
	def show_coll_f(self,q,r):
		ex = list(map(str,q))
		names = {}
		for i in ex:
			show = f"SELECT * FROM {i}"
			self.cur.execute(show)
			names[i] = tuple(description[0] for description in self.cur.description)
		return names
	def ddl_f(self,q,r):
		ddl = f"SELECT sql FROM sqlite_master WHERE name='{q}' AND type='table'"
		self.cur.execute(ddl)
		ddl = self.cur.fetchone()
		return ddl[0] if len(ddl)>0 else None
	def create_f(self,q,r):
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
	def alter_column_f(self,q,r):
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
		alters = []
		for tab, vals in q.items():
			alters.append(f'''ALTER TABLE {tab} DROP COLUMN {vals}''')
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
	def show_foreign_f(self,q,r):
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
	def create_foreign_f(self,q,r):
		try:
			self.cur.execute(f'''ALTER TABLE {q} ADD CONSTRAINT {r};''')
			return True
		except BaseException as e:
			raise e
