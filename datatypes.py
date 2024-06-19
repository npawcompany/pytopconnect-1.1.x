from .condition import *
from datetime import *
from decimal import Decimal
import json
import urllib.parse
import types
from typing import Union

def __check_variable_name__(text):
	pattern = r'^[a-zA-Z_][a-zA-Z0-9_]*$'
	return re.match(pattern, text)

class DataTypes:

	def __init__(self,dn):
		self.__TC_DATANAME__ = dn

	# Строки запроса

	def NULL(self,x:bool=True) -> str:
		if x is None:
			return None
		return 'NULL' if x else 'NOT NULL'

	def DEFAULT(self,x=None) -> str:
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

	def PRIMARY(self,x:bool=True) -> Union[str,None]:
		return 'PRIMARY KEY' if x is not None else None

	def UNIQUE(self,x:bool=True) -> Union[str,None]:
		return 'UNIQUE' if x is not None else None

	def AUTO(self,x:bool=True) -> Union[str,None]:
		if self.__TC_DATANAME__.lower() in ['sqlite']:
			return "AUTOINCREMENT" if x else None
		elif self.__TC_DATANAME__.lower() in ['mysql']:
			return "AUTO_INCREMENT" if x is not None else None
		elif self.__TC_DATANAME__.lower() in ['sqlserver']:
			return "IDENTITY(1,1)" if x is not None else None

	def COMMENT(self,x:str=None) -> str:
		comm = f"COMMENT('{x}')" if x is not None else None
		if self.__TC_DATANAME__.lower() in ['mysql','sqlserver']:
			return re.sub(r"COMMENT\(([^)]+)\)", r"COMMENT \1", comm)
		return comm

	def CHECK(self,column:str,operator:str,x=None) -> str:
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

	def FOREIGN(self,column:str,references:pd.DataFrame, name:str='',table:pd.DataFrame=None ) -> str:
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

	def REFERENCES(self,references:pd.DataFrame ) -> str:
		if self.__TC_DATANAME__.lower() not in ['sqlite','postgresql']:
			return ''
		if not isinstance(references,pd.DataFrame):
			raise QueryException(f'Table was not found or does not exist')
		prime_key = tuple(dict(filter(lambda x: x[1].get('is_primary',False) ,references.types().items())).keys())
		if len(prime_key)==0:
			raise QueryException(f'Table "{references.table}" is missing a primary key')
		return f"""REFERENCES {references.table} ({prime_key[0]})"""

	def COLLATE(self, func:str=None):
		if self.__TC_DATANAME__.lower() in ['sqlite']:
			if func is not None:
				if func.strip().upper() not in ['BINARY','NOCASE','RTRIM','LIKE','LOCALIZED']:
					raise QueryException(f'COLLATE list does not have this method "{func.strip().upper()}"')
			return f'COLLATE {func.strip().upper() if func else "BINARY"}'
		elif self.__TC_DATANAME__.lower() in ['mysql']:
			pass

	def ENGINES(self, x:str=None):
		if self.__TC_DATANAME__.lower() in ['mysql']:
			if x is not None or x in ['InnoDB','MyISAM','Memory','CSV','Archive','Blackhole','NDB','Merge','Federated','Example']:
				return f'ENGINE = {x}'
			return 'ENGINE = InnoDB'

	def TRIGGER(self):
		if self.__TC_DATANAME__.lower() in ['postgresql']:
			return 'trigger'

	# Логические данные

	def BOOL(self) -> Union[str,None]:
		if self.__TC_DATANAME__.lower() in ['mysql']:
			return 'TINYINT(1)'
		elif self.__TC_DATANAME__.lower() in ['sqlite','postgresql']:
			return 'BOOLEAN'
		elif self.__TC_DATANAME__.lower() in ['sqlserver']:
			return 'BIT(1)'

	# Числовые данные

	def SERIAL(self) -> Union[str,None]:
		if self.__TC_DATANAME__.lower() in ['postgresql']:
			return 'SERIAL'

	def SMALLSERIAL(self) -> Union[str,None]:
		if self.__TC_DATANAME__.lower() in ['postgresql']:
			return 'SMALLSERIAL'

	def BIGSERIAL(self) -> Union[str,None]:
		if self.__TC_DATANAME__.lower() in ['postgresql']:
			return 'BIGSERIAL'

	def INTEGER(self,x:int=None) -> Union[str,None]:
		if self.__TC_DATANAME__.lower() in ['sqlite','postgresql']:
			return (f'INTEGER({x})' if x>-2147483648 and x <2147483648 else None) if x is not None else 'INTEGER'

	def INT(self,x:int=None) -> Union[str,None]:
		if self.__TC_DATANAME__.lower() in ['mysql','sqlite','sqlserver']:
			return (f'INT({x})' if x>-2147483648 and x <2147483648 else None) if x is not None else 'INT'

	def TINYINT(self,x:int=None) -> Union[str,None]:
		if self.__TC_DATANAME__.lower() in ['mysql','sqlserver']:
			return (f'TINYINT({x})' if x>-128 and x <128 else None) if x is not None else 'TINYINT'

	def BIT(self,x:int=None) -> Union[str,None]:
		if self.__TC_DATANAME__.lower() in ['mysql']:
			return (f'BIT({x})' if x>-128 and x <128 else None) if x is not None else 'BIT'

	def SMALLINT(self,x:int=None) -> Union[str,None]:
		if self.__TC_DATANAME__.lower() in ['mysql','sqlserver','postgresql']:
			return (f'SMALLINT({x})' if x>-32768 and x <32768 else None) if x is not None else 'SMALLINT'

	def BIGINT(self,x:int=None) -> Union[str,None]:
		if self.__TC_DATANAME__.lower() in ['mysql','sqlserver','postgresql']:
			return (f'BIGINT({x})' if x>-9223372036854775808 and x <9223372036854775808 else None) if x is not None else 'BIGINT'

	def MEDIUMINT(self,x:int=None) -> Union[str,None]:
		if self.__TC_DATANAME__.lower() in ['mysql']:
			return (f'MEDIUMINT({x})' if x>-32768 and x <32768 else None) if x is not None else 'MEDIUMINT'

	def NUMERIC(self,x:int,d:int=0) -> Union[str,None]:
		if self.__TC_DATANAME__.lower() in ['postgresql']:
			return f'NUMERIC({x},{d})'

	def FLOAT(self,x:int=None) -> Union[str,None]:
		if self.__TC_DATANAME__.lower() in ['mysql']:
			return (f'FLOAT({x})' if x>-1.175494351*(10**(-39)) and x <1.175494351*(10**(-39)) else None) if x is not None else 'FLOAT'
		elif self.__TC_DATANAME__.lower() in ['sqlite','postgresql']:
			return (f'REAL({x})' if x>-1.175494351*(10**(-39)) and x <1.175494351*(10**(-39)) else None) if x is not None else 'REAL'

	def DOUBLE(self,x:int=None,d:int=2) -> Union[str,None]:
		if self.__TC_DATANAME__.lower() in ['mysql','sqlite']:
			return (f'DOUBLE({x})' if x>-2.2250738585072015*(10**(-308)) and x <2.2250738585072015*(10**(-308)) else None) if x is not None else 'DOUBLE'
		elif self.__TC_DATANAME__.lower() in ['postgresql']:
			return f'DOUBLE PRECISION'

	def DECIMAL(self,x:int=None,d:int=2) -> Union[str,None]:
		if self.__TC_DATANAME__.lower() in ['mysql','sqlite','postgresql']:
			return (f'DECIMAL({x},{d})' if x>-99.99 and x <99.99 else None) if x is not None else 'DECIMAL'

	# Текстовые данные

	def STRING(self,n:int=None) -> Union[str,None]:
		if self.__TC_DATANAME__.lower() in ['sqlite']:
			return (f'STRING({n})') if n != None else 'STRING'

	def TEXT(self) -> Union[str,None]:
		if self.__TC_DATANAME__.lower() in ['mysql','sqlite','postgresql']:
			return 'TEXT'

	def TINYTEXT(self) -> Union[str,None]:
		if self.__TC_DATANAME__.lower() in ['mysql']:
			return 'TINYTEXT'

	def MEDIUMTEXT(self) -> Union[str,None]:
		if self.__TC_DATANAME__.lower() in ['mysql']:
			return 'MEDIUMTEXT'

	def LONGTEXT(self) -> Union[str,None]:
		if self.__TC_DATANAME__.lower() in ['mysql']:
			return 'LONGTEXT'

	def CHAR(self,n:int) -> Union[str,None]:
		if self.__TC_DATANAME__.lower() in ['mysql','sqlite']:
			return f'CHAR({n})' if type(n) is int else None
		elif self.__TC_DATANAME__.lower() in ['postgresql']:
			return f'CHARACTER({n})'

	def VARCHAR(self,n:int) -> Union[str,None]:
		if self.__TC_DATANAME__.lower() in ['mysql','sqlite']:
			return f'VARCHAR({n})' if type(n) is int else None
		elif self.__TC_DATANAME__.lower() in ['postgresql']:
			return f'CHARACTER VARYING({n})'

	# Структурные данные

	def JSON(self) -> Union[str,None]:
		if self.__TC_DATANAME__.lower() in ['mysql','postgresql']:
			return 'JSON'

	# Временые данные

	def DATE(self) -> Union[str,None]:
		if self.__TC_DATANAME__.lower() in ['mysql','sqlite','postgresql']:
			return 'DATE'

	def TIME(self) -> Union[str,None]:
		if self.__TC_DATANAME__.lower() in ['mysql','sqlite','postgresql']:
			return 'TIME'

	def DATETIME(self) -> Union[str,None]:
		if self.__TC_DATANAME__.lower() in ['mysql','sqlite','postgresql']:
			return 'DATETIME'

	def TIMESTAMP(self) -> Union[str,None]:
		if self.__TC_DATANAME__.lower() in ['mysql','postgresql']:
			return 'TIMESTAMP'

	def YEAR(self) -> Union[str,None]:
		if self.__TC_DATANAME__.lower() in ['mysql']:
			return 'YEAR'

	def INTERVAL(self) -> Union[str,None]:
		if self.__TC_DATANAME__.lower() in ['postgresql']:
			return 'INTERVAL'

	# Бинарные типы данных

	def TINYBLOB(self) -> Union[str,None]:
		if self.__TC_DATANAME__.lower() in ['mysql','sqlite']:
			return 'TINYBLOB'

	def BLOB(self) -> Union[str,None]:
		if self.__TC_DATANAME__.lower() in ['mysql','sqlite']:
			return 'BLOB'

	def MEDIUMBLOB(self) -> Union[str,None]:
		if self.__TC_DATANAME__.lower() in ['mysql','sqlite']:
			return 'MEDIUMBLOB'

	def LONGBLOB(self) -> Union[str,None]:
		if self.__TC_DATANAME__.lower() in ['mysql','sqlite']:
			return 'LONGBLOB'

	def LARGEBLOB(self) -> Union[str,None]:
		if self.__TC_DATANAME__.lower() in ['mysql','sqlite']:
			return 'LARGEBLOB'

	def BYTEA(self) -> Union[str,None]:
		if self.__TC_DATANAME__.lower() in ['postgresql']:
			return 'BYTEA'

	# Составные типы

	def ENUM(self) -> Union[str,None]:
		if self.__TC_DATANAME__.lower() in ['mysql']:
			return 'ENUM'

	def SET(self) -> Union[str,None]:
		if self.__TC_DATANAME__.lower() in ['mysql']:
			return 'SET'

class CollationMappings:
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
	def get_collations(cls, key:str):
		return getattr(cls, key, [])

	@classmethod
	def get_collation(cls, key:str):
		coll = cls.get_collations(key.split('_')[0])
		return key if key in coll else None

	@classmethod
	def get(cls, key:str='utf8mb4'):
		return key if hasattr(cls,key) else 'utf8mb4'

class Index:

	def __init__(self, name:str, dbname:str,
		NOT_EXISTS=False,
		UNIQUE:bool=False, CLUSTERED:bool=False, FULLTEXT:bool=False, SPATIAL:bool=False,
		USING:Union[tuple,list,str]='BTREE',
		INCLUDES:Union[tuple,list]=[],
		WHERE:str='',
		**columns
	):
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
		if self.__TC_DATANAME__ in ['sqlite']:
			return f'''CREATE {self.unique} {self.clustered} INDEX {self.exists} {self.name} ON {"{table}"} ({','.join(self.columns)}) {self.where}'''
		elif self.__TC_DATANAME__ in ['mysql']:
			return f'''CREATE {self.unique} {self.fulltext}{self.spatial} INDEX {self.name} ON {"{table}"} ({','.join(self.columns)}) USING {self.using} {self.where}'''
		elif self.__TC_DATANAME__ in ['sqlserver','postgresql']:
			return f'''CREATE {self.unique} {self.clustered} INDEX {self.exists} {self.name} ON {"{table}"} ({','.join(self.columns)}) {f'INCLUDE ({",".join(self.includes)})'if len(self.includes)>0 else ""} {self.where}'''
		return ''

