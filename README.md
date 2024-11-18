# PyTopConnect 1.1.X
The PyTopConnect module is a tool for working with various database management systems (DBMS) based on object-oriented database (OODB) principles. The main purpose of the module is to facilitate interaction between different databases, allowing them to exchange information and work together.

Key features of the PyTopConnect module include:
1. Support for Various DBMS: The module provides the ability to work with different database management systems such as MySQL, PostgreSQL, SQLite and others.

2. Establishing Connections Between Databases: PyTopConnect enables establishing connections between different databases, facilitating data transfer and collaboration.

3. Data Exchange Methods: The module offers methods for exchanging data between databases, including reading, writing, updating, and deleting information.

4. Error Handling and Exceptions: PyTopConnect handles errors and exceptions that may occur when working with databases to ensure stable application performance.

By using the PyTopConnect module, developers can easily create applications that interact with multiple databases simultaneously, ensuring efficient data management and enhanced application functionality.
## Installation
Supports version with Python 3.6 and higher
It includes modules:

1. pymysql >= 1.0

2. psycopg2 >= 2.8

3. numpy >= 1.20

4. pandas >= 1.2

5. tqdm >= 4.60

6. fuzzywuzzy >= 0.18

```bash
pip install -U pytopconnect
```

## Documentation
You can study the full documentation on [ptc.npaw.ru](https://ptc.npaw.ru "ptc.npaw.ru")
### Module connection
```py
import os
from pytopconnect import QueryRead as QR

DATA = {
	"postgesql":[{"host":"localhost","port":5432,"user":"admin","password":"root","database":"","schema":""}],
	"mysql":[{"host":"localhost","port":3306,"user":"admin","password":"root","database":""}],
	"sqlite":[{"dbFile": "exempel.db"}]
}

DATA_BASE = QR(DATA)
PSQL_DB = DATA_BASE.postgesql
MySQL_DB = DATA_BASE.mysql
SQLITE_DB = DATA_BASE.sqlite
```
### Getting data
```py
from pytopconnect.condition import Condition, Where, OrderBy

cond = Condition(MySQL_DB.database.table)
where = {
	"age":lambda col, **kwargs: Where(col, f"{col.column} > 15"),
	"name":lambda col, **kwargs: Where(col, f"{col.length(**kwargs)} > 5")
}
cond.where(where,options=["and"])
cond.orderBy(OrderBy(MySQL_DB.database.table.age))

result = MySQL_DB.database.table.get(["name","age"],condition=cond)

```
#### Parameters for the get method
1. columns: list = None: This argument represents a list of columns to be selected from the table. By default, it is set to None, which means selecting all columns. If a list of columns is passed, only those columns will be selected.

2. condition: Condition = Condition(): This argument represents the condition that the data rows must satisfy. By default, it is an empty condition. If a specific condition is provided, the data will be selected according to that condition.

3. distinct: bool = False: This argument indicates whether only unique data rows should be selected. By default, it is set to False, meaning all data rows are selected.

4. sql: bool = False: This argument specifies whether to execute an SQL query to fetch the data. By default, it is set to False. If set to True, the method will execute an SQL query to select the data.

#### Request result
The get method is used to retrieve data from a table considering the specified parameters. It validates the data types of the arguments, then selects columns and applies conditions to the data before returning it as a DataFrame.
### Adding data
```py
values = [['Alex',13],['Rick',9]]

columns = ['name','age']

DB_MYSQL.database.table.add(values,columns)
```
#### Parameters for the add method
1. values: list: This argument represents a list of values to be added to the table. Each value in the list corresponds to one data row, where the order of values matches the order of columns.

2. columns: list: This argument represents a list of columns to which values from 'values' should be added. The order of columns in the list must match the order of values in 'values'.

#### Request result
The add method is used to add new data rows to a table. It validates the data types of the arguments, then checks for the existence of the specified columns in the table. Values are then added to the table, an SQL insert query is formed, the query is executed, and the DataFrame is updated with the new data. If the data is successfully added, the method returns True; otherwise, it returns False. Any errors that occur are caught, and an exception is raised.

### Data update
```py
from pytopconnect.condition import Condition, Where

cond = Condition(MySQL_DB.database.table)
where = {
	"name":lambda col, **kwargs: Where(col, f"{col.length(**kwargs)} <= 5")
}
cond.where(where)

colval = {
	"age":0,
	"adress":None
}

DB_MYSQL.database.table.update(colval,cond)
```
#### Parameters for the update method
1. items: dict: This argument represents a dictionary where keys correspond to the columns of the table that need to be updated, and values correspond to the new values for those columns.

2. condition: Condition=Condition(): This argument represents the condition based on which the data should be updated. By default, no condition is set.

#### Request result
The data types of the items and condition arguments are validated. If they do not match the expected types, an exception is raised. A query to update data in the table is formed considering the provided values and condition. If the data update is successful, the corresponding values in the DataFrame are updated. The method returns True if the update is successful, otherwise it returns False. Any errors that occur are caught and an exception is raised.

### Data deletion
```py
from pytopconnect.condition import Condition, Where

cond = Condition(MySQL_DB.database.table)
where = {
	"id":lambda col, **kwargs: Where(col, f"(({col.column}%2)==0) and ({col.column} > 10)")
}
cond.where(where)

DB_MYSQL.database.table.delete(cond)
```
#### Parameters for the delet method
1. condition: Condition=Condition(): This argument represents the condition based on which the data should be deleted from the table. By default, no condition is set.


#### Request result
The data type of the condition argument is validated. If it does not match the expected type, an exception is raised. A query to delete data from the table is formed considering the provided condition. If the data deletion is successful, the corresponding rows are deleted from the DataFrame. The method returns True if the deletion is successful, otherwise it returns False. Any errors that occur are caught and an exception is raised.