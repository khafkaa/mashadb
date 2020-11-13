import mysql.connector as engine
from mysql.connector import Error as SqlError
from pandas import DataFrame as df

from utilities.system.tty import echo
from utilities.decolab.boundinnerclass import BoundInnerClass
from utilities.iter.accessories import CustomDict, flatten

from utilities.databases.sql.expandops import expander
from utilities.databases.sql.expandops import logic, expansions
from utilities.databases.sql.expandops import expansion_operators

class MashaDB:

    def __init__(self, **kwargs):
        '''ARGUMENTS:
               user=username
               password=pass
               host=hostname
               database=dbname
           USAGE:
               db = MashaDB(user=username, password=pass, host=hostname, database=dbname)
        '''
        self.credentials = CustomDict(**kwargs).add(auth_plugin='mysql_native_password')
        self.user = self.credentials['user']
        self.host = self.credentials['host']
        self.database = self.credentials['database']
        self.verbose = True
        self.version = None

    def __enter__(self):
        self.verbose = False
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, traceback):
        self.commit()
        self.closeall()

    def __repr__(self):
        return f'{self.__status__()}'

    def __status__(self):
        connected = f'Masha Version {self.version}: Connected to Database: {self.database}'
        disconnected = f'Masha Version {self.version} Status Disconnected.'
        if self.version:
            return connected if self.konnect.is_connected() else disconnected
        else:
            return f'MashaDb: Use MashaDB.connect() to connect to {self.host}.'

    def __tables__(self):
        self.kursor.execute("SHOW TABLES")
        result = self.kursor.fetchall()
        return list(flatten(result))

    def __table_exists__(self, table):
        self.kursor.execute(f"show tables like '{table}'")
        return self.kursor.fetchone()[0]

    def __update_tables__(self):
        for table in self.tables:
            if not hasattr(self.Table, table):
                setattr(self, table, self.Table(table))

    def connect(self):
        try:
            self.konnect = engine.connect(**self.credentials)
            self.kursor = self.konnect.cursor(buffered=True)
            self.version = self.konnect.get_server_info()

            if self.konnect.is_connected():
                self.tables = self.__tables__()
                self.__update_tables__()
                if self.verbose:
                    echo.info(f"MariaDB {self.version}\nConnected to Database: {self.database}")

        except SqlError as error:
            echo.alert(f"Connection Error: {error}")

    def create(self, table, **kwargs):
        """Create a new table:
            Syntax:
                    db.create('tablename', id=primary(), FirstName=varchar(255), LastName=char(10))
        """
        statement = []
        for key, value in kwargs.items():
            if value.endswith('PRIMARY KEY'):
                statement.append(f"{key} {value}({key})")
            else:
                statement.append(f"{key} {value}")
        try:
            self.kursor.execute(f"CREATE TABLE IF NOT EXISTS {table}({', '.join(statement)})")
            echo.info(f'Created Table {table}')

        except SqlError as error:
            echo.alert(f"{error}")
        else:
            setattr(self, table, self.Table(table))
            self.tables = self.__tables__()

    def drop(self, table):
        try:
            delattr(self, table)
            self.kursor.execute(f"DROP TABLE IF EXISTS {table}")

        except AttributeError:
            echo.alert(f"The table '{table}' does not exist")

        except SqlError as error:
            echo.alert(error)
        else:
            self.tables = self.__tables__()
            echo.info(f"Table {table} has been deleted.")

    def rename(self, table, new_name):
        try:
            delattr(self, table)
            self.kursor.execute(f'ALTER TABLE {table} RENAME TO {new_name}')

        except AttributeError:
            echo.alert(f"The table '{table}' does not exist")

        except SqlError as error:
            echo.alert(error)

        else:
            setattr(self, new_name, self.Table(new_name))
            self.tables = self.__tables__()
            echo.info(f"Table {table} has been renamed {new_name}")

    def commit(self):
        try:
            self.konnect.commit()
            if self.verbose:
                echo.info("Data Commit")

        except SqlError as error:
            echo.alert(error)

    def rollback(self):
        try:
            self.konnect.rollback()
            echo.info("Rollback Successful")

        except SqlError as error:
            echo.alert(error)

    def closeall(self):
        if self.konnect.is_connected():
            self.kursor.close()
            self.konnect.close()
            if self.verbose:
                echo.info("MariaDB Server Has Disconnected. Session Ended.")

    @BoundInnerClass
    class Table:

        def __init__(self, outer, tablename):
            self._name = tablename
            self.kursor = outer.kursor
            self.verbose = outer.verbose
            self.rows = self.__rows__()
            self.columns = self.__columns__()
            self.primarykey = self.__get_primary__()

            setattr(self.Selector, 'kursor', self.kursor)

        def __repr__(self):
            rep = df(self.describe()).transpose().head(3)
            return f"{rep.rename(index={0: 'COLUMNS:', 1: 'TYPES:', 2: 'NULL'}).to_string(header=False)}"

        def __str__(self):
            return self._name

        def __rows__(self):
            self.kursor.execute(f"SELECT COUNT(*) FROM {self._name};")
            return self.kursor.fetchone()[0]

        def __columns__(self):
            return [column[0] for column in self.describe()]

        def __get_primary__(self):
            self.kursor.execute(f"SELECT COLUMN_NAME from information_schema.KEY_COLUMN_USAGE where TABLE_NAME='{self._name}' and constraint_name = 'PRIMARY'")
            try:
                return self.kursor.fetchone()[0]

            except TypeError:
                return 0

        def describe(self):
            try:
                self.kursor.execute(f"DESC {self._name};")

            except SqlError as error:
                echo.alert(error)
            else:
                return self.kursor.fetchall()

        def write(self, **kwargs):
            data = tuple(kwargs.values())
            columns = ', '.join(kwargs.keys())
            values = ('%s, ' * len(data)).strip(', ')
            try:
                self.kursor.execute(f'INSERT IGNORE INTO {self._name} ({columns}) VALUES ({values})', data)
                self.rows = self.__rows__()
                if self.verbose:
                    echo.info(f"{self.kursor.rowcount} record inserted into {self._name}")

            except SqlError as error:
                echo.alert(error)

        def update(self, id, **kwargs):
            data = tuple(kwargs.values())
            columns = f"{'=%s, '.join(kwargs.keys())}=%s"
            try:
                self.kursor.execute(f"UPDATE {self._name} SET {columns} WHERE {self.primarykey}={id}", data)
                echo.info(f"Updated Row: {id} Column(s): {columns.replace('=%s', '')}")

            except SqlError as error:
                echo.alert(error)

        def delete(self, id, row):
            try:
                self.kursor.execute(f"DELETE FROM {self._name} WHERE {id}={row}")
                echo.info(f"Deleted row {row} from {self._name}")

            except SqlError as error:
                echo.alert(error)

        def add(self, column, datatype, location='last'):
            self.kursor.execute(f"ALTER TABLE {self._name} ADD COLUMN {column} {datatype} {location}")
            echo.info(f"Added Column {column} To {self._name}")

        def drop(self, column):
            try:
                self.kursor.execute(f'ALTER TABLE {self._name} DROP COLUMN {column}')
                echo.info(f"Dropped column {column} from {self._name}")
                self.renumber()

            except SqlError as error:
                echo.alert(error)

        def rename(self, column, new_name):
            try:
                self.kursor.execute(f'ALTER TABLE {self._name} RENAME COLUMN {column} TO {new_name}')
                echo.info(f"Column {column} has been renamed {new_name}")

            except SqlError as error:
                echo.alert(error)

        def renumber(self):
            try:
                if self.primarykey:
                    self.kursor.execute(f'ALTER TABLE {self._name} DROP COLUMN {self.primarykey}')
                    self.kursor.execute(f'ALTER TABLE {self._name} ADD COLUMN {self.primarykey} INT NOT NULL AUTO_INCREMENT PRIMARY KEY FIRST')
                    echo.info(f"{self._name} {self.primarykey} index reset")
                else:
                    self.primarykey = 'id'
                    self.kursor.execute(f'ALTER TABLE {self._name} ADD COLUMN id INT NOT NULL AUTO_INCREMENT PRIMARY KEY FIRST')
                    echo.info(f"{self._name} {self.primarykey} index created")

            except SqlError as error:
                echo.alert(error)

        def record_exists(self, column, data):
            try:
                self.kursor.execute(f"SELECT EXISTS(SELECT 1 FROM {self._name} WHERE {column}='{data}' LIMIT 1)")
                return self.kursor.fetchone()[0]

            except SqlError as error:
                echo.alert(error)

        def distinct(self, *columns, count=False):
            selection = ', '.join(columns)
            if count:
                return f"SELECT COUNT(DISTINCT {self.selection}) FROM {self._name}"
            return f"SELECT DISTINCT {self.selection}) FROM {self._name}"

        def select(self, columns: str, **kwargs):
            return self.Selector(self._name, columns, **kwargs)

        class Selector:

            def __new__(cls, name: str, columns: str, filter: bool=False, sort: str=None, limit: str=None):
                columns = columns.strip()
                order = '' if sort is None else f"ORDER BY {sort}"
                limit = '' if limit is None else f"LIMIT {limit}"
                if filter is False:
                    columns = ', '.join(columns.replace(',', '').split())
                    columns = columns if columns else 'ALL'
                    cls.kursor.execute(f"SELECT {columns} FROM {name} {order} {limit}".strip())
                    return cls.kursor.fetchall()
                return object.__new__(cls)

            def __init__(self, name, columns: str, **kwargs: str) -> None:
                self.opts = CustomDict(**kwargs)
                self.opts.add(sort='')
                self.opts.add(limit='')
                self._name = name
                self.selection = 'ALL' if columns is None else columns.replace(' ', ', ')
                self.order = f"ORDER BY {self.opts['sort']}" if self.opts['sort'] else ''
                self.limit = f"LIMIT {self.opts['limit']}" if self.opts['limit'] else ''

            def __repr__(self):
                return f"You must call select.where(condition) if the filter flag is set to True"

            def expand(self, key, value):
                result = expansions.match(value).group(0)
                return expander[expansion_operators.search(result).group(0)](key, value)

            def where(self, condition: str=None, operator: str='OR', **kwargs: str) -> None:
                if condition:
                    self.kursor.execute(f"SELECT {self.selection} FROM {self._name} WHERE {condition}".strip())
                    return self.kursor.fetchall()

                statements = []
                conditions = []
                for key, value in kwargs.items():
                    values = logic.split(value)
                    for value in values:
                        if expansions.match(value):
                            clause = self.expand(key, value)
                            statements.append(clause)
                        else:
                            clause = f"{key}='{value}'"
                            statements.append(clause)
                        conditions.append(' OR '.join(statements))
                        statements.clear()

                chain = f' {operator} '.join(conditions)
                self.kursor.execute(f"SELECT {self.selection} FROM {self._name} WHERE {chain} {self.order} {self.limit}".strip())
                return self.kursor.fetchall()
