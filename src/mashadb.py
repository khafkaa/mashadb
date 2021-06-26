from itertools import chain
import mysql.connector as engine
from mysql.connector import Error as SQLError
from pandas import DataFrame as df

from src.utilities import echo
from src.utilities import expander
from src.utilities import logic, expansions
from src.utilities import expansion_operators
from src.boundinnerclass import BoundInnerClass


class MashaDB:
    """python interface for MySQL and MariaDB

       USAGE:
            Connect and Commit:

            db = MashaDB(**config)
            db.connect(database=db_name)
            db.commit()
            db.closeall()

            Write Operations:

            db.table.write(column=data, column=data, column=data)
            db.table.write(**data)

            Read Operations:

            data = db.table.select(column, column).all(sort=column)
            data = db.table.select(column).where(clause, limit=10)

            With Context Manager:
            Automatically commits data and closes the connection.

            from functools import partial
            masha = partial(MashaDB, **config)

            with masha(database=database) as db:
                db.table.write(**data)
                query = db.table.select(column).all()

            create a table:
            db.create(table: str, **kwargs: str)
            db.create(table: str,
                      id: str='INT AUTO_INCREMENT PRIMARY KEY'
                      username: str='VARCHAR(40)',
                      password: str='VARCHAR(255)'
                )
    """

    def __init__(self, **kwargs):
        """ ARGUMENTS:
                Required:

                user=username
                password=password
                host=hostname

                Optional:

                database=database_name

                any other keyword arguments that must be passed to
                mysql.connector to ensure its system compatibilty and
                function.

                see mysql-connector docs for all possible arguments and information:

                https://dev.mysql.com/doc/connector-python/en/connector-python-connectargs.html
                https://dev.mysql.com/doc/connector-python/en/connector-python-reference.html

            USAGE:
                pass a dict with parameters:

                config = {
                    user: username,
                    password: password,:
                    hostname: host,
                    port: 3306,
                    database: database_name,
                    auth_plugin: 'mysql_native_password'
                }

                db = MashaDB(**config)

                pass keyword arguments:
                db = MashaDB(user=user, password=password, host=host, database=database)
        """
        self.verbose = True
        self.config = kwargs
        self.host = self.config['host']
        self.database = self.config.get('database')

    def __server_connect__(self):
        self.konnect = engine.connect(**self.config)
        self.kursor = self.konnect.cursor(buffered=True)
        self.version = self.konnect.get_server_info()

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
        """displays database and connection status"""
        name = type(self).__name__
        version = self.__dict__.get('version')
        connection_type = self.config.get('database', self.host)

        connected = f"{name} Version {version}: Connected to {connection_type}"
        disconnected = f'{name} Version {version} Status Disconnected.'

        if version:
            return connected if self.konnect.is_connected() else disconnected

        return f"{name} isn't connected. Use obj.connect() to connect to {self.host}."

    def __update_tables__(self):
        """database tables are added as object attributes"""
        for table in self.tables:
            if not hasattr(self.Table, table):
                setattr(self, table, self.Table(table))

    @property
    def databases(self):
        """returns list of databases"""
        try:
            self.kursor.execute('SHOW DATABASES')
            return tuple(chain(*self.kursor.fetchall()))

        except SQLError as error:
            echo.alert(error)

    @property
    def tables(self):
        """returns a list of tables contained in the database"""
        try:
            self.kursor.execute("SHOW TABLES")
            return tuple(chain(*self.kursor.fetchall()))

        except (AttributeError, SQLError):
            echo.alert(f'{type(self).__name__} is not connected to a database')

    def connect(self, database=None):
        if database:
            self.config.update({'database': database})

        connection = self.config.get('database', self.host)

        try:
            self.__server_connect__()
            if connection != self.host and self.konnect.is_connected():
                self.__update_tables__()

            if self.verbose:
                echo.info(f"MashaDB {self.version} Connected to {connection}")

                if connection == self.host:
                    echo.info(("Use obj.connect(database='database_name'"
                               " to connect to a specific database."))

        except SQLError as error:
            echo.alert(error)

    def execute(self, query):
        """execute any mysql command or statement"""
        try:
            self.kursor.execute(query)
            if self.kursor.with_rows:
                return tuple(chain(*self.kursor.fetchall()))

            else:
                if self.verbose:
                    echo.info(f"Number of affected rows: {self.kursor.rowcount}")

        except SQLError as error:
            echo.alert(error)

    def table_exists(self, table):
        """checks for the existence of a table in the database"""
        self.kursor.execute(f"show tables like '{table}'")
        try:
            _ = self.kursor.fetchone()[0]
            return True

        except TypeError:
            return False

    def create(self, table: str, **kwargs: str) -> None:
        """create a new table in the database.

           ARGUMENTS:
                uses positional and keyword arguments:

                table: str: the new table name
                keyword:    the column name
                value: str: the column dataype; sql statement

            USAGE:
                db.create('table', **kwargs)
                db.create('table', column='datatype', column='datatype')
                db.create('users', id='INT AUTO_INCREMENT PRIMARY KEY')

                import Columns, Primary

                primary = Primary()
                name = Column('VARCHAR(100)')
                email = Column('VARCHAR(255', unique=True)

                db.create(id=primary.key, Name=name.column, Email=email.column)
        """
        statement = []
        for key, value in kwargs.items():
            if value.endswith('PRIMARY KEY'):
                statement.append(f"{key} {value}({key})")
            else:
                statement.append(f"{key} {value}")
        try:
            # print(f"CREATE TABLE IF NOT EXISTS {table}({', '.join(statement)})")
            self.kursor.execute(f"CREATE TABLE IF NOT EXISTS {table}({', '.join(statement)})")
            if self.verbose:
                echo.info(f'Created Table {table}')

        except SQLError as error:
            echo.alert(f"{error}")
        else:
            setattr(self, table, self.Table(table))

    def drop(self, table):
        """remove specified table from the database.

           ARGUMENTS:
                table: str: name of the table to be deleted

           USAGE:
                db.drop('users')
        """
        try:
            delattr(self, table)
            self.kursor.execute(f"DROP TABLE IF EXISTS {table}")

        except AttributeError:
            echo.alert(f"The table '{table}' does not exist")

        except SQLError as error:
            echo.alert(error)

        else:
            if self.verbose:
                echo.info(f"Table {table} has been deleted.")

    def rename(self, table, new_name):
        """rename a table in the database.

           ARGUMENTS:
                table:    str: name of the target table
                new_name: str: new name for the target table

           USAGE:
                db.rename('users', 'superusers')
        """
        try:
            delattr(self, table)
            self.kursor.execute(f'ALTER TABLE {table} RENAME TO {new_name}')

        except AttributeError:
            echo.alert(f"The table '{table}' does not exist")

        except SQLError as error:
            echo.alert(error)

        else:
            setattr(self, new_name, self.Table(new_name))
            echo.info(f"Table {table} has been renamed {new_name}")

    def commit(self):
        """commit the last transaction(s) and make changes permanent"""
        try:
            self.konnect.commit()
            if self.verbose:
                echo.info("Data Commit")

        except SQLError as error:
            echo.alert(error)

    def rollback(self):
        """roll back the current transaction and cancel its changes"""
        try:
            self.konnect.rollback()
            if self.verbose:
                echo.info("Rollback Successful")

        except SQLError as error:
            echo.alert(error)

    def closeall(self):
        """close the connection to the database"""
        if self.konnect.is_connected():
            self.kursor.close()
            self.konnect.close()
            if self.verbose:
                echo.info("MariaDB Server Has Disconnected. Session Ended.")

    @ BoundInnerClass
    class Table:
        """dynamic bound inner class of the MashaDB database object.

           each table in the target database is modeled as an attribute
           of the database object and is instantiated when operations require
           access to the associated database table.

           since a table is a child attribute of the database object, it
           is called usings standard dot notation.

           EXAMPLE:

            create a new database object:
                db = MashaDB(**config)

            connect to the target database:
                db.connect()

            list tables in the database:
                db.tables

            to perform operations on a table named 'users':

                db.users                  describes table users
                db.users.rows             list row count
                db.users.columns          list column names
                db.users.write(**kwargs)  write data to the table
                db.users.select(column)   lookup data in the table
        """

        def __init__(self, outer, tablename):
            self._name = tablename
            self._base = outer.config['database']
            self.kursor = outer.kursor
            self.verbose = outer.verbose
            setattr(self.Selector, 'kursor', self.kursor)

        def __repr__(self):
            rep = df(self.describe()).transpose().head(3)
            return f"{rep.rename(index={0: 'COLUMNS:', 1: 'TYPES:', 2: 'NULL'}).to_string(header=False)}"

        def __str__(self):
            return self._name

        @ property
        def rows(self):
            self.kursor.execute(f"SELECT COUNT(*) FROM {self._name};")
            return self.kursor.fetchone()[0]

        @ property
        def columns(self):
            return [column[0] for column in self.describe()]

        @ property
        def primary(self):
            self.kursor.execute(f"SELECT COLUMN_NAME from information_schema.KEY_COLUMN_USAGE \
                                where TABLE_NAME='{self._name}' and constraint_name = 'PRIMARY'")
            try:
                return self.kursor.fetchone()[0]

            except TypeError:
                return 0

        def describe(self):
            """returns information about data stored within the table.

               this method is called by __repr__, which formats the
               output using a pandas dataframe. This provides an elegant
               display while using the repl, but there is considerable
               overhead when loading the pandas module. It has no use in
               production so my advice is to disable it.
            """
            try:
                self.kursor.execute(f"DESC {self._name};")

            except SQLError as error:
                echo.alert(error)
            else:
                return self.kursor.fetchall()

        def write(self, **kwargs):
            """insert data into the table

               ARGUMENTS:
                uses kwargs:
                key:   n/a: column name
                value: str: data written to specified column

               USAGE:
                db.table.write(**data)
                db.table.write(column=data, column=data, column=data)
            """
            data = tuple(kwargs.values())
            columns = ', '.join(kwargs.keys())
            values = ('%s, ' * len(data)).strip(', ')
            try:
                self.kursor.execute(f'INSERT IGNORE INTO {self._name} ({columns}) VALUES ({values})', data)
                if self.verbose:
                    echo.info(f"{self.kursor.rowcount} record inserted into {self._name}")

            except SQLError as error:
                echo.alert(error)

        def update(self, id, **kwargs):
            """update columns in a table row with new data

               ARGUMENTS:
                    id:     int: str: the row number or id
                    kwargs:      str: column_name=new_data

               USAGE:
                    db.table.update('10', name='Someone', email='someone@example.com')
            """
            data = tuple(kwargs.values())
            columns = f"{'=%s, '.join(kwargs.keys())}=%s"
            try:
                self.kursor.execute(f"UPDATE {self._name} SET {columns} WHERE {self.primary}={id}", data)
                if self.verbose:
                    echo.info(f"Updated Row: {id} Column(s): {columns.replace('=%s', '')}")

            except SQLError as error:
                echo.alert(error)

        def delete(self, id, value):
            """delete a record in the table

               ARGUMENTS:
                    id:     str:      column containing row ids (usually named id)
                    value:  str: int: value that indicates the row to be deleted

               USAGE:
                    db.table.delete('user_id', '12')
            """
            try:
                self.kursor.execute(f"DELETE FROM {self._name} WHERE {id}={value}")
                if self.verbose:
                    echo.info(f"Deleted row {value} from {self._name}")

            except SQLError as error:
                echo.alert(error)

        def add(self, column, datatype, location='last'):
            """add a column to table

               ARGUMENTS:
                    column:   str: name of the new column
                    datatype: str: data type of new column i.e varchar(255)
                    location: str: where in the table the column will be inserted
                                   options are:
                                        'first', 'last' or f'after {column}'

               USAGE:
                    db.table.add('lastname', 'varchar(100)', location='after firstname')
            """
            self.kursor.execute(f"ALTER TABLE {self._name} ADD COLUMN {column} {datatype} {location}")
            echo.info(f"Added Column {column} To {self._name}")

        def drop(self, column):
            """drop a column from the table

               ARGUMENTS:
                    column: str: name of the new column

               USAGE:
                    db.table.drop('lastname')
            """
            try:
                self.kursor.execute(f'ALTER TABLE {self._name} DROP COLUMN {column}')
                if self.verbose:
                    echo.info(f"Dropped column {column} from {self._name}")
                self.renumber()

            except SQLError as error:
                echo.alert(error)

        def rename(self, column, new_name):
            """rename an column in the table"""
            try:
                self.kursor.execute(f'ALTER TABLE {self._name} RENAME COLUMN {column} TO {new_name}')
                if self.verbose:
                    echo.info(f"Column {column} has been renamed {new_name}")

            except SQLError as error:
                echo.alert(error)

        def renumber(self):
            """renumber all rows starting with 1. expects conventional primary key.
               fallback tries to renumber by a column named 'id' else it
               fails gracefully.
            """
            primary_key = self.primary
            try:
                if primary_key:
                    self.kursor.execute(f'ALTER TABLE {self._name} DROP COLUMN {primary_key}')
                    self.kursor.execute(f'ALTER TABLE {self._name} ADD COLUMN {primary_key} INT NOT NULL AUTO_INCREMENT PRIMARY KEY FIRST')
                    if self.verbose:
                        echo.info(f"{self._name} {primary_key} index reset")
                else:
                    primary_key = 'id'
                    self.kursor.execute(f'ALTER TABLE {self._name} ADD COLUMN id INT NOT NULL AUTO_INCREMENT PRIMARY KEY FIRST')
                    if self.verbose:
                        echo.info(f"{self._name} {primary_key} index created")

            except SQLError as error:
                echo.alert(error)

        def record_exists(self, column, data):
            """boolean test for the existence of a record within the table

               ARGUMENTS:
                    column: str: name of the target column
                    data:   str: the desired record

               USAGE:
                   if db.users.record_exists('email', 'someone@example.com'):
                       perform some operation....
            """
            try:
                self.kursor.execute(f"SELECT EXISTS(SELECT 1 FROM {self._name} WHERE {column}='{data}' LIMIT 1)")
                return self.kursor.fetchone()[0]

            except SQLError as error:
                echo.alert(error)

        def distinct(self, column, count=False):
            """select distinct records from specified column in the table
               returns the number of distinct records if count=True

               ARGUMENTS:
                    columns: str: column name

               USAGE:
                    data = db.users.distinct('lastname', 'country')
                    data = db.users.distinct('firstname', count=True)
            """
            try:
                if count:
                    self.kursor.execute(f"SELECT COUNT(DISTINCT {column}) FROM {self._name}")
                    return self.kursor.fetchone()[0]

                self.kursor.execute(f"SELECT DISTINCT {column} FROM {self._name}")
                result = self.kursor.fetchall()
                return tuple(chain(*result))

            except SQLError as error:
                echo.alert(error)

        def select(self, *columns: str):
            """create selection objects targeting single or multiple columns:
               calling select creates a Table.Selector object

               ARGUMENTS:
                    columns: str: a list of target columns names

               USAGE:
                    assuming a table in the database called 'subscribers':

                        cities = db.subscribers.select('city')
                        people = db.subscribers.select('people')

                    filter each selection object as desired

                    for complex clauses pass an explicit sql statement as str:
                        cities.where("city like '%ville order by city desc")

                    for brevity:
                        cities.all()
                        cities.where(people='Al or Bob or Vlad')
                        cities.where(op='or', gender='female', income='60000..80000')

                        people.all(sort='country desc', limit=10)
                        people.where(people='Al or Vlad', city='London or Moscow')

                    filtering:
                        where(people='tom', city='London or Moscow')
                        WHERE people EQUALS tom AND city EQUALS London OR city EQUALS Moscow;

                        where(operator='or', people='tom', city='london or moscow')
                        WHERE people EQUALS tom OR city EQUALS London OR city EQUALS Moscow;

                        where(id='1..1000')
                        WHERE id BETWEEN 1 AND 10000';

                        where(id='1..1000', city='Berlin..London')
                        WHERE id BETWEEN 1 AND 10000 AND city BETWEEN Berlin AND London;

                        where(op='or, id='1..1000', city='berlin..london')
                        WHERE id BETWEEN 1 AND 10000 OR city BETWEEN Berlin AND London;
            """
            return self.Selector(columns)

        @ BoundInnerClass
        class Selector:

            def __init__(self, outer, columns):
                self._name = outer._name
                self._base = outer._base
                self.columns = ', '.join(columns) if columns else '*'

            def __repr__(self):
                return f"{self._base}.{self._name}.{type(self).__name__}({self.columns})"

            def all(self, sort=None, limit=None):
                """select all results from the selection object

                   ARGUMENTS:
                        sort:  str: sort the results
                        limit: str: limit results to a specifc number

                   USAGE:
                        selection = Table.select('people')
                        results = selection.all()
                        results = selection.all(sort='people desc', limit=10)
                """
                limit = f"LIMIT {limit}" if limit else ''
                order = f"ORDER BY {sort}" if sort else ''
                query = f"SELECT {self.columns} FROM {self._name} {order} {limit}"
                try:
                    self.kursor.execute(query.strip())
                    return self.kursor.fetchall()

                except SQLError as error:
                    echo.alert(error)

            def expand(self, key, value):
                """check and process expansion syntax"""
                result = expansions.match(value).group(0)
                return expander[expansion_operators.search(result).group(0)](key, value)

            def where(self, condition=None, op='and', sort=None, limit=None, **kwargs):
                """filter the Table.selection results

                   ARGUMENTS:
                        condition:  str: an explicit sql statement;
                                         passing condition overides all
                                         other options.

                        op:         str: logical operator applied between
                                         compound statements. defaults to AND

                                         examples:
                                         where(name='AL', city='LA')
                                         WHERE name = Al AND city = LA

                                         where(op='or', name='AL', city='LA')
                                         WHERE name = Al OR city = LA

                        sort:       str: sort the results
                        limit:      str: limit results to a specifc number
                        kwargs:     str: conditions as key-value pairs

                    USAGE:
                        where(people='tom', city='London or Moscow')
                        WHERE people EQUALS tom AND city EQUALS London OR city EQUALS Moscow;

                        where(operator='or', people='tom', city='london or moscow')
                        WHERE people EQUALS tom OR city EQUALS London OR city EQUALS Moscow;

                        where(id='1..1000')
                        WHERE id BETWEEN 1 AND 10000';

                        where(id='1..1000', city='Berlin..London')
                        WHERE id BETWEEN 1 AND 10000 AND city BETWEEN Berlin AND London;

                        where(op='or, id='1..1000', city='berlin..london')
                        WHERE id BETWEEN 1 AND 10000 OR city BETWEEN Berlin AND London;

                        pre-format where clauses
                        clause_1 = {'city': 'Berlin', 'sort': 'city', 'limit': 10}
                        clause_2 = {'logic': 'or', 'people': 'Al or Bob', 'city': 'Berlin..London'}

                        db.table.select('people').where(**clause_1)
                        db.table.select('people').where(**clause_2)
                """
                if condition:
                    query = f"SELECT {self.columns} FROM {self._name} WHERE {condition}"
                    self.kursor.execute(query.strip())
                    return self.kursor.fetchall()

                limit = f"LIMIT {limit}" if limit else ''
                order = f"ORDER BY {sort}" if sort else ''
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
                    statement = ' OR '.join(statements)
                    conditions.append(statement)
                    statements.clear()

                chain = f' {op} '.join(conditions)
                query = f"SELECT {self.columns} FROM {self._name} WHERE {chain} {order} {limit}"
                try:
                    self.kursor.execute(query.strip())
                    return self.kursor.fetchall()

                except SQLError as error:
                    echo.alert(error)
