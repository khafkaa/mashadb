"""SQL Data Type Function Factory:
"""
DATATYPES = (
    'CHAR(size)', 'VARCHAR(size)', 'TINYTEXT(size)', 'TEXT(size)',
    'MEDIUMTEXT(size)', 'LONGTEXT(size)', 'BINARY(size)', 'VARBINARY(size)',
    'BIT', 'TINYINT(m)', 'SMALLINT(m)', 'MEDIUMINT(m)', 'INT(m)', 'INTEGER(m)',
    'BIGINT(m)', 'DECIMAL(m,d)', 'DEC(m,d)', 'NUMERIC(m,d)', 'FIXED(m,d)',
    'FLOAT(m,d)', 'DOUBLE(m,d)', 'DOUBLE PRECISION(m,d)', 'REAL(m,d)', 'FLOAT(p)',
    'BOOL', 'BOOLEAN', 'DATE', 'DATETIME', 'TIMESTAMP', 'TIME', 'YEAR[2|4)',
    'TINYBLOB', 'BLOB(size)', 'MEDIUMBLOB', 'LONGTEXT')


def primary(datatype='INT', init=0):
    initval = f'AUTO_INCREMENT={init}' if init else 'AUTO_INCREMENT'
    return f"{datatype} NOT NULL {initval}, PRIMARY KEY"


def char(size, unique=False, null=False):
    unique = '' if unique is False else 'UNIQUE'
    null = '' if null is True else 'NOT NULL'
    return f"CHAR({size}) {null} {unique}".strip()


def varchar(size, unique=False, null=False):
    unique = '' if unique is False else 'UNIQUE'
    null = '' if null is True else 'NOT NULL'
    return f"VARCHAR({size}) {null} {unique}".strip()


def boolean(default=0):
    default = 1 if default >= 1 else 0
    return f"BOOLEAN NOT NULL DEFAULT {default}"


def text(size):
    pass


def bin(size):
    return f"BINARY(size)"


def varbin(size):
    return f"VARBINARY(size)"


def date():
    pass


def time():
    pass
