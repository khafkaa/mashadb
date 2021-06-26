from dataclasses import dataclass


@dataclass
class Column:
    datatype: str
    null: bool = False
    unique: bool = False

    @property
    def column(self):
        null = '' if self.null is True else 'NOT NULL'
        unique = '' if self.unique is False else 'UNIQUE'
        return f'{self.datatype} {null} {unique}'.strip()


@dataclass
class Primary:
    init: int = 0

    @property
    def key(self):
        init = f'AUTO_INCREMENT={self.init}' if self.init else 'AUTO_INCREMENT'
        return f'INT NOT NULL {init}, PRIMARY KEY'
