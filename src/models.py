from dataclasses import dataclass

@dataclass
class Column:
    name: str
    datatype: str
    null: bool=False
    unique: bool=False

    @property
    def column(self):
        null = '' if self.null is True else 'NOT NULL'
        unique = '' if self.unique is False else 'UNIQUE'
        return f'{self.name} {self.datatype} {null} {unique}'.strip()


@dataclass
class Primary:
    name: str
    init: int=0

    @property
    def column(self):
        init = f'AUTO INCREMENT={self.init}' if self.init else 'AUTO INCREMENT'
        return f'{self.name} INT  NOT NULL {init}, PRIMARY KEY'

