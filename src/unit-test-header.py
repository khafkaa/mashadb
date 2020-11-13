from functools import partial
from src.mashadb import MashaDB

masha = partial(MashaDB, user='kafka', password='animistic', host='localhost')
