"""MashaDB syntax expansion utilities
"""
import re
from iter.accessories import multisub

logic = re.compile(r'\sor\s', re.IGNORECASE)
expansion_operators = re.compile(r'(\+|-|%|\.\.)')
expansions = re.compile(r'(^%.+|.+%$|^.+%.+$|.*?\.\..*|^[+-].+)')


def expComp(key, value):
    """Exapand Comparison Operators
    """
    return f"{key}{multisub({'+': ' >= ', '-': ' <= '}, value)}"


def expRange(key, value):
    """Expand a range of values to MySQL BETWEEN statement
    """
    low, high = value.split('..')
    return f"{key} BETWEEN '{low}' AND '{high}'"


def expLike(key, value):
    """Expand SQL wildcard characters to LIKE statement
    """
    return f"{key} LIKE '{value}'"


expander = {'+': expComp, '-': expComp, '%': expLike, '..': expRange}
