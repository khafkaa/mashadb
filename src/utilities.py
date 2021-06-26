"""assorted auxillary functions"""
import re
from functools import partial
from collections import namedtuple

termcolors = {
    "term": "[0m",
    "red": "[31m",
    "green": "[32m",
    "orange": "[33m",
    "blue": "[34m",
    "magenta": "[35m",
    "cyan": "[36m",
    "info": "[37m",
    "grey": "[90m",
    "alert": "[91m",
    "success": "[92m",
    "yellow": "[93m",
    "purple": "[94m",
    "pink": "[95m",
    "aqua": "[96m"
}


def cprint(text, color='term', newline=True):
    """color print with line termination options"""
    terminate = '\n' if newline is True else ''
    print(f"\x1b{termcolors[color]}{text}\x1b{termcolors['term']}", end=terminate)


cprinter = namedtuple('cprint', termcolors.keys())
colors = [partial(cprint, color=key) for key in termcolors.keys()]

# ECHO COMMAND LINE COLOR PRINTING
echo = cprinter(*colors)


def multisub(characters, string):
    """Performs several string substitutions on a single pass using
       a dictionary to provide key-replacement_value pairs.

       USAGE:
           multisub({'#': '$', 'Thomas': 'Hank'}, text_to_be_processed)

           The dict key is the character to be replaced;
           The dict value is the replacement character.

       ARGUMENTS:
           characters: dict: The target characters and their
                             substitution values as key-value
                             pairs.

           string: str: The target string upon which the subs will
                       be performed.
    """
    regex = re.compile("|".join(map(re.escape, characters.keys())))
    return regex.sub(lambda match: characters[match.group(0)], string)


# Syntax Expansion Utilities
logic = re.compile(r'\sor\s', re.IGNORECASE)
expansion_operators = re.compile(r'(\+|-|%|\.\.)')
expansions = re.compile(r'(^%.+|.+%$|^.+%.+$|.*?\.\..*|^[+-].+)')


def expComp(key, value):
    """Exapand Comparison Operators"""
    return f"{key}{multisub({'+': ' >= ', '-': ' <= '}, value)}"


def expRange(key, value):
    """Expand a range of values to MySQL BETWEEN statement"""
    low, high = value.split('..')
    return f"{key} BETWEEN '{low}' AND '{high}'"


def expLike(key, value):
    """Expand SQL wildcard characters to LIKE statement"""
    return f"{key} LIKE '{value}'"


expander = {'+': expComp, '-': expComp, '%': expLike, '..': expRange}
