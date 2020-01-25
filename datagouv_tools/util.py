#  DataGouv Tools. An utility to import some data from data.gouv.fr to
#                  PostgreSQL and other DBMS.
#       Copyright (C) 2020 J. Férard <https://github.com/jferard>
#
#   This file is part of DataGouv Tools.
#
#  DataGouv Tools is free software: you can redistribute it and/or modify it
#  under the terms of the GNU General Public License as published by the Free
#  Software Foundation, either version 3 of the License, or (at your option) any
#  later version.
#
#  DataGouv Tools is distributed in the hope that it will be useful, but
#  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
#  or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
#  more details.
#  You should have received a copy of the GNU General Public License along with
#  this program. If not, see <http://www.gnu.org/licenses/>.
from typing import Optional, Tuple, Iterable


def split_on_cat(text: str,
                 dont_split: Optional[
                     Iterable[Tuple[Optional[str], Optional[str]]]] = None
                 ) -> Iterable[str]:
    """
    Split the text on unicodedata differences. The default behavior ignores
    transitions from upper case to lower case:

    >>> list(split_on_cat("LoremIpsum"))
    ['Lorem', 'Ipsum']

    You can specifiy transisions to ignore in unicode categories. No transition
    ignored:
    >>> list(split_on_cat("LoremIpsum", ()))
    ['L', 'orem', 'I', 'psum']

    Ignore all transitions before a number:
    >>> list(split_on_cat("Lorem2Ipsum", ((None, "Nd"),)))
    ['L', 'orem2', 'I', 'psum']

    Ignore all transitions:
    >>> list(split_on_cat("LoremIpsum", ((None, None),)))
    ['LoremIpsum']

    :param text: the text
    :param dont_split: transitions (cat1, cat2) that are not a valid split
    :return: yield chunks of text
    """
    import unicodedata
    if dont_split is None:
        dont_split = (("Lu", "Ll"), (None, "Pc"), ("Pc", None))

    def split_between(lc, c):
        for ds_lc, ds_c in dont_split:
            if ((ds_lc is None or lc == ds_lc)
                    and (ds_c is None or c == ds_c)):
                return False

        return True

    previous_end = None
    last_cat = unicodedata.category(text[0])
    for i, cat in enumerate(map(unicodedata.category, text)):
        if cat != last_cat and split_between(last_cat, cat):
            yield text[previous_end:i]
            previous_end = i
        last_cat = cat
    yield text[previous_end:None]


def to_snake(text: str) -> str:
    """
    Converts a camel case text to snake case.

    >>> to_snake("LoremIpsum")
    'lorem_ipsum'
    >>> to_snake("Lorem2Ipsum")
    'lorem_2_ipsum'

    `to_snake` is idempotent:

    >>> to_snake(to_snake("LoremIpsum"))
    'lorem_ipsum'

    :param text: the camel case text
    :return: the snake case text
    """
    return '_'.join(split_on_cat(text)).lower()


if __name__ == "__main__":
    import doctest
    doctest.testmod()
