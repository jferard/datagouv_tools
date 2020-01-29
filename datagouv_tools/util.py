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
import codecs
from io import BytesIO
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


def sanitize(text: str) -> str:
    """
    Remove accents and special chars from a string

    >>> sanitize("Code Départ’ement")
    'Code Departement'


    @param text: the unicode string
    @return: the ascii string
    """
    import unicodedata
    try:
        text = unicodedata.normalize('NFKD', text).encode('ascii',
                                                          'ignore').decode(
            'ascii')
    except UnicodeError:
        pass
    return text


def to_standard(text: str) -> str:
    """
    >>> to_standard("Code Départ’ement")
    'code_departement'

    :param text:
    :return:
    """
    return sanitize(text.replace(" ", "_")).casefold()


class CSVStream(BytesIO):
    """
    A stream.
    """

    def __init__(self, name: str, header, queue,
                 encode=codecs.getencoder("ascii")):
        super().__init__()
        self._name = name
        self._queue = queue
        self._encode = encode
        self._header = header
        self._remaining_bytes = self._encode("\t".join(header) + "\n")[0]
        self._queue_ended = False

    def send(self, data):
        self._queue.put(data)

    def read(self, size=-1):
        if size < 0 or size >= 8192:
            b = bytearray(8192)
            n = self.readinto(b)
            return b[:n]
        else:
            b = bytearray(size)
            n = self.readinto(b)
            return b[:n]

    def readinto(self, b):
        i = len(self._remaining_bytes)
        len_b = len(b)
        if len_b <= i:
            b[:] = self._remaining_bytes[:len_b]
            self._remaining_bytes = self._remaining_bytes[len_b:]
            return len_b

        # len_b > i
        b[:i] = self._remaining_bytes
        self._remaining_bytes = b''
        if self._queue_ended:
            return i

        while i < len_b:
            csv_line = self._queue.get()
            if csv_line is None:
                self._queue_ended = True
                return i

            data = self._encode(csv_line)[0]
            next_i = i + len(data)
            if next_i >= len_b:
                room_b = len_b - i
                b[i:] = data[:room_b]
                self._remaining_bytes = data[room_b:]
                return len_b
            else:
                b[i:next_i] = data

            i = next_i

    def close(self):
        pass


if __name__ == "__main__":
    import doctest

    doctest.testmod()
