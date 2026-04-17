"""AMQP Table Encoding/Decoding"""
import struct
import decimal
import calendar

from datetime import datetime, timezone

from pika import exceptions
from pika.compat import long, as_bytes


def encode_short_string(pieces, value):
    """Encode a string value as short string and append it to pieces list
    returning the size of the encoded value.

    :param list pieces: Already encoded values
    :param str value: String value to encode
    :rtype: int

    """
    encoded_value = as_bytes(value)
    length = len(encoded_value)

    # 4.2.5.3
    # Short strings, stored as an 8-bit unsigned integer length followed by zero
    # or more octets of data. Short strings can carry up to 255 octets of UTF-8
    # data, but may not contain binary zero octets.
    # ...
    # 4.2.5.5
    # The server SHOULD validate field names and upon receiving an invalid field
    # name, it SHOULD signal a connection exception with reply code 503 (syntax
    # error).
    # -> validate length (avoid truncated utf-8 / corrupted data), but skip null
    # byte check.
    if length > 255:
        raise exceptions.ShortStringTooLong(encoded_value)

    pieces.append(struct.pack('B', length))
    pieces.append(encoded_value)
    return 1 + length


def decode_short_string(encoded, offset):
    """Decode a short string value from ``encoded`` data at ``offset``.
    """
    pass


def encode_table(pieces, table):
    """Encode a dict as an AMQP table appending the encded table to the
    pieces list passed in.

    :param list pieces: Already encoded frame pieces
    :param dict table: The dict to encode
    :rtype: int

    """
    table = table or {}
    length_index = len(pieces)
    pieces.append(None)  # placeholder
    tablesize = 0
    for (key, value) in table.items():
        tablesize += encode_short_string(pieces, key)
        tablesize += encode_value(pieces, value)

    pieces[length_index] = struct.pack('>I', tablesize)
    return tablesize + 4


def encode_value(pieces, value): # pylint: disable=R0911
    """Encode the value passed in and append it to the pieces list returning
    the the size of the encoded value.

    :param list pieces: Already encoded values
    :param any value: The value to encode
    :rtype: int

    """

    if isinstance(value, str):
        value = as_bytes(value)
        pieces.append(struct.pack('>cI', b'S', len(value)))
        pieces.append(value)
        return 5 + len(value)
    elif isinstance(value, bytes):
        pieces.append(struct.pack('>cI', b'x', len(value)))
        pieces.append(value)
        return 5 + len(value)
    elif isinstance(value, bool):
        pieces.append(struct.pack('>cB', b't', int(value)))
        return 2
    elif isinstance(value, long):
        if value < 0:
            pieces.append(struct.pack('>cq', b'L', value))
        else:
            pieces.append(struct.pack('>cQ', b'l', value))
        return 9
    elif isinstance(value, int):
        try:
            packed = struct.pack('>ci', b'I', value)
            pieces.append(packed)
            return 5
        except struct.error:
            if value < 0:
                packed = struct.pack('>cq', b'L', long(value))
            else:
                packed = struct.pack('>cQ', b'l', long(value))
            pieces.append(packed)
            return 9
    elif isinstance(value, decimal.Decimal):
        value = value.normalize()
        if value.as_tuple().exponent < 0:
            decimals = -value.as_tuple().exponent
            raw = int(value * (decimal.Decimal(10)**decimals))
            pieces.append(struct.pack('>cBi', b'D', decimals, raw))
        else:
            # per spec, the "decimals" octet is unsigned (!)
            pieces.append(struct.pack('>cBi', b'D', 0, int(value)))
        return 6
    elif isinstance(value, datetime):
        pieces.append(
            struct.pack('>cQ', b'T', calendar.timegm(value.utctimetuple())))
        return 9
    elif isinstance(value, dict):
        pieces.append(struct.pack('>c', b'F'))
        return 1 + encode_table(pieces, value)
    elif isinstance(value, list):
        list_pieces = []
        for val in value:
            encode_value(list_pieces, val)
        piece = b''.join(list_pieces)
        pieces.append(struct.pack('>cI', b'A', len(piece)))
        pieces.append(piece)
        return 5 + len(piece)
    elif value is None:
        pieces.append(struct.pack('>c', b'V'))
        return 1
    else:
        raise exceptions.UnsupportedAMQPFieldException(pieces, value)


def decode_table(encoded, offset):
    """Decode the AMQP table passed in from the encoded value returning the
    decoded result and the number of bytes read plus the offset.

    :param str encoded: The binary encoded data to decode
    :param int offset: The starting byte offset
    :rtype: tuple

    """
    pass


def decode_value(encoded, offset): # pylint: disable=R0912,R0915
    """Decode the value passed in returning the decoded value and the number
    of bytes read in addition to the starting offset.

    :param str encoded: The binary encoded data to decode
    :param int offset: The starting byte offset
    :rtype: tuple
    :raises: pika.exceptions.InvalidFieldTypeException

    """
    pass
