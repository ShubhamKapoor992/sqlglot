from __future__ import annotations

import typing as t

from sqlglot import tokens
from sqlglot.dialects.dialect import Dialect, NormalizationStrategy
from sqlglot.generators.db2 import Db2 as Db2Generator
from sqlglot.parsers.db2 import Db2Parser
from sqlglot.tokens import TokenType

if t.TYPE_CHECKING:
    pass


class Db2(Dialect):
    NULL_ORDERING = "nulls_are_large"
    TYPED_DIVISION = True

    # Time format mappings for DB2
    # https://www.ibm.com/docs/en/db2/11.5?topic=functions-timestamp-format
    TIME_MAPPING = {
        "YYYY": "%Y",
        "YY": "%y",
        "MM": "%m",
        "DD": "%d",
        "HH": "%H",
        "HH12": "%I",
        "HH24": "%H",
        "MI": "%M",
        "SS": "%S",
        "FF": "%f",
        "FF3": "%f",
        "FF6": "%f",
        "MON": "%b",
        "MONTH": "%B",
        "DY": "%a",
        "DAY": "%A",
    }

    class Tokenizer(tokens.Tokenizer):
        KEYWORDS = {
            **tokens.Tokenizer.KEYWORDS,
            "DBCLOB": TokenType.DBCLOB,
            "GRAPHIC": TokenType.GRAPHIC,
            "VARGRAPHIC": TokenType.VARGRAPHIC,
        }

        VAR_SINGLE_TOKENS = {"@"}

    Parser = Db2Parser
    Generator = Db2Generator
