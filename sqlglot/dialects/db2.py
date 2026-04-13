from __future__ import annotations

import typing as t

from sqlglot import tokens
from sqlglot.dialects.dialect import Dialect
from sqlglot.generators.db2 import Db2 as Db2Generator
from sqlglot.parsers.db2 import Db2Parser
from sqlglot.tokens import TokenType

if t.TYPE_CHECKING:
    pass


class Db2(Dialect):
    NULL_ORDERING = "nulls_are_large"
    TYPED_DIVISION = True

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
