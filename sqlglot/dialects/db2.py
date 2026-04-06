from __future__ import annotations

import typing as t

from sqlglot import exp, generator, tokens, transforms
from sqlglot.dialects.dialect import (
    Dialect,
    NormalizationStrategy,
    max_or_greatest,
    min_or_least,
    rename_func,
    trim_sql,
    no_ilike_sql,
    no_pivot_sql,
    no_trycast_sql,
)
from sqlglot.parsers.db2 import Db2Parser
from sqlglot.tokens import TokenType

if t.TYPE_CHECKING:
    pass


def _date_add_sql(
    kind: str,
) -> t.Callable[[Db2.Generator, exp.DateAdd | exp.DateSub], str]:
    def func(self: Db2.Generator, expression: exp.DateAdd | exp.DateSub) -> str:
        this = self.sql(expression, "this")
        unit = expression.args.get("unit")
        value = self._simplify_unless_literal(expression.expression)

        if not isinstance(value, exp.Literal):
            self.unsupported("Cannot add non literal")

        value_sql = self.sql(value)
        unit_sql = self.sql(unit) if unit else "DAY"

        return f"{this} {kind} {value_sql} {unit_sql}"

    return func


class Db2(Dialect):
    # DB2 is case-insensitive by default for unquoted identifiers
    NORMALIZATION_STRATEGY = NormalizationStrategy.UPPERCASE

    # DB2 supports NULL ordering
    NULL_ORDERING = "nulls_are_large"

    # DB2 specific settings
    TYPED_DIVISION = True
    SAFE_DIVISION = True

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
        # DB2 uses @ for variables
        VAR_SINGLE_TOKENS = {"@"}

        # DB2 specific keywords
        KEYWORDS = {
            **tokens.Tokenizer.KEYWORDS,
            "CHAR": TokenType.CHAR,
            "CLOB": TokenType.TEXT,
            "DBCLOB": TokenType.TEXT,
            "DECFLOAT": TokenType.DECIMAL,
            "GRAPHIC": TokenType.NCHAR,
            "VARGRAPHIC": TokenType.NVARCHAR,
            "SMALLINT": TokenType.SMALLINT,
            "INTEGER": TokenType.INT,
            "BIGINT": TokenType.BIGINT,
            "REAL": TokenType.FLOAT,
            "DOUBLE": TokenType.DOUBLE,
            "DECIMAL": TokenType.DECIMAL,
            "NUMERIC": TokenType.DECIMAL,
            "VARCHAR": TokenType.VARCHAR,
            "TIMESTAMP": TokenType.TIMESTAMP,
            "TIMESTMP": TokenType.TIMESTAMP,
            "SYSIBM": TokenType.SCHEMA,
            "SYSFUN": TokenType.SCHEMA,
            "SYSTOOLS": TokenType.SCHEMA,
        }

    Parser = Db2Parser

    class Generator(generator.Generator):
        LIMIT_FETCH = "FETCH"
        JOIN_HINTS = False
        TABLE_HINTS = False
        QUERY_HINTS = False
        NVL2_SUPPORTED = False
        LAST_DAY_SUPPORTS_DATE_PART = False

        # DB2 uses CONCAT operator
        CONCAT_COALESCE = True

        TYPE_MAPPING = {
            **generator.Generator.TYPE_MAPPING,
            exp.DataType.Type.BOOLEAN: "SMALLINT",
            exp.DataType.Type.TINYINT: "SMALLINT",
            exp.DataType.Type.BINARY: "BLOB",
            exp.DataType.Type.VARBINARY: "BLOB",
            exp.DataType.Type.TEXT: "CLOB",
            exp.DataType.Type.NCHAR: "GRAPHIC",
            exp.DataType.Type.NVARCHAR: "VARGRAPHIC",
            exp.DataType.Type.TIMESTAMPTZ: "TIMESTAMP",
            exp.DataType.Type.DATETIME: "TIMESTAMP",
        }

        TRANSFORMS = {
            **generator.Generator.TRANSFORMS,
            exp.ArgMax: rename_func("MAX"),
            exp.ArgMin: rename_func("MIN"),
            exp.DateAdd: _date_add_sql("+"),
            exp.DateSub: _date_add_sql("-"),
            exp.DateDiff: lambda self,
            e: f"{self.func('DAYS', e.this)} - {self.func('DAYS', e.expression)}",
            exp.CurrentDate: lambda self, e: "CURRENT DATE",
            exp.CurrentTimestamp: lambda self, e: "CURRENT TIMESTAMP",
            exp.ILike: no_ilike_sql,
            exp.Max: max_or_greatest,
            exp.Min: min_or_least,
            exp.Pivot: no_pivot_sql,
            exp.Select: transforms.preprocess([transforms.eliminate_distinct_on]),
            exp.StrPosition: rename_func("POSSTR"),
            exp.TimeToStr: rename_func("VARCHAR_FORMAT"),
            exp.TryCast: no_trycast_sql,
            exp.Trim: trim_sql,
        }

        def extract_sql(self, expression: exp.Extract) -> str:
            this = self.sql(expression, "this")
            expression_sql = self.sql(expression, "expression")

            if this.upper() in ("DAYOFWEEK", "DAYOFYEAR"):
                return f"{this.upper()}({expression_sql})"

            return f"EXTRACT({this} FROM {expression_sql})"

        def offset_sql(self, expression: exp.Offset) -> str:
            return f"{super().offset_sql(expression)} ROWS"

        def fetch_sql(self, expression: exp.Fetch) -> str:
            count = expression.args.get("count")
            if count:
                return f" FETCH FIRST {self.sql(count)} ROWS ONLY"
            return " FETCH FIRST ROW ONLY"

        def boolean_sql(self, expression: exp.Boolean) -> str:
            return "1" if expression.this else "0"
