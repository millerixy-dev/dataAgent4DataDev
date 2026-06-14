"""SQL statement splitter — preserves quoted strings, backticks, comments."""

from __future__ import annotations


def has_sql_content(sql_text: str) -> bool:
    """True when ``sql_text`` contains tokens beyond whitespace, semicolons and comments."""

    index = 0
    length = len(sql_text)

    in_line_comment = False
    in_block_comment = False

    while index < length:
        char = sql_text[index]
        next_char = sql_text[index + 1] if index + 1 < length else ""

        if in_line_comment:
            if char == "\n":
                in_line_comment = False
            index += 1
            continue

        if in_block_comment:
            if char == "*" and next_char == "/":
                in_block_comment = False
                index += 2
                continue
            index += 1
            continue

        if char == "-" and next_char == "-":
            in_line_comment = True
            index += 2
            continue

        if char == "/" and next_char == "*":
            in_block_comment = True
            index += 2
            continue

        if not char.isspace() and char != ";":
            return True

        index += 1

    return False


def split_sql_statements(sql_text: str) -> list[str]:
    """Split a script into individual SQL statements.

    The splitter ignores semicolons inside single quotes, double quotes,
    backticks, line comments and block comments. Whitespace-only or
    comment-only fragments are dropped.
    """

    statements: list[str] = []
    current: list[str] = []

    in_single_quote = False
    in_double_quote = False
    in_backtick = False
    in_line_comment = False
    in_block_comment = False

    index = 0
    length = len(sql_text)

    while index < length:
        char = sql_text[index]
        next_char = sql_text[index + 1] if index + 1 < length else ""

        if in_line_comment:
            current.append(char)
            if char == "\n":
                in_line_comment = False
            index += 1
            continue

        if in_block_comment:
            current.append(char)
            if char == "*" and next_char == "/":
                current.append(next_char)
                in_block_comment = False
                index += 2
                continue
            index += 1
            continue

        if in_single_quote:
            current.append(char)
            if char == "\\" and next_char:
                current.append(next_char)
                index += 2
                continue
            if char == "'" and next_char == "'":
                current.append(next_char)
                index += 2
                continue
            if char == "'":
                in_single_quote = False
            index += 1
            continue

        if in_double_quote:
            current.append(char)
            if char == "\\" and next_char:
                current.append(next_char)
                index += 2
                continue
            if char == '"' and next_char == '"':
                current.append(next_char)
                index += 2
                continue
            if char == '"':
                in_double_quote = False
            index += 1
            continue

        if in_backtick:
            current.append(char)
            if char == "`" and next_char == "`":
                current.append(next_char)
                index += 2
                continue
            if char == "`":
                in_backtick = False
            index += 1
            continue

        if char == "-" and next_char == "-":
            current.append(char)
            current.append(next_char)
            in_line_comment = True
            index += 2
            continue

        if char == "/" and next_char == "*":
            current.append(char)
            current.append(next_char)
            in_block_comment = True
            index += 2
            continue

        if char == "'":
            current.append(char)
            in_single_quote = True
            index += 1
            continue

        if char == '"':
            current.append(char)
            in_double_quote = True
            index += 1
            continue

        if char == "`":
            current.append(char)
            in_backtick = True
            index += 1
            continue

        if char == ";":
            statement = "".join(current).strip()
            if statement and has_sql_content(statement):
                statements.append(statement)
            current = []
            index += 1
            continue

        current.append(char)
        index += 1

    if in_single_quote:
        raise ValueError("SQL 脚本存在未闭合的单引号字符串")
    if in_double_quote:
        raise ValueError("SQL 脚本存在未闭合的双引号字符串")
    if in_backtick:
        raise ValueError("SQL 脚本存在未闭合的反引号标识符")
    if in_block_comment:
        raise ValueError("SQL 脚本存在未闭合的块注释")

    remaining_statement = "".join(current).strip()
    if remaining_statement and has_sql_content(remaining_statement):
        statements.append(remaining_statement)

    return statements
