# -*- coding: utf-8 -*-
#
# Copyright 2018 Marcus Klang (marcus.klang@cs.lth.se)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from typing import List, Dict, Tuple, Optional
from html import escape


class PrintOptions:
    def __init__(self):
        """The maximum number of nodes to output, -1 for infinite"""

        self._max_rows = 100
        self._max_columns = 8
        self._max_column_width = 30
        self._max_display_width = 120

    @property
    def max_rows(self):
        return self._max_rows

    @max_rows.setter
    def max_rows(self, max_rows):
        """Max number of rows, will truncate table if larger."""
        if max_rows is None:
            self._max_rows = None
        elif max_rows <= 0:
            raise ValueError("Max rows must be None for infinite or >= 1")
        else:
            self._max_rows = int(max_rows)

    @property
    def max_columns(self):
        return self._max_rows

    @max_columns.setter
    def max_columns(self, max_columns):
        """Max number of columns to display"""
        if max_columns is None:
            self._max_columns = None
        elif max_columns <= 0:
            raise ValueError("Max columns must be None for infinite or >= 1")
        else:
            self._max_columns = int(max_columns)

    @property
    def max_column_width(self):
        return self._max_column_width

    @max_column_width.setter
    def max_column_width(self, max_column_width):
        """Max column width, the maximum number of characters to show inside a column, will truncate if larger."""
        if max_column_width is None:
            self._max_column_width = None
        elif max_column_width <= 6:
            raise ValueError("Max column width must be None for infinite or >= 7")
        else:
            self._max_column_width = int(max_column_width)

    @property
    def max_display_width(self):
        return self._max_display_width

    @max_display_width.setter
    def max_display_width(self, max_display_width):
        """Max display width, the maximum number of characters in width for a
           full table, columns will be wrapped if longer."""
        if max_display_width is None:
            self._max_display_width = None
        elif max_display_width <= 10:
            raise ValueError("Max column width must be None for infinite or >= 10")
        else:
            self._max_display_width = int(max_display_width)


options = PrintOptions()


class TableStyle:
    def __init__(self, padding=2):
        self.padding = padding


def truncate(text):
    if options.max_column_width is None:
        return text
    else:
        min_sz = min(options._max_display_width-5, options.max_column_width-5)
        if len(text) > min_sz:
            part = min_sz >> 1
            return "%s ... %s" % (text[0:part], text[-part:])
        else:
            return text


class TableRow:
    def __init__(self, *elems, index=None):
        """
        :param elems: The content of the row
        :param index: The index name of this row, is used by table to change id to a named index
        """
        self.index = index
        self.elems = list(elems)

    def num_columns(self):
        return len(self.elems)


class TableCell:
    def __init__(self, text=None, html=None):
        self.text = text
        self.html = html


class Table:
    """Table representation for text and HTML"""
    def __init__(self, caption: Optional[str]=None, style=TableStyle(), hide_index=False, hide_headers=False):
        self.caption = caption
        self.style = style  # type: TableStyle
        self.header = None  # type: TableRow
        self.body = []  # type: List[TableRow]
        self.hide_header = hide_headers
        self.hide_index = hide_index

    def set_header(self, *row):
        if len(row) == 1 and isinstance(row[0], TableRow):
            self.header = row[0]
            row[0].index = "#"
        else:
            self.header = TableRow(*row, index="#")

    def add_body(self, *row):
        if len(row) == 1 and isinstance(row[0], TableRow):
            self.body.append(row[0])
        else:
            self.body.append(TableRow(*row))

    def set_footer(self, *row):
        if len(row) == 1 and isinstance(row[0], TableRow):
            self.footer = row[0]
        else:
            self.footer = TableRow(*row)

    def format_text(self, row: TableRow, index: int):
        output = [str(row.index) if row.index is not None else str(index)]
        for col in row.elems:
            if isinstance(col, TableCell):
                output.append(truncate(col.text))
            else:
                output.append(truncate(str(col)))

        return output

    def format_html(self, row: TableRow, index: int):
        output = [str(row.index) if row.index is not None else str(index)]
        for col in row.elems:
            if isinstance(col, TableCell):
                output.append(col.html)
            else:
                output.append(escape(truncate(str(col))))

        return output

    def _compile_text(self):
        headers = self.format_text(self.header, 0)

        if options.max_rows is not None and len(self.body) > options.max_rows:
            body = []
            part = options.max_rows >> 1
            upper_part = zip(range(0, part), self.body[0:part])
            lower_part = zip(range(len(self.body)-part, len(self.body)), self.body[-part:])

            body.extend(list(map(lambda tup: self.format_text(row=tup[1], index=tup[0]), upper_part)))
            body.append(None)
            body.extend(list(map(lambda tup: self.format_text(row=tup[1], index=tup[0]), lower_part)))
        else:
            body = list(map(lambda tup: self.format_text(row=tup[1], index=tup[0]), zip(range(len(self.body)), self.body)))

        return headers, body

    def _compile_html(self):
        headers = self.format_html(self.header, 0)

        if options.max_rows is not None and len(self.body) > options.max_rows:
            body = []
            part = options.max_rows >> 1
            upper_part = zip(range(0, part), self.body[0:part])
            lower_part = zip(range(len(self.body)-part, len(self.body)), self.body[-part:])

            body.extend(list(map(lambda tup: self.format_html(row=tup[1], index=tup[0]), upper_part)))
            body.append(None)
            body.extend(list(map(lambda tup: self.format_html(row=tup[1], index=tup[0]), lower_part)))
        else:
            body = list(map(lambda tup: self.format_html(tup[1], tup[0]), zip(range(len(self.body)), self.body)))

        return headers, body

    def _get_column_format(self, width):
        return "%s%s%s" % (" " * self.style.padding, "{:<%d}" % width, " " * self.style.padding)

    def render_text(self):
        output = []
        headers, rows = self._compile_text()

        col_widths = list(map(len, headers))
        row_widths = [max(map(len, map(lambda row: row[i], filter(lambda x: x is not None, rows))), default=0) for i in range(len(headers))]

        if self.hide_header:
            actual_widths = row_widths
        else:
            actual_widths = list(map(max, zip(col_widths, row_widths)))

        column_pos = 1

        while column_pos < len(headers):
            column_width = 0
            if not self.hide_index:
                column_width += actual_widths[0] + self.style.padding*2

            for i in range(column_pos, len(headers)):
                column_width += actual_widths[i] + self.style.padding*2
                column_end = i+1

                if options.max_display_width is not None and column_width > options.max_display_width:
                    break

            if column_pos == 1 and self.caption is not None:
                # Print caption
                output.append(str.format("{:^%d}" % column_width, truncate(self.caption)))
                output.append("")
            elif column_pos > 1:
                output.append("")
                output.append("")

            # Print header
            if self.hide_index:
                real_header = headers[column_pos:column_end]
                line_format = "".join([self._get_column_format(actual_widths[k]) for k in range(column_pos, column_end)])
            else:
                real_header = [headers[0]] + headers[column_pos:column_end]
                line_format = "".join([self._get_column_format(actual_widths[0])]
                                 + [self._get_column_format(actual_widths[k]) for k in range(column_pos, column_end)])

            if not self.hide_header:
                output.append(str.format(line_format, *real_header) + (" \\" if column_end != len(headers) else ""))
                output.append("")

            # Print body
            if self.hide_index:
                for row in rows:
                    if row is None:
                        output.append("")
                        output.append(str.format("{:^%d}" % column_width, "..."))
                        output.append("")
                    else:
                        output.append(str.format(line_format, *row[column_pos:column_end]))
            else:
                for row in rows:
                    if row is None:
                        output.append("")
                        output.append(str.format("{:^%d}" % column_width, "..."))
                        output.append("")
                    else:
                        real_row = [row[0]]
                        real_row.extend(row[column_pos:column_end])

                        output.append(str.format(line_format, *real_row))

            column_pos = column_end

        output.append("")
        return "\n".join(output)

    def render_html(self):
        pass


def get_representation(value):
    from docria.model import Node

    if value is None:
        return "NIL"
    elif isinstance(value, list):
        if len(value) == 1:
            return "[%s]" % repr(value[0])
        elif len(value) > 0:
            all_nodes = True
            nodetypes = set()

            for elem in value:
                if isinstance(elem, Node):
                    nodetypes.add(elem.collection.name)
                else:
                    all_nodes = False
                    break

            if all_nodes:
                return "[%d nodes from layer: %s]" % (len(value), ", ".join(nodetypes))
            else:
                return "[%d nodes from %d layers]" % (len(value), len(nodetypes))
    else:
        return repr(value)
