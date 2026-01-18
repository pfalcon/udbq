# udbq - Minimalist database query builder / anti-ORM
#
# https://github.com/pfalcon/udbq
#
# This module is part of the Pycopy ecosystem, minimalist and lightweight
# Python environment.
#
# https://github.com/pfalcon/pycopy
#
# The MIT License (MIT)
#
# Copyright (c) 2023-2025 Paul Sokolovsky
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

import sqlite3
import copy
import logging


_log = logging.getLogger(__name__)


class where:
    @staticmethod
    def render_clause(cond=None, *vals, **kwargs):
        outvals = []
        if cond is not None and kwargs:
            raise TypeError("Use either textual condition or kwargs")
        if cond is not None:
            cond += " ?"  * len(vals)
            outvals.extend(vals)
        else:
            keys = []
            for k, v in kwargs.items():
                if isinstance(v, tuple):
                    keys.append("%s IN (%s)" % (k, ", ".join(["?"] * len(v))))
                    outvals.extend(v)
                elif v is None:
                    keys.append("%s IS NULL" % k)
                else:
                    keys.append("%s=?" % k)
                    outvals.append(v)
            cond = " AND ".join(keys)
        return cond, outvals

    def __init__(self, cond=None, *vals, **kwargs):
        self.cond, self.vals = self.render_clause(cond, *vals, **kwargs)

    def _connect(self, op, cond=None, *vals, **kwargs):
        if isinstance(cond, where):
            self.cond += " %s (%s)" % (op, cond.cond)
            vals = cond.vals
        else:
            cond, vals = self.render_clause(cond, *vals, **kwargs)
            self.cond += " %s %s" % (op, cond)
        self.vals.extend(vals)
        return self

    def and_(self, cond=None, *vals, **kwargs):
        return self._connect("AND", cond, *vals, **kwargs)

    def or_(self, cond=None, *vals, **kwargs):
        return self._connect("OR", cond, *vals, **kwargs)


class table:
    def __init__(self, *tables):
        self.pristine = True
        self.row_type = Model
        if tables:
            if isinstance(tables[0], type):
                self.row_type = tables[0]
                tables = tables[1:]
        self.tables = tables
        self.op = "SELECT"
        self.cols = "*"
        self.cond = None
        self._order_by = ""
        self._group_by = ""
        self._having = None
        self._limit = ""
        self._offset = ""
        self.withs = []
        self.alias = None

    def with_(self, alias, query):
        self.withs.append((alias, query))
        return self

    def copy(self):
        return copy.deepcopy(self)

    def clone_if(self):
        if self.pristine:
            self = self.copy()
            self.pristine = False
        return self

    def add_table(self, table):
        """Add table after constructor was called. This is useful when we
        want to add joining against a table based on some condition (so we
        construct a base query and then add more tables/joining .where's)."""
        self.tables += (table,)
        return self

    def join(self, table, on_clause):
        self.tables = list(self.tables)
        self.tables[-1] = "%s JOIN %s ON %s" % (self.tables[-1], table, on_clause)
        return self

    def left_join(self, table, on_clause):
        self.tables = list(self.tables)
        self.tables[-1] = "%s LEFT JOIN %s ON %s" % (self.tables[-1], table, on_clause)
        return self

    def select(self, *cols):
        self = self.clone_if()
        self.op = "SELECT"
        if not cols:
            cols = "*"
        self.cols = cols
        return self

    def add_select(self, *cols):
        self.cols += cols

    def insert(self, **kwargs):
        self.op = "INSERT"
        self.updates = kwargs
        return self

    def replace(self, **kwargs):
        self.op = "INSERT OR REPLACE"
        self.updates = kwargs
        return self

    def update(self, **kwargs):
        self.op = "UPDATE"
        self.updates = kwargs
        return self

    def delete(self):
        self.op = "DELETE"
        return self

    def where(self, cond=None, *vals, **kwargs):
        self = self.clone_if()
        if self.cond is None:
            self.cond = where(cond, *vals, **kwargs)
        else:
            self.cond.and_(cond, *vals, **kwargs)
        return self

    def having(self, cond=None, *vals, **kwargs):
        self = self.clone_if()
        if self._having is None:
            self._having = where(cond, *vals, **kwargs)
        else:
            self._having.and_(cond, *vals, **kwargs)
        return self

    def and_(self, cond=None, *vals, **kwargs):
        self.cond.and_(cond, *vals, **kwargs)
        return self

    def or_(self, cond=None, *vals, **kwargs):
        self.cond.or_(cond, *vals, **kwargs)
        return self

    def group_by(self, *cols):
        self = self.clone_if()
        self._group_by = " GROUP BY " + ", ".join(cols)
        return self

    def order_by(self, *cols):
        self = self.clone_if()
        self._order_by = " ORDER BY " + ", ".join(cols)
        return self

    def limit(self, n):
        self = self.clone_if()
        self._limit = " LIMIT %d" % n
        return self

    def offset(self, n):
        self = self.clone_if()
        self._offset = " OFFSET %d" % n
        return self

    def as_(self, alias):
        self.alias = alias
        return self

    def exe(self, db=None):
        if db is None:
            db = self.__db__
        return db(self)

    all = exe

    def first(self, db=None):
        if db is None:
            db = self.__db__
        return db.first(self)

    def render(self):
        sql = ""
        vals = []
        if self.withs:
            sql += "WITH "
            need_comma = False
            for alias, subq in self.withs:
                if need_comma:
                    sql += ", "
                sub_sql, sub_vals = subq.render()
                sql += "%s AS (%s) " % (alias, sub_sql)
                vals += sub_vals
                need_comma = True

        tables = ", ".join(self.tables) + " "

        if sql:
            sql += " "
        sql += self.op + " "
        if self.op.startswith("INSERT"):
            cols = self.updates.keys()
            sql += " INTO %s(%s) VALUES (%s)" % (tables, ", ".join(cols), ", ".join(["?"] * len(cols)))
            vals = tuple(self.updates.values())
        else:
            if self.op in ("SELECT", "DELETE"):
                if self.op == "SELECT":
                    cols = []
                    for c in self.cols:
                        if isinstance(c, table):
                            _sql, _vals = c.render()
                            c = "(%s) AS %s" % (_sql, c.alias)
                            vals += _vals
                        cols.append(c)
                    cols = ", ".join(cols)
                    sql += cols + " "
                if self.tables:
                    sql += "FROM "
                    sql += tables
            elif self.op == "UPDATE":
                sql += tables
                sql += " SET"
                comma = False
                for k, v in self.updates.items():
                    if comma:
                        sql += ","
                    if isinstance(v, tuple):
                        sql += " %s=%s" % (k, v[0])
                        vals.extend(v[1:])
                    else:
                        sql += " %s=?" % k
                        vals.append(v)
                    comma = True
            else:
                raise ValueError(self.op)
            if self.cond:
                sql += " WHERE " + self.cond.cond
                vals += self.cond.vals
            sql += self._group_by
            if self._having:
                sql += " HAVING " + self._having.cond
                vals += self._having.vals
            sql += self._order_by
            sql += self._limit
            sql += self._offset
        return sql, vals

    def sql(self):
        sql, vals = self.render()
        assert not vals
        return sql


class Model:
    def __init__(self, r):
        self.r = r

    def __getattr__(self, a):
        return self.r[a]

    def __getitem__(self, a):
        return self.r[a]

    def keys(self):
        return self.r.keys()

    def __repr__(self):
        return "<%s %s>" % (self.__class__.__name__, dict(self.r))


class ResultSet:
    def __init__(self, cur, cls):
        self.cur = cur
        self.cls = cls

    def __iter__(self):
        return self

    def __next__(self):
        row = self.cur.fetchone()
        if row is None:
            self.cur.close()
            self.cur = None
            raise StopIteration
        return self.cls(row)

    def close(self):
        if self.cur:
            self.cur.close()
            self.cur = None


class DB:
    def __init__(self, dburn):
        self.conn = sqlite3.connect(dburn)
        self.conn.row_factory = sqlite3.Row

    def execute(self, stmt):
        if not isinstance(stmt, table):
            raise TypeError("table() clause expected")
        sql, vals = stmt.render()
        _log.debug("%s %s", sql, vals)
        cur = self.conn.cursor()
        cur.execute(sql, vals)
        return cur

    def __call__(self, stmt):
        cur = self.execute(stmt)
        if stmt.op.startswith("INSERT"):
            id = cur.lastrowid
            cur.close()
            return id
        elif stmt.op == "SELECT":
            return ResultSet(cur, stmt.row_type)
        else:
            r = cur.rowcount
            cur.close()
            return r

    def first(self, stmt):
        cur = self.execute(stmt)
        res = cur.fetchone()
        cur.close()
        if res is None:
            return None
        return stmt.row_type(res)

    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()

    def close(self):
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
