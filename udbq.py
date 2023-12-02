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
# Copyright (c) 2023 Paul Sokolovsky
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
        if isinstance(tables[0], type):
            self.row_type = tables[0]
            tables = tables[1:]
        self.tables = tables
        self.op = "SELECT"
        self.cols = "*"
        self.cond = None
        self._order_by = ""
        self._limit = ""

    def copy(self):
        return copy.copy(self)

    def clone_if(self):
        if self.pristine:
            self = self.copy()
            self.pristine = False
        return self

    def select(self, *cols):
        self = self.clone_if()
        self.op = "SELECT"
        self.cols = cols
        return self

    def insert(self, **kwargs):
        self.op = "INSERT"
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
        self.cond = where(cond, *vals, **kwargs)
        return self

    def and_(self, cond=None, *vals, **kwargs):
        self.cond.and_(cond, *vals, **kwargs)
        return self

    def or_(self, cond=None, *vals, **kwargs):
        self.cond.or_(cond, *vals, **kwargs)
        return self

    def order_by(self, *cols):
        self = self.clone_if()
        self._order_by = " ORDER BY " + ", ".join(cols)
        return self

    def limit(self, n):
        self = self.clone_if()
        self._limit = " LIMIT %d" % n
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


class Model:
    def __init__(self, r):
        self.r = r

    def __getattr__(self, a):
        return self.r[a]

    def __getitem__(self, a):
        return self.r[a]

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
            raise StopIteration
        return self.cls(row)


class DB:
    def __init__(self, dburn):
        self.conn = sqlite3.connect(dburn)
        self.conn.row_factory = sqlite3.Row

    def execute(self, stmt):
        if not isinstance(stmt, table):
            raise TypeError("table() clause expected")

        tables = ", ".join(stmt.tables) + " "

        sql = stmt.op + " "
        vals = []
        if stmt.op == "INSERT":
            cols = stmt.updates.keys()
            sql += " INTO %s(%s) VALUES (%s)" % (tables, ", ".join(cols), ", ".join(["?"] * len(cols)))
            vals = tuple(stmt.updates.values())
        else:
            if stmt.op in ("SELECT", "DELETE"):
                if stmt.op == "SELECT":
                    cols = ", ".join(stmt.cols)
                    sql += cols + " "
                sql += "FROM "
                sql += tables
            elif stmt.op == "UPDATE":
                sql += tables
                sql += " SET "
                for k, v in stmt.updates.items():
                    sql += "%s=? " % k
                    vals.append(v)
            else:
                1/0
            if stmt.cond:
                sql += "WHERE " + stmt.cond.cond
                vals += stmt.cond.vals
            sql += stmt._order_by
            sql += stmt._limit
        _log.debug("%s %s", sql, vals)
        cur = self.conn.cursor()
        cur.execute(sql, vals)
        return cur

    def __call__(self, stmt):
        cur = self.execute(stmt)
        if stmt.op == "INSERT":
            id = cur.lastrowid
            cur.close()
            return id
        return ResultSet(cur, stmt.row_type)

    def first(self, stmt):
        cur = self.execute(stmt)
        res = cur.fetchone()
        cur.close()
        if res is None:
            return None
        return stmt.row_type(res)
