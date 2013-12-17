

binary_ops = [
    ('Eq', '=', '__eq__'),
    ('Ne', '!=', '__ne__'),
    ('Lt', '<', '__lt__'),
    ('Gt', '>', '__gt__'),
    ('Le', '<=', '__le__'),
    ('Ge', '>=', '__ge__'),
    ('And', 'and', '__and__'),
    ('Or', 'or', '__or__'),
    ('Add', '+', '__add__'),
    ('Sub', '-', '__sub__'),
    ('Mul', '*', '__mul__'),
    ('Div', '/', '__div__'),
    ('Mod', '%', '__mod__'),
    ('In', 'in', 'isin'),
    ('Like', 'like', 'like'),
    ('Glob', 'glob', 'glob'),
    ('Match', 'match', 'match'),
    ('Regexp', 'regexp', 'regexp'),
]

prefix_unary_ops = [
    ('Not', 'not', '__invert__'),
    ('Pos', '+', '__pos__'),
    ('Neg', '-', '__neg__'),
]

postfix_unary_ops = [
    ('IsNull', 'isnull', 'isnull'),
    ('NotNull', 'notnull', 'notnull'),
]


class Expr(object):
    def __init__(self, value):
        self.value = value

    for class_name, op, method_name in prefix_unary_ops + postfix_unary_ops:
        exec ('''
            def {0}(self):
                return {1}(self)
        '''.format(method_name, class_name)).strip()
    del class_name, op, method_name

    for class_name, op, method_name in binary_ops[2:]:
        exec ('''
            def {0}(self, other):
                return {1}(self, other)
        '''.format(method_name, class_name)).strip()
    del class_name, op, method_name

    def __eq__(self, other):
        if other is None:
            return IsNull(self)
        return Eq(self, other)

    def __ne__(self, other):
        if other is None:
            return NotNull(self)
        return Ne(self, other)

    def sql(self):
        try:
            sql = self.value.sql
        except AttributeError:
            return '?'
        return sql()

    __str__ = sql

    def args(self):
        try:
            args = self.value.args
        except AttributeError:
            return (self.value,)
        return args()

    def execute(self):
        cur = connection.get_connection().cursor()
        cur.execute(self.sql(), self.args())
        return cur

    def executemany(self, args):
        cur = connection.get_connection().cursor()
        cur.executemany(self.sql(), args)
        return cur


class Parenthesizing(object):
    pass


class PrefixUnaryOp(Expr, Parenthesizing):
    def sql(self):
        sql = super(PrefixUnaryOp, self).sql()
        if isinstance(self.value, Parenthesizing):
            return '%s (%s)' % (self._op, sql)
        return '%s %s' % (self._op, sql)


class PostfixUnaryOp(Expr, Parenthesizing):
    def sql(self):
        sql = super(PostfixUnaryOp, self).sql()
        if isinstance(self.value, Parenthesizing):
            return '(%s) %s' % (sql, self._op)
        return '%s %s' % (sql, self._op)


for class_name, op, method_name in prefix_unary_ops:
    locals()[class_name] = type(class_name, (PrefixUnaryOp,), dict(_op=op))
del class_name, op, method_name, prefix_unary_ops


for class_name, op, method_name in postfix_unary_ops:
    locals()[class_name] = type(class_name, (PostfixUnaryOp,), dict(_op=op))
del class_name, op, method_name, postfix_unary_ops


class BinaryOp(Expr, Parenthesizing):
    def __init__(self, lvalue, rvalue):
        self.lvalue = lvalue if isinstance(lvalue, Expr) else Expr(lvalue)
        self.rvalue = rvalue if isinstance(rvalue, Expr) else Expr(rvalue)

    def sql(self):
        lsql = self.lvalue.sql()
        if isinstance(self.lvalue, Parenthesizing):
            lsql = '(%s)' % (lsql,)
        rsql = self.rvalue.sql()
        if isinstance(self.rvalue, Parenthesizing):
            rsql = '(%s)' % (rsql,)
        return '%s %s %s' % (lsql, self._op, rsql)

    def args(self):
        return self.lvalue.args() + self.rvalue.args()


for class_name, op, method_name in binary_ops:
    locals()[class_name] = type(class_name, (BinaryOp,), dict(_op=op))
del class_name, op, method_name, binary_ops


class Sql(Expr):
    def sql(self):
        return self.value

    def args(self):
        return ()


class ExprList(list, Expr, Parenthesizing):
    _no_sequence = object()

    def __init__(self, sequence=_no_sequence):
        if sequence is ExprList._no_sequence:
            return super(ExprList, self).__init__()
        return super(ExprList, self).__init__((
            item if isinstance(item, Expr) else Expr(item)
            for item in sequence
        ))

    def append(self, object):
        super(ExprList, self).append(
            object if isinstance(object, Expr) else Expr(object))

    def extend(self, iterable):
        super(ExprList, self).extend((
            object if isinstance(object, Expr) else Expr(object)
            for object in iterable
        ))

    def insert(self, index, object):
        super(ExprList, self).insert(index,
            object if isinstance(object, Expr) else Expr(object))

    def __getitem__(self, y):
        if isinstance(y, (int, long)):
            return super(ExprList, self).__getitem__(y)
        return ExprList(super(ExprList, self).__getitem__(y))

    def __getslice__(self, i, j):
        return ExprList(super(ExprList, self).__getslice__(i, j))

    def __setitem__(self, i, y):
        if isinstance(i, (int, long)):
            super(ExprList, self).__setitem__(i,
                y if isinstance(y, Expr) else Expr(y))
        else:
            super(ExprList, self).__setitem__(i, ExprList(y))

    def __setslice__(self, i, j, y):
        super(ExprList, self).__setslice__(i, j, ExprList(y))

    def __add__(self, other):
        ret = self[:]
        ret.extend(other)
        return ret

    def __iadd__(self, other):
        self.extend(other)
        return self

    def __mul__(self, n):
        return ExprList(super(ExprList, self).__mul__(n))

    __rmul__ = __mul__

    def sql(self):
        items = ((item.sql(), isinstance(item, Parenthesizing))
                 for item in self)
        return ', '.join(
            '(%s)' % (sql,) if paren else sql
            for sql, paren in items
        )

    def args(self):
        args = []
        for item in self:
            args.extend(item.args())
        return tuple(args)


class Asc(Expr):
    def sql(self):
        return super(Asc, self).sql() + ' asc'


class Desc(Expr):
    def sql(self):
        return super(Desc, self).sql() + ' desc'


class Limit(Sql):
    def __init__(self, limit_slice):
        if isinstance(limit_slice, (int, long)):
            limit_slice = slice(limit_slice)
        if limit_slice.step is not None:
            raise TypeError('step is not supported')
        if (
            (limit_slice.stop is not None and limit_slice.stop < 0) or
            (limit_slice.start is not None and limit_slice.start < 0)
        ):
            raise NotImplementedError('negative slice values not supported')
        if (
            (limit_slice.stop is not None and
             not isinstance(limit_slice.stop, (int, long))) or
            (limit_slice.start is not None and
             not isinstance(limit_slice.start, (int, long)))
        ):
            raise TypeError('slice values must be numbers')
        if (
            limit_slice.start is not None and
            limit_slice.stop is not None and
            limit_slice.stop < limit_slice.start
        ):
            raise ValueError('stop must be greater than start')
        self.offset = limit_slice.start
        self.limit = (None if limit_slice.stop is None else (
            limit_slice.stop if limit_slice.start is None
            else limit_slice.stop - limit_slice.start))

    def sql(self):
        if self.offset is None and self.limit is None:
            return ''
        if self.offset is None:
            return 'limit %d' % (self.limit,)
        if self.limit is None:
            return 'limit %d, -1' % (self.offset,)
        return 'limit %d, %d' % (self.offset, self.limit)


class Select(Expr, Parenthesizing):
    def __init__(self, what=None, sources=None,
                 where=None, order=None, limit=None):
        if what is None:
            if sources is None:
                raise TypeError('must specify sources if not specifying what')
            self.what = Sql('*')
        else:
            self.what = what if isinstance(what, Expr) else Expr(what)
        self.sources = sources
        self.where = where
        self.order = order
        self.limit = limit

    def order_by(self, *args):
        if args:
            order = ExprList(args)
        else:
            order = None
        return type(self)(
            self.what, self.sources, self.where, order, self.limit)

    def find(self, where, *ands):
        if not isinstance(where, Expr):
            where = Expr(where)
        if ands:
            where = reduce(And, ands, where)
        if self.where:
            where = self.where & where
        return type(self)(
            self.what, self.sources, where, self.order, self.limit)

    def delete(self):
        return Delete(self.sources, self.where, self.order, self.limit)

    def exists(self):
        q = Select(Sql('1'), self.sources, self.where, limit=Limit(1))
        return q.execute().fetchone() is not None

    def __len__(self):
        q = Select(Sql('count(*)'), self.sources, self.where)
        n = q.execute().fetchone()[0]
        if self.limit is not None:
            if self.limit.offset:
                n -= self.limit.offset
            if self.limit.limit is not None and n > self.limit.limit:
                return self.limit.limit
            if n < 0:
                return 0
        return n

    def __iter__(self):
        return iter(self.execute())

    def __getitem__(self, y):
        q = type(self)(self.what, self.sources, self.where, self.order)
        if isinstance(y, (int, long)):
            q.limit = Limit(slice(y, y + 1))
            try:
                return iter(q).next()
            except StopIteration:
                raise IndexError(y)
        else:
            q.limit = Limit(y)
            return q

    def sql(self):
        sql = 'select ' + self.what.sql()
        if self.sources is not None:
            sql += ' from ' + self.sources.sql()
        if self.where is not None:
            sql += ' where ' + self.where.sql()
        if self.order is not None:
            sql += ' order by ' + self.order.sql()
        if self.limit is not None:
            limit = self.limit.sql()
            if limit:
                sql += ' ' + limit
        return sql

    def args(self):
        args = list(self.what.args())
        if self.sources is not None:
            args.extend(self.sources.args())
        if self.where is not None:
            args.extend(self.where.args())
        if self.order is not None:
            args.extend(self.order.args())
        if self.limit is not None:
            args.extend(self.limit.args())
        return tuple(args)


class Delete(Expr):
    def __init__(self, sources, where=None, order=None, limit=None):
        if isinstance(sources, ExprList) and len(sources) > 1:
            raise TypeError("can't delete from more than one table")
        self.sources = sources
        self.where = where
        self.order = order
        self.limit = limit

    def order_by(self, *args):
        if args:
            order = ExprList(args)
        else:
            order = None
        return type(self)(self.sources, self.where, order, self.limit)

    def sql(self):
        sql = 'delete from ' + self.sources.sql()
        if self.where is not None:
            sql += ' where ' + self.where.sql()
        if self.order is not None:
            sql += ' order by ' + self.order.sql()
        if self.limit is not None:
            limit = self.limit.sql()
            if limit:
                sql += ' ' + limit
        return sql

    def args(self):
        args = list(self.sources.args())
        if self.where is not None:
            args.extend(self.where.args())
        if self.order is not None:
            args.extend(self.order.args())
        if self.limit is not None:
            args.extend(self.limit.args())
        return tuple(args)


class Insert(Expr):
    def __init__(self, model, columns=None, values=None, on_conflict=None):
        self.model = model
        if columns is None and values is not None:
            if not isinstance(values, Select):
                raise TypeError(
                    'must specify columns when values is not a Select')
        self.columns = columns
        self.values = values
        self.on_conflict = on_conflict

    def sql(self):
        sql = 'insert'
        if self.on_conflict is not None:
            sql += ' or ' + self.on_conflict
        sql += ' into ' + self.model.sql()
        if self.values is None:
            sql += ' default values'
            return sql
        if self.columns is not None:
            sql += ' (' + self.columns.sql() + ')'
        if isinstance(self.values, Select):
            sql += ' ' + self.values.sql()
        else:
            sql += ' values (' + self.values.sql() + ')'
        return sql

    def args(self):
        args = list(self.model.args())
        if self.columns is not None:
            args.extend(self.columns.args())
        if self.values is not None:
            args.extend(self.values.args())
        return tuple(args)


class Update(Expr):
    def __init__(self, model, columns, values, where=None, on_conflict=None):
        self.model = model
        self.columns = columns
        self.values = values
        self.where = where
        self.on_conflict = on_conflict

    def sql(self):
        sql = 'update'
        if self.on_conflict is not None:
            sql += ' or ' + self.on_conflict
        sql += ' ' + self.model.sql()
        sql += ' set ' + ExprList(
            Sql('%s = %s' % (column.sql(), value.sql()))
            for column, value in zip(self.columns, self.values)
        ).sql()
        if self.where is not None:
            sql += ' where ' + self.where.sql()
        return sql

    def args(self):
        args = list(self.model.args())
        args.extend(self.columns.args())
        args.extend(self.values.args())
        if self.where is not None:
            args.extend(self.where.args())
        return tuple(args)

class Query(object):
    '''
    Gives quick access to database by setting attributes (query conditions, et
    cetera), or by the sql methods.
    
    Instance Methods
    ----------------
    
    Creating a Query object requires a Model class at the bare minimum. The 
    doesn't run until results are pulled using a slice, ``list()`` or iterated
    over.
    
    For example::
    
        q = Query(model=MyModel)
        
    This sets up a basic query without conditions. We can set conditions using
    the ``filter`` method::
        
        q.filter(name='John', age=30)
        
    We can also chain the ``filter`` method::
    
        q.filter(name='John').filter(age=30)
        
    In both cases the ``WHERE`` clause will become::
    
        WHERE `name` = 'John' AND `age` = 30
    
    You can also order using ``order_by`` to sort the results::
    
        # The second arg is optional and will default to ``ASC``
        q.order_by('column', 'DESC')
    
    You can limit result sets by slicing the Query instance as if it were a 
    list. Query is smart enough to translate that into the proper ``LIMIT`` 
    clause when the query hasn't yet been run::
    
        q = Query(model=MyModel).filter(name='John')[:10]   # LIMIT 0, 10
        q = Query(model=MyModel).filter(name='John')[10:20] # LIMIT 10, 10
        q = Query(model=MyModel).filter(name='John')[0]    # LIMIT 0, 1
    
    Simple iteration::
    
        for obj in Query(model=MyModel).filter(name='John'):
            # Do something here
            
    Counting results is easy with the ``count`` method. If used on a ``Query``
    instance that has not yet retrieve results, it will perform a ``SELECT
    COUNT(*)`` instead of a ``SELECT *``. ``count`` returns an integer::
        
        count = Query(model=MyModel).filter=(name='John').count()
            
    Class Methods
    -------------
    
    ``Query.raw_sql(sql, values)`` returns a database cursor. Usage::
    
        query = 'SELECT * FROM `users` WHERE id = ?'
        values = (1,) # values must be a tuple or list
        
        # Now we have the database cursor to use as we wish
        cursor = Query.raw_swl(query, values)
        
    ``Query.sql(sql, values)`` has the same syntax as ``Query.raw_sql``, but 
    it returns a dictionary of the result, the field names being the keys.
    
    '''
    
    def __init__(self, query_type='SELECT *', conditions={}, model=None, db=None):
        from autumn.model import Model
        self.type = query_type
        self.conditions = conditions
        self.order = ''
        self.limit = ()
        self.cache = None
        if not issubclass(model, Model):
            raise Exception('Query objects must be created with a model class.')
        self.model = model
        if db:
            self.db = db
        elif model:
            self.db = model.db
        
    def __getitem__(self, k):
        if self.cache != None:
            return self.cache[k]
        
        if isinstance(k, (int, long)):
            self.limit = (k,1)
            lst = self.get_data()
            if not lst:
                return None
            return lst[0]
        elif isinstance(k, slice):
            if k.start is not None:
                assert k.stop is not None, "Limit must be set when an offset is present"
                assert k.stop >= k.start, "Limit must be greater than or equal to offset"
                self.limit = k.start, (k.stop - k.start)
            elif k.stop is not None:
                self.limit = 0, k.stop
        
        return self.get_data()
        
    def __len__(self):
        return len(self.get_data())
        
    def __iter__(self):
        return iter(self.get_data())
        
    def __repr__(self):
        return repr(self.get_data())
        
    def count(self):
        if self.cache is None:
            self.type = 'SELECT COUNT(*)'
            return self.execute_query().fetchone()[0]
        else:
            return len(self.cache)
        
    def filter(self, **kwargs):
        self.conditions.update(kwargs)
        return self
        
    def order_by(self, field, direction='ASC'):
        self.order = 'ORDER BY %s %s' % (escape(field), direction)
        return self
        
    def extract_condition_keys(self):
        if len(self.conditions):
            return 'WHERE %s' % ' AND '.join("%s=%s" % (escape(k), self.db.conn.placeholder) for k in self.conditions)
        
    def extract_condition_values(self):
        return list(self.conditions.itervalues())
        
    def query_template(self):
        return '%s FROM %s %s %s %s' % (
            self.type,
            self.model.Meta.table_safe,
            self.extract_condition_keys() or '',
            self.order,
            self.extract_limit() or '',
        )
        
    def extract_limit(self):
        if len(self.limit):
            return 'LIMIT %s' % ', '.join(str(l) for l in self.limit)
        
    def get_data(self):
        if self.cache is None:
            self.cache = list(self.iterator())
        return self.cache
        
    def iterator(self):        
        for row in self.execute_query().fetchall():
            obj = self.model(*row)
            obj._new_record = False
            yield obj
            
    def execute_query(self):
        values = self.extract_condition_values()
        return Query.raw_sql(self.query_template(), values, self.db)
        
    @classmethod
    def get_db(cls, db=None):
        if not db:
            db = getattr(cls, "db", autumn_db)
        return db
        
    @classmethod
    def get_cursor(cls, db=None):
        db = db or cls.get_db()
        return db.conn.connection.cursor()
        
    @classmethod
    def sql(cls, sql, values=(), db=None):
        db = db or cls.get_db()
        cursor = Query.raw_sql(sql, values, db)
        fields = [f[0] for f in cursor.description]
        return [dict(zip(fields, row)) for row in cursor.fetchall()]
            
    @classmethod
    def raw_sql(cls, sql, values=(), db=None):
        db = db or cls.get_db()
        cursor = cls.get_cursor(db)
        try:
            cursor.execute(sql, values)
            if db.b_commit:
                db.conn.connection.commit()
        except BaseException, ex:
            if db.b_debug:
                print "raw_sql: exception: ", ex
                print "sql:", sql
                print "values:", values
            raise
        return cursor

    @classmethod
    def raw_sqlscript(cls, sql, db=None):
        db = db or cls.get_db()
        cursor = cls.get_cursor(db)
        try:
            cursor.executescript(sql)
            if db.b_commit:
                db.conn.connection.commit()
        except BaseException, ex:
            if db.b_debug:
                print "raw_sqlscript: exception: ", ex
                print "sql:", sql
            raise
        return cursor



# begin() and commit() for SQL transaction control
# This has only been tested with SQLite3 with default isolation level.
# http://www.python.org/doc/2.5/lib/sqlite3-Controlling-Transactions.html

    @classmethod
    def begin(cls, db=None):
        """
        begin() and commit() let you explicitly specify an SQL transaction.
        Be sure to call commit() after you call begin().
        """
        db = db or cls.get_db()
        db.b_commit = False

    @classmethod
    def commit(cls, db=None):
        """
        begin() and commit() let you explicitly specify an SQL transaction.
        Be sure to call commit() after you call begin().
        """
        cursor = None
        try:
            db = db or cls.get_db()
            db.conn.connection.commit()
        finally:
            db.b_commit = True
        return cursor
