from autumn.db.query import Query
from autumn.model import cache

def deference_column(name):
    model_name, column_attr = name.rsplit('.', 1)
    try:
        return getattr(cache.get(model_name), column_attr)
    except cache.NotInCache:
        raise RuntimeError('unknown model {0}'.format(model_name))
    except AttributeError:
        raise RuntimeError('unknown column {0}'.format(name))

class Relation(object):
    
    def __init__(self, lft_col, rgt_col):
        self.lft_col = lft_col
        self.rgt_col = rgt_col

    def _set_up(self, instance, owner):
        if isinstance(self.rgt_col, basestring):
            model_name, column_attr = self.rgt_col.rsplit('.', 1)
            try:
                self.rgt_col = getattr(cache.get(model_name), column_attr):
            except cache.NotInCache:
                raise RuntimeError('unknown model {0}'.format(model_name)
            except AttributeError:
                raise RuntimeError('unknown column {0}'.format(name))

class ForeignKey(Relation):
        
    def __get__(self, instance, owner):
        if instance is None:
            return self
        super(ForeignKey, self)._set_up(instance, owner)
        value = getattr(instance, self.lft_col.name)
        q = self.rgt_col.model.select(self.lft_col==value)
        try:
            return q[0]
        except IndexError:
            return None

    def __set__(self, instance, value):
        super(ForeignKey, self)._set_up(instance, value)
        _value = getattr(instance, self.rgt_col.name)
        setattr(instance, self.lft_col.name, value)


class OneToMany(Relation):
    
    def __get__(self, instance, owner):
        super(OneToMany, self)._set_up(instance, owner)
        if not instance:
            return self.model
        if not self.field:
            self.field = '%s_id' % instance.Meta.table
        conditions = {self.field: getattr(instance, instance.Meta.pk)}
        return Query(model=self.model, conditions=conditions)