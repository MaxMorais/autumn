"""
Extracted from https://github.com/coleifer/peewee
"""
from __future__ import absolute_import, unicode_literals
from autumn.util import Cache

cache = Cache()

class SignalBase(type):
    def __new__(cls, name, bases, attrs):
        if name == 'Signal':
            return super(SignalBase, cls).__new__(cls, name, bases, attrs)

        new_class = type.__new__(cls, name, bases, attrs)
        cache.add(new_cls)
        return new_class

class Signal(object):
    __metaclass__ = SignalBase
    def __init__(self):
        self._flush()

    def connect(self, receiver, name=None, sender=None):
        name = name or receiver.__name__
        if name not in self._receivers:
            self._receivers[name] = (receiver, sender)
            self._receiver_list.append(name)
        else:
            raise ValueError('receiver named {0} already connected'.format(name))

    def disconnect(self, receiver=None, name=None):
        if receiver:
            name = receiver.__name__
        if name:
            del self._receivers[name]
            self._receiver_list.remove(name)
        else:
            raise ValueError('a receiver or a name must be provided')

    def send(self, sender, *args, **kwargs):
        responses = []
        for name in self._receiver_list:
            r, s = self._receivers[name]
            if s is None or sender is s:
                responses.append((r, r(sender, *args, **kwargs)))
        return responses

    def _flush(self):
        self._receivers = {}
        self._receiver_list = []

class WithSignalsBase(type):
    def __new__(cls, name, bases, attrs):
        if name == 'WithSignals':
            return super(WithSignals, cls).__new__(cls, name, bases, attrs)

        handlers = attrs.get('__handlers__', ())
        if instance(handlers, basestring):
            if handlers.lower in ('*', 'all'):
                handlers = filter(lambda k: callable(attrs[k], attrs.iterkeys())
            else:
                handlers = (handlers,)
        elif instance(handlers, (tuple,list,set)):
            handlers = tuple(handlers)
        else:
            handlers = ()

        for handler in handlers:
            attribute = attrs.get(handler, None)

            if attribute is not None:
                sender = '{0}.{1}'.format(name, attribute)
                attribute = signal_wrapper(name, attribute, sender)
   
                for when in attrs.get('__when__', ()):
                    signal = signal_factory(instant, cls.__module__)
                    signalname = '.'.join([name, when, handler])

                    setattr(attribute, when, connect(
                        signal,
                        signalname,
                        sender
                    ))

        return type.__new__(cls, name, bases, attrs)


class WithSignals(object):
    __metaclass__ = WithSignalsBase
    __handlers__ = ()
    __when__ = ('before', 'after', 'custom')
    def __init__(self):
        Super(WithSignals, self).__init__()

def signal_wrapper(class_name, method, sender):
    method_name = method.__name__
    def inner(self, *args, **kwargs):
        send_signal(**{
            'signal': class_name+'_before_'+method_name,
            'sender': sender,
            'instance': self,
            'data': {},
            'options': {'args': args, 'kwargs': kwargs}
        })

        ret = method(self, *args, **kwargs)
        
        for instant in ['after', 'custom']:
            send_signal(**{
                'signal': '_'.join([class_name, instant, method_name]),
                'sender': sender,
                'data': {'result': ret},
                'instance': self,
                'options': {'args': args, 'kwargs': kwargs}
            })
    
        return ret

    return inner


def signal_factory(name, module):
    try:
        signal = cache.get('.'.join([module, name]))
    except cache.NotInCache:
        signal = type(name, (Signal,), {'__module__': module})()
    return signal

def connect(signal, name=None, sender=None):
    def decorator(fn):
        signal.connect(fn, name, sender)
        return staticmethod(fn)
    return decorator

def send_signal(**kw):
    """Send signal abstract handler.

    You can to override it by settings.SIGNAL_SEND_HANDLER
    For example, you can use one from next event systems:
    https://github.com/jesusabdullah/pyee
    https://bitbucket.org/jek/blinker
    https://launchpad.net/pydispatcher
    https://github.com/theojulienne/PySignals
    https://github.com/olivierverdier/dispatch
    and others.
    """
    return cache.get(kw.pop('signal')).send(kw)
