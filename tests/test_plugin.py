from mock import patch, Mock

import pytest

from patroni.plugin import load, configure, get_event_handler

class Plugin1:

    def __init__(self, name, log):
        self.log = log
        self.name = name

    def event(self, arg1):
        self.log.append((self.name, 'event', arg1))


class Plugin2:

    def __init__(self, name, log):
        self.log = log
        self.name = name

    def event(self, arg1):
        self.log.append((self.name, 'event', arg1))
        return arg1 + '-ho'

    def other_event(self, arg1, arg2):
        self.log.append((self.name, 'other_event', arg1, arg2))
        return arg1 + arg2

@patch('patroni.plugin.iter_entry_points')
def test_multiple_plugins(iter_entry_points):
    plugin1 = Mock()
    plugin1.name = 'plugin1'
    plugin2 = Mock()
    plugin2.name = 'plugin2'
    plugin3 = Mock()
    plugin3.name = 'plugin3'
    iter_entry_points.return_value = [plugin2, plugin3, plugin1]
    config_12 = dict(plugins=['plugin1', 'plugin2'])
    config_21 = dict(plugins=['plugin2', 'plugin1'])
    config_23 = dict(plugins=['plugin2', 'plugin3'])
    plugins = load(config_12)
    plugin1.load.assert_called_once_with(require=False)
    assert plugins == [('plugin1', plugin1.load()), ('plugin2', plugin2.load())]
    # reverse order in the config file and you see the plugins are returned in that order
    plugins = load(config_21)
    assert plugins == [('plugin2', plugin2.load()), ('plugin1', plugin1.load())]
    # try now with only one plugin configured
    assert not plugin3.load.called
    plugins = load(config_23)
    plugin3.load.assert_called_once_with(require=False)
    assert plugins == [('plugin2', plugin2.load()), ('plugin3', plugin3.load())]

def test_get_event_handler():
    log = []
    plugins = configure([
            ('plugin1', Plugin1),
            ('plugin2', Plugin2),
            ('plugin3', Plugin1),
            ], log)
    handler = get_event_handler(plugins, ['no_op_event', 'event', 'other_event'])
    assert log == []
    # The no-op event has no handlers, so it is defined as None
    assert handler.no_op_event is None
    # Call the real event
    result = handler.event('hey')
    assert result == [
        None,
        'hey-ho',
        None,
        ]
    assert log == [
        ('plugin1', 'event', 'hey'),
        ('plugin2', 'event', 'hey'),
        ('plugin3', 'event', 'hey'),
        ]
    log[:] = []
    # with non-returning events, the events are just called
    result = handler.other_event(1, 5)
    assert result == [6]
    assert log == [
        ('plugin2', 'other_event', 1, 5),
        ]

def test_get_event_handler_with_single_event():
    # Test an "single" event. This is an event which can not have more than one handler
    log = []
    plugins = configure([
            ('plugin2', Plugin2),
            ], log)
    handler = get_event_handler(plugins, [dict(name='event', type='single', required=True)])
    # Call the real event
    result = handler.event('hey')
    assert result == 'hey-ho'
    assert log == [
        ('plugin2', 'event', 'hey'),
        ]
    # it is an error to configure no handlers for this event
    plugins = configure([], log)
    with pytest.raises(AssertionError) as ex:
        handler = get_event_handler(plugins, [dict(name='event', type='single', required=True)])
    # unless not required
    plugins = configure([
            ], log)
    handler = get_event_handler(plugins, [dict(name='event', type='single', required=False)])
    assert handler.event is None
    # and more than one is allways an error
    plugins = configure([
            ('plugin1', Plugin1),
            ('plugin2', Plugin2),
            ], log)
    with pytest.raises(AssertionError) as ex:
        handler = get_event_handler(plugins, [dict(name='event', type='single', required=True)])
