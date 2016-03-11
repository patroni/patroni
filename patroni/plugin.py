"""Plugin Machinery

Plugins are objects which subscribe to events. An object subscribes to an event
by implementing a method with the event name. There is a single namespace of event
names, plugins can subscribe to as many events as they choose.

Events come in 2 flavours:

    * multiple: Multiple plugins can subscribe to a multiple event. They will all be run
                in the order of their declaration in the config file. The return value
                may or may not be used.
    * single: Only one of these subscribers is allowed at a time.

Events must document the arguments they will be called with.

Registration and Usage
----------------------

Plugins are registered by adding them to the entry points in setup.py. e.g.

    entry_points={
        'patroni.plugin': [
            'myplugin = mymodule:MyPlugin',
        ],
    },

Plugins are only actually used if they appear in the configuration file in an
ordered list of plugins to use:

    plugins:
        - myplugin1
        - myplugin2

Each plugin should provide it's own documentation about what configuration it
needs.
"""
from pkg_resources import iter_entry_points

EVENTS = (
    {'name': 'postgresql_name',
        'type': 'single',
        'required': False},
    )


def get_plugins(config, app):
    plugins = load(config)
    plugins = configure(plugins, config, app)
    return get_event_handler(plugins, EVENTS)


def load(config):
    """Gets the plugin factories from the config file.

    Plugins are sorted by the order they are specified in the config file.
    """
    plugin_names = [c.strip() for c in config.get('plugins', [])]
    seen = set([])
    for i in plugin_names:
        if i in seen:
            raise AssertionError('Duplicate plugin: {}'.format(i))
        seen.add(i)
    plugins = {}
    available_plugins = []
    for i in iter_entry_points('patroni.plugin'):
        available_plugins.append(i.name)
        if i.name in plugin_names:
            plugin_factory = i.load(require=False)  # EEK never auto install ANYTHING
            if i.name in plugins:
                raise Exception('Duplicate plugins for name {}, one was: {}'.format(i.name, plugin_factory))
            plugins[i.name] = plugin_factory
    not_seen = set(plugin_names) - set(plugins)
    if not_seen:
        raise Exception('Plugins were configured in the config file, but I could NOT find them: {}\n'
                        'Available plugins: {}'.format(not_seen, available_plugins))
    return [(i, plugins[i]) for i in plugin_names]  # set order to what is specified in config file


def configure(plugins, *args, **kw):
    """Setup all plugins by calling them with the passed arguments"""
    c_plugins = []
    for name, plugin_factory in plugins:
        plugin = plugin_factory(name, *args, **kw)
        c_plugins.append((name, plugin))
    return c_plugins


def _handlers_executor(handlers):
    if not handlers:
        return None

    def call(self, *args, **kw):
        return [h(*args, **kw) for _, _, h in handlers]
    return call


def _handlers_executor_single(handlers):
    if not handlers:
        return None
    assert len(handlers) == 1
    handler = handlers[0][2]

    def call(self, *args, **kw):
        return handler(*args, **kw)
    return call


def get_event_handler(setup_plugins, events):
    class Handler:
        plugins = dict(setup_plugins)
    for event_name in events:
        if isinstance(event_name, dict):
            spec = event_name.copy()
            event_name = spec.pop('name')
        else:
            spec = dict(type='multiple',
                        required=False)
        handlers = []
        for name, plugin in setup_plugins:
            handler = getattr(plugin, event_name, None)
            if handler is None:
                continue
            handlers.append((name, event_name, handler))
        if spec['required'] and not handlers:
            raise AssertionError('At least one plugin must implement {}'.format(event_name))
        if spec['type'] == 'multiple':
            executor = _handlers_executor(handlers)
        elif spec['type'] == 'single':
            if len(handlers) > 1:
                raise AssertionError('Only one plugin can implement {}'.format(event_name))
            executor = _handlers_executor_single(handlers)
        else:
            raise NotImplementedError('unknown event spec type')
        setattr(Handler, event_name, executor)
    return Handler()
