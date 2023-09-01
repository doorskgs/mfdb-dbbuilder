import functools

service_types = {}


def declare_service_type(name, name_attr):
    service_types[name] = {}

    def service_wrapper(hashable):
        def decorator(cls):
            @functools.wraps(cls, updated=())
            class WrappedClass(cls):
                pass

            setattr(WrappedClass, name_attr, hashable)
            inst = WrappedClass()

            service_types[name][hashable] = inst
            return WrappedClass
        return decorator

    def service_getter(hashable):
        if hashable not in service_types[name]:
            raise Exception(f"{name} for {hashable} is not registered!")

        return service_types[name][hashable]

    return service_wrapper, service_getter


def iter_services(name):
    for serv in service_types[name].values():
        yield serv
