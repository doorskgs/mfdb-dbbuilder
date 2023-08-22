
def list_elements_equal(lst):
    """
    Validates if all items in list are equal
    :param lst:
    :return:
    """
    if len(lst) <= 1:
        return True
    return all(ele == lst[0] for ele in lst)


def get_inequal_attributes(disco_group, attr_to_validate_on):
    """
    Validates multiple runs of metabolite discovery by checking if they have equal attributes.
    :param disco_group: list of metabolite discovery results
    :param attr_to_validate_on: list of attributes to check equivalence on
    """
    pass
    # for attr in attr_to_validate_on:
    #     initv = disco_group[0][attr]
    #
    #
    #
    #     if not all(equal_sol(disco_run[attr], initv) for disco_run in disco_group):
    #         yield attr



def iter_sol(meta, attr):
    v = meta.get(attr)

    # iterates scalar or list
    if isinstance(v, (list, set, tuple)):
        for e in v:
            yield e
    elif v is not None:
        yield v