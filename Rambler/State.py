class State(object):
    """This class is used to represents state machines. It aids
    debugging because you don't need to look up numeric ids. Future
    versions may even include the ability to specify transition
    behavior.
    """

    # each state group is stored in this dict
    statesByGroup = {}

    def __new__(cls, group, stateName):
        if not cls.statesByGroup.has_key(group):
            cls.statesByGroup[group] = {}

        if not cls.statesByGroup[group].has_key(stateName):
            
            # the given state was already instantiated, create it and
            # cache it so we don't bloat memory

            # TODO: Consider auto assiging a numeric value to states as well

            state = object.__new__(cls)
            state.name = stateName
            state.group = group
            cls.statesByGroup[group][stateName] = state
            
        return cls.statesByGroup[group][stateName]

    def __repr__(self):
        return "<State %s(%s)>" % (self.group, self.name)

    def __str__(self):
        return self.name
            
        
