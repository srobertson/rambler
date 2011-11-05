import copy
import logging
import types
from collections import defaultdict


from Rambler import outlet



class Binding:
    def __init__(self, name, interface, attribute, allowUnassembled=False):
        self.name = name
        self.interface = interface
        self.attribute = attribute
        self.allowUnassembled = allowUnassembled
        
MARKER = ()
        
class CompBinder(object):

    """Instantiates and binds componets to each other after ensuring
    that dependencies are met. Components can be either a class or an
    instance of a new style class.

    For example say we have two classes that each represesnt a component:

    >>> class Comp1(object):
    ...   def __init__(self):
    ...     self.bindCount = 0
    ...   def _get_subcomp(self):
    ...     return self._subcomp
    ...   def _set_subcomp(self, subcomp):
    ...     self._subcomp = subcomp
    ...     self.bindCount += 1
    ...   subcomp = property(_get_subcomp, _set_subcomp)
    
    >>> class Comp2(object):
    ...   bound = False
    ...   def assembled(cls):
    ...     cls.bound=True
    ...   assembled = classmethod(assembled)

    Comp2 is bound to Comp1 as Comp1's 'subcomp' property. (say that 5
    times fast)

    We can use a CompBinder to glue these two components together.

    >>> compBinder = CompBinder()

    >>> bindings=[Binding('Comp2', object, 'subcomp')]
    >>> compBinder.addComponent('Comp1', Comp1(), bindings)

    Comp2 has no bindings. Another way to say it is, Comp2 is not
    dependant upon any other component.
    
    >>> compBinder.addComponent('Comp2', Comp2)

    Now we'll bind all our components. The CompBinder will first
    verify that all dependencies have been met, instantiate each
    component and finally bind the componetns together by assigning
    them to the attributes as listed in the bindings section.

    >>> compBinder.bind()

    Binding completed with no errors which means we can now get our
    hands on Comp1 and it should have 'subcomp' attribute which points
    to Comp2. In other words Comp2 was bound to Comp1's subcomp
    attribute. Say that three times fast

    >>> comp1=compBinder.lookup('Comp1')

    Notice that we instantiated Comp1 prior to handing it to the
    CompBinder. However with Comp2 we passed the class directly in.

    >>> type(comp1) == Comp1
    True
    >>> comp1.subcomp == Comp2
    True

    This is because the component system doesn't care whether you use
    an instance or a class, it treats them both as components.

    Because of the delayed binding somtimes you can't use your
    components until everything has been bound. If you have a
    component that needs to know when it's safe to use all of it's
    components you can define a assembled() method, which will be
    called by the CompBinder after it has bound all the objects
    together.

    If you noticed in the above implementation of Comp2, we did
    this. Since everything has been bound at this point...

    >>> Comp2.bound
    True

    It's not an error to call bind() multiple times, this can be used
    by system that need to start up certain services prior to loading others. 

    >>> compBinder.bind()

    >>> comp1a = compBinder.lookup('Comp1')
    >>> comp1a == comp1
    True

    If an object was previously bound, calling bind won't rebind new components to it.
    >>> comp1.bindCount == 1
    True

    Components can have the ComponentRegistry bound to them just like
    any other component

    >>> bindings = [Binding('ComponentRegistry', object, 'componentRegistry')]
    >>> compBinder.addComponent('NeedsCompReg', Comp1, bindings)
    >>> compBinder.bind()
    >>> compWithReg = compBinder.lookup('NeedsCompReg')
    >>> compWithReg.componentRegistry == compBinder
    True
    
  
    Only newstyle classes, or instance of newstyle classes can be bound.

    >>> class Comp3:
    ...   pass
    >>> compBinder.addComponent('Comp3', Comp3)
    Traceback (most recent call last):
    ...
    TypeError: Error adding component 'Comp3' only new style classes or instance of new style classes can be used as components.
    

    If the dependencies aren't met for an object, it won't be fully bound

    >>> class Comp3(object):
    ...   pass
    >>> bindings = [Binding('MissingComponent', object, 'missingAttribute')]
    >>> compBinder.addComponent('NeedsMissingComponent', Comp3, bindings)
    >>> compBinder.bind()
    >>> 'NeedsMissingComponent' in compBinder.needsBinding
    True

    If later you can meet the dependencies you can call bind again and
    your component will be removed from the list.

    >>> compBinder.addComponent('MissingComponent', Comp2)
    >>> compBinder.bind()
    >>> 'NeedsMissingComponent' in compBinder.needsBinding
    False


    
    """

    def __init__(self):
        self.deps  = {'ComponentRegistry': []}
        self.needsBinding = {}
        self.bound = {'ComponentRegistry': self}
        self.log=logging.getLogger('CompBinder')
        self.circular_bindings = set()

    def addComponent(self, name, comp, bindings=None):
        if (name in self.bound) or (name in self.needsBinding):
          return
           
        if  type(comp) in (types.InstanceType, types.ClassType):
            raise TypeError, "Error adding component '%s' only new style classes "\
                  "or instance of new style classes can be used as components." % name

        if bindings is None:
            bindings = []

        # check the components attributes for outlets
        if issubclass(type(comp), type):
            mostDerivedClass = comp
        else:
            mostDerivedClass = comp.__class__

        # loop through the moste derived classes and any class it
        # inherits from looking for outlet definitions. Q: WHat
        # happens if we inherit from an object which is also
        # registered as a component? I don't think it'll cause
        # problems, worst case scenario is that its outlet will get
        # set twice...

        # we use the method resolution order to walk the inheritance chain

        for klass in mostDerivedClass.__mro__:
            attributes = klass.__dict__.items()
                     
            for attr, value  in attributes:
                if type(value) == outlet:
                    bindings.append(Binding(value.compName, value.interface, 
                                            attr, value.allowUnassembled))

        if len(bindings) ==  0:
            # No deependencies
            self.bound[name] = comp
            self.notifyAssembled(comp)
        else:
            self.needsBinding[name] = (comp, bindings)
        
            for binding in bindings:
                # returns the dependency list or a blank list
                deps = self.deps.get(binding.name, [])
                deps.append(name)
                self.deps[binding.name] = deps



    def bind(self):

        
        while self.needsBinding: # While there's items that need binding
            
            waitingToBeBound = self.needsBinding.keys()
            for compname in waitingToBeBound:
                comp, bindings = self.needsBinding[compname]

                deferred = []
                for binding in bindings:
                    if self.bound.has_key(binding.name):
                        victim = self.bound[binding.name]
                        self._bind(comp, compname, binding, victim)

                    elif binding.allowUnassembled and self.needsBinding.has_key(binding.name):
                        # Component is explicitly declaring that it's
                        # ok to bind the specified component to it in
                        # a non-useable state.  There is an implied
                        # contract that the component won't use the
                        # bound component until the system is fully
                        # started.

                        victim = self.needsBinding[binding.name][0]
                        self._bind(comp, compname, binding, victim)
                        # track the binding so that later we can bust it
                        self.circular_bindings.add((compname,binding.name))
                        
                        
                    else:
                        self.log.debug("Defered binding %s to %s" % (binding.name, compname))
                        # A component that this component depends on
                        # hasn't been assembeled yet, which means this                    
                        # component will need to be reexamined
                        deferred.append(binding)

                if len(deferred) == 0:
                    self.log.debug("Assembeled %s" % (compname,))
                    # Object was fully assembeled, notify it if the
                    # component wants to be notified
                    self.bound[compname] = comp
                    del self.needsBinding[compname]
                    self.notifyAssembled(comp)
                else:
                    # The remaing attributes to bind
                    self.needsBinding[compname] = (comp, deferred)
            
            if self.needsBinding.keys() == waitingToBeBound:
                
                # Every pass through the components should decrease
                # this list, if not...
                break
                
                
    def _bind(self, comp, compname, binding, victim):
        if hasattr(victim, "__binding__"):
            # Components that define __binding__ can
            # have a surrogate object other than
            # themselves bound in their place.
                            
            victim = victim.__binding__(compname, comp)

        self.log.debug("Binding component %s to %s's %s attribute."
                  % (binding.name, comp, binding.attribute))

        # descriptors __set__ aren't called by setattr on classes, only instances
        # this mimics the behavior
        try:
          comp.__dict__[binding.attribute].__set__(comp, victim)
        except:
          setattr(comp, binding.attribute, victim)

        
    def missingDeps(self):
        """Returns a list of components that are needed."""
        missingDeps = {}
        for dep in self.deps:
            if not self.needsBinding.has_key(dep) and not self.bound.has_key(dep):
                missingDeps[dep] = self.deps[dep]
        return missingDeps
            # Make sure all the dependencaies are met
            
            # TODO: Verify the interfaces here as well
        #    if not self.needsBinding.has_key(dep) and not self.bound.has_key(dep):
        #        raise FailedDependencyError("Missing '%s', which is needed by %s"
        #                                    % (dep, self.deps[dep]))



            
    def lookup(self, name, default=MARKER):
        # Guess we shouldn't be able to call this when we're not bound
        comp = self.bound.get(name, default)
        if comp is MARKER:
          raise KeyError(name)
        return comp

    # TODO: Deprecate get
    get = lookup


    def notifyAssembled(self, comp):
        if  hasattr(comp, 'assembled'):
            comp.assembled()
    
    def notify(self, object, what):
      method = getattr(object, what, None)
      if callable(method):
        method()

    def analyzeFailures(self):
        """Returns a string that's useful for analyzing why components have failed"""
        
        # TODO: provide methods for better diagnosis, i.e. tell us
        # which componets hand circular refs and which had failed deps
        errMsg = ["Missing Components:"]
        missing = self.missingDeps()
        failed = set()
        for compname, dependents in missing.items():
            failed.update(dependents)
            errMsg.append(("  %s: needed by\n    " % compname) + "\n    ".join(dependents))

        errMsg.append("Circular Dependencies:")

        for compname in self.needsBinding:
            if compname not in failed:

                # If it didn't fail because of missing dependecies
                # then it failed because of circular dependencies
                errMsg.append("  %s" % compname)

        return "\n".join(errMsg)
        
    def bindings_for(self, comp):
      bindings = []
      
      # check the components attributes for outlets
      if issubclass(type(comp), type):
        mostDerivedClass = comp
      else:
        mostDerivedClass = comp.__class__
        
      # loop through the moste derived classes and any class it
      # inherits from looking for outlet definitions. Q: WHat
      # happens if we inherit from an object which is also
      # registered as a component? I don't think it'll cause
      # problems, worst case scenario is that its outlet will get
      # set twice...
      
      # we use the method resolution order to walk the inheritance chain

      for klass in mostDerivedClass.__mro__:
        attributes = klass.__dict__.items()

        for attr, value  in attributes:
          if type(value) == outlet:
            bindings.append(Binding(value.compName, value.interface, 
                          attr, value.allowUnassembled))
      return bindings
      
        
    def unbind(self):
      """Disassemble the system starting with components that have no dependents.
      
      notifying each component with a disassembled"""
      
      deps = copy.deepcopy(self.deps)
      
      # break  circular dependencies first
      for dependent, compname in self.circular_bindings:
        deps[compname].remove(dependent)

      while 1:
        start = copy.deepcopy(deps)
        for name in self.bound.keys():
          
          dependents = deps.get(name)
          if not dependents: # safe to unbind            
            self.log.debug('Ubinding %s', name)
            comp = self.bound.pop(name)
            self.notify(comp, 'will_disassemble')

            for binding in self.bindings_for(comp):
              self.log.debug("  %s[%s] = None",  binding.attribute, binding.name)
              
              try:
                deps[binding.name].remove(name)
              except KeyError:
                # this should always return one item
                items = [(c,d) for c,d in self.circular_bindings if (c==name) and (d == binding.name)  ]
                assert len(items) == 1
                self.circular_bindings.remove(items[0])
              except ValueError,e:
                #TODO: I think components that have inherited attributes are showing are multiplying
                # the number of deps. For example in the auction the Components that inherit from
                # DownloadComponent are not here when we call .remove(name). 
                #self.log.exception('Error removing dep  deps[%s].remove(%s)',  binding.name, name)
                pass
              
              try:
                comp.__dict__[binding.attribute].__set__(comp, None)
              except:
                setattr(comp, binding.attribute, None)
            deps.pop(name, None)
              
            # return the componet back to the needsBinding state
            #self.addComponent(name, comp)
            self.notify(comp, 'did_disassemble')
            
        if start == deps:
          # if no changes were made in this pass we're done, if we have circular_bindings,
          # let's see if we can bust them
          break
          

      if len(deps) != 0:
        self.log.warn("The following components were left bound %s", self.bound.keys())
        
      self.deps  = {'ComponentRegistry': []}
      self.bound = {'ComponentRegistry': self}

if __name__=="__main__":
    import doctest
    import CompBinder
    #doctest.debug(CompBinder,'CompBinder')
    doctest.testmod(CompBinder)
