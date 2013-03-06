# this is a simple class to add '.' style access
# takes a (possibly) hierarcical dictionary and returns a DotDict class
# in addition to '.' syntax, the the resulting class can be used as a dictionary

# most useful for interactive use!
import copy

class DotDict(dict):
    def __init__(self,arg={}):
        for k in arg.keys():
            if (type(arg[k]) is dict):
                self[k]=DotDict(arg[k])
            else:
                self[k]=arg[k]

    def __getattr__(self, attr):
        return self.get(attr, None)

    def __setattr__(self, name, value):
        if isinstance(value, dict):
            dict.__setitem__(self, name, DotDict(value))
        else:
            dict.__setitem__(self, name, value)
    __delattr__= dict.__delitem__

    def __dir__(self):
        return self.keys() + dir(dict(self))

    def __deepcopy__(self, memo):
        # for details: http://www.peterbe.com/plog/must__deepcopy__
        return DotDict(copy.deepcopy(dict(self))) 
