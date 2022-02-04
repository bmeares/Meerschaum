#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

from __future__ import annotations

"""
Local copy of `cascadict` by JNeverly.
The source code (licensed under MIT) was copied over to resolve an issue with Python 3.10
and because the original project is now unmaintained.
"""

import collections.abc

class CascaDictError(Exception):
    pass


class CascaDict(collections.abc.MutableMapping):
    
    def __init__(self, *args, **kwargs):
        self.final_dict = dict()
        
        #assign ancestor and remove it from kwargs if necessary
        self._ancestor = kwargs.get('ancestor') or self
        if 'ancestor' in kwargs:
            del kwargs['ancestor']
                   
        self.update(dict(*args, **kwargs))
        
        #Cascade remaining nested cascadicts
        if not self.is_root():
            for k,v in self._ancestor.items():
                #If k is in final dict, it means it has been already cascaded during update.
                if isinstance(v, CascaDict) and not (k in self.final_dict):
                    self.final_dict[k]  = v.cascade()     

    def __getitem__(self, key):
        temp_dict = self
        while True:
            try:
                return temp_dict.final_dict[self.__keytransform__(key)]
            except KeyError as e:
                if temp_dict.is_root():
                    raise e
                else:
                    temp_dict = temp_dict.get_ancestor()

    def __setitem__(self, key, value):
        _value = value
        #This is the entire mechanism of nesting here:
        if isinstance(value, CascaDict):
            _value = value.cascade()
        elif isinstance(value, dict):
            _value = CascaDict(value).cascade()
        
        if key in self and isinstance(self[self.__keytransform__(key)], CascaDict) and isinstance(value, (CascaDict, dict)):
            if key in self.final_dict:
                self.final_dict[self.__keytransform__(key)].update(value)
            else:
                self.final_dict[self.__keytransform__(key)] = self[self.__keytransform__(key)].cascade(value)
        else:
            self.final_dict[self.__keytransform__(key)] = _value

    def __delitem__(self, key):
        try:
            del self.final_dict[self.__keytransform__(key)]
        except KeyError as ke:
            if self.__contains__(key):
                raise CascaDictError("Cannot delete ancestor's key.")
            else:
                raise ke
            
    def __iter__(self):
        temp_dict = self
        used_keys = []
        while True:
            for (key, value) in temp_dict.final_dict.items():
                if not (key in used_keys):
                    yield key
                    used_keys.append(key)
                 
            if temp_dict.is_root():
                return
            else:
                temp_dict = temp_dict.get_ancestor()          

    def __len__(self):
        return len(self.__flatten__())
    
    def __contains__(self, key):
        temp_dict = self
        while True:
            temp = (self.__keytransform__(key) in temp_dict.final_dict)
            if temp:
                return temp
            else:
                if temp_dict.is_root():
                    return False
                else:
                    temp_dict = temp_dict.get_ancestor()
    
    def __repr__(self):
        return "<{0}, Ancestor: {1}>".format(self.final_dict, self._ancestor if (not self.is_root()) else None)

    def __keytransform__(self, key):
        return key
    
    def __sizeof__(self):
        return self.__flatten__().__sizeof__()
    
#     def items(self, level='top'):
#         return self.__flatten__(level).items()
#     
#     def keys(self, level='top'):
#         return self.__flatten__(level).keys()
#     
#     def values(self, level='top'):
#         return self.__flatten__(level).values()
#     
#     def iterkeys(self, level='top'):
#         return self.__flatten__(level).iterkeys()
# 
#     def iteritems(self, level='top'):
#         return self.__flatten__(level).iteritems()
# 
#     def itervalues(self, level='top'):
#         return self.__flatten__(level).itervalues()
    
    #===========================================================================
    # CascaDict methods
    #===========================================================================
    
    
    @classmethod
    def new_cascadict(cls, dict):
        """Helper constructor for automatically cascading new CascaDict from object,
        regardless if it's another :class:`CascaDict` or simple :class:`dict`.

        Parameters
        ----------
        dict :
            class:`CascaDict` or  :class:`dict` object which will be cascaded.

        Returns
        -------

        """
        if isinstance(dict, CascaDict):
            return dict.cascade()
        else:
            return CascaDict(dict)
    
    def __flatten__(self, level='top', recursive=True):
        """ Create flat :class:`dict` containing all keys (even from ancestors). 
            In case of overlapping values, value according to the 'level' argument will be selected.
        
        :param level:    ['top', 'bottom', 'skim'] Default: 'top'
                         
                         - 'top' level flattens with top level values for overlapping keys.
                         - 'bottom' level flattens with bottom level (=closer to root) for overlapping keys.
                         - 'skim' means that only values which were added to the final :class:`CascaDict`
                             will be returned. Ancestor values are ignored, even those which are not overlapped.
                             
        :param recursive: [:const:`True`, :const:`False`] Default :const:`True`. 
                            If :const:`True`, same flattening protocol is used for nested CascaDicts. 
                            Otherwise nested CascaDicts are simply referenced.
        """
        if not (level in ['top', 'bottom', 'skim']):
            raise CascaDictError("Unknown level '{0}'".format(level))

        flat_dict = {}
        temp_dict = self
        while True:
            for (key, value) in temp_dict.final_dict.items():
                
                if level == 'top':
                    if key not in flat_dict:
                        if recursive and isinstance(value, CascaDict):
                            value = value.__flatten__(level=level, recursive=recursive)
                        flat_dict[key] = value 
                    
                else:
                    if recursive and isinstance(value, CascaDict):
                            value = value.__flatten__(level=level, recursive=recursive)
                    flat_dict[key] = value
                    
            if temp_dict.is_root() or (level == 'skim'):
                return flat_dict
            else:
                temp_dict = temp_dict.get_ancestor()              
    
    def cascade(self, *args, **kwargs):
        """Create new empty :class:`CascaDict` cascading from this one.

        Parameters
        ----------
        *args :
            
        **kwargs :
            

        Returns
        -------

        """
        kwargs['ancestor'] = self
        return CascaDict(*args, **kwargs)
    
    def get_ancestor(self):
        """ """
        return self._ancestor or self #self ancestor must not be none
    
    def get_root(self):
        """Returns root ancestor for given CascaDict."""
        temp = self
        while not temp.is_root():
            temp = temp.get_ancestor()
        return temp
    
    def is_root(self):
        """Returns :const:`True` if CascaDict has no ancestors (is root of the ancestor tree)."""
        return (self._ancestor is self)
        
    def get_cascaded(self, key, default=[None,]):
        """Get item. If key is contained also in ancestors,
            a list of items from all ancestor for given key is retuned, sorted from top to bottom.

        Parameters
        ----------
        key :
            param default:    Default value to be returned when no key is found.
        default :
             (Default value = [None)
        ] :
            

        Returns
        -------

        """
        temp_dict = self
        tempval = []
        while True:
            try:
                temp =  temp_dict.final_dict[self.__keytransform__(key)]
                tempval.append(temp)
            except KeyError as e:
                pass
            
            if temp_dict.is_root():
                break
            else:
                temp_dict = temp_dict.get_ancestor()
        return tempval or default                    

    #For compatibility
    def has_key(self, k):
        return self.__contains__(k)
    
    def copy_flat(self, level='top', recursive=True):
        """

        Parameters
        ----------
        level :
             (Default value = 'top')
        recursive :
             (Default value = True)

        Returns
        -------
        type
            Wrapper function for :func:`__flatten__`

        """
        return self.__flatten__(level=level, recursive=recursive)
            
