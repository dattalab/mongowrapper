mongo-numpy
===========
#### A wrapper for MongoDB that makes saving and loading NumPy arrays transparent.

This is a python based interface layer for storing and retreiving documents
from a pymongo database. The major modules uses are `pymongo`, `gridfs`, 
`pickle`, `numpy`.

There are some  restrictions to work around for using mongodb
to store typical scientific data structures (typically numpy arrays). First, 
documents have to be less than 16MB, and the default python MongoDB driver
doesn't automatically store custom objects in a document. We use MongoDB's
custom filesystem, `GridFS`, to store the numpy arrays behind the scenes.
However, when storing and retrieving your document, you'll never have to
think about this.

Core methods:

`save(dictionary)` any python dictionary, even with NumPy arrays as values  
`load(query)`, where query is a regular pymongo query  
`delete(objectId)`  

Usage:
```
db = mdb.MongoWrapper(dbName='test',
                      collectionName='test_collection', 
                      hostname="localhost", 
                      port="27017") 
my_dict = {"name": "Important experiment", 
            "data":np.random.random((100,100))}
```

The dictionary's just as you'd expect it to be:
```
print my_dict
{'data': array([[ 0.773217,  0.517796,  0.209353, ...,  0.042116,  0.845194,
         0.733732],
       [ 0.281073,  0.182046,  0.453265, ...,  0.873993,  0.361292,
         0.551493],
       [ 0.678787,  0.650591,  0.370826, ...,  0.494303,  0.39029 ,
         0.521739],
       ..., 
       [ 0.854548,  0.075026,  0.498936, ...,  0.043457,  0.282203,
         0.359131],
       [ 0.099201,  0.211464,  0.739155, ...,  0.796278,  0.645168,
         0.975352],
       [ 0.94907 ,  0.363454,  0.912208, ...,  0.480943,  0.810243,
         0.217947]]),
 'name': 'Important experiment'}
```

And saving is really super easy.
```
db.save(my_dict)
```

Loading the dictionary back in is just as super duper easy.
```
my_loaded_dict = db.load({"name":"Important experiment"})
```

You'll notice some extra stuff in the dictionary:
```
print my_loaded_dict
{u'_id': ObjectId('513797159ee8623b8e4c5868'),
 u'_npObjectIDs': [ObjectId('513797159ee8623b8e4c5866')],
 u'data': array([[ 0.464792,  0.356568,  0.941366, ...,  0.306581,  0.748432,
         0.422235],
       [ 0.328053,  0.374875,  0.42858 , ...,  0.151123,  0.224338,
         0.355108],
       [ 0.582536,  0.279675,  0.123347, ...,  0.608384,  0.829723,
         0.551811],
       ..., 
       [ 0.647574,  0.890914,  0.249452, ...,  0.833569,  0.224882,
         0.166035],
       [ 0.360945,  0.426059,  0.112768, ...,  0.275226,  0.065119,
         0.155346],
       [ 0.278053,  0.532446,  0.592866, ...,  0.632853,  0.678774,
         0.902657]]),
 u'insertion_date': datetime.datetime(2013, 3, 6, 14, 20, 53, 293000),
 u'name': u'Important experiment'}
```

Note that there'll be some added keys in the dictionary, 
 -  `_npObjectIDs`
 -  `_id`
 -  `insertion_date`  
The insertion_date you might not care about, but do keep
_id and _npObjectIDs around. They're important for tracking
the document and the numpy arrays associated with it.
