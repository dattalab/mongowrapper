
from bson.binary import Binary
from bson.objectid import ObjectId
import gridfs
import pymongo
import pickle
import numpy as np
import datetime

import copy

import hashlib

__all__ = ['MongoWrapper']

class MongoWrapper(object):
    """Basic and light mongodb interface for saving experimental data files.

    Overview:

    This is a python based interface layer for storing and retreiving documents
    from a pymongo database. The major modules uses are pymongo, gridfs, 
    pickle, numpy.

    Each mongo database can contain multiple 'collections', each of which in 
    turn holds docuemnts.  Documents can be of any structure.  In python, we 
    can simply use dictionaries containing many different types of objects as
    our documents.

    However, there are some small restrictions to work around for using mongodb
    to store typical scientific data structures (typically numpy arrays).  The
    first is that every document inserted into a mongodb must be less than 
    16mb in size.  This is an abritrary, but hard limit. The second 
    limitiation is that numpy arrays (and indeed, all custom objects) must 
    be first encoded into a basic python type (typically a string) to be 
    inserted in the database.  One way to handle encoding is to 
    subclass SONManipulator
    (see http://api.mongodb.org/python/current/examples/custom_type.html)

    However, this approach won't work for large objects, because it will 
    generate strings that are still larger than 16mb.  Luckily, we can use 
    the gridFS abstraction layer to store objects of arbitrary size in a 
    a mongodb.  Briefly- gridFS is an abstraction layer on top of a collection 
    in a db (named 'fs' by default) that splits large objects over multiple 
    documents.  The idea then, is when saving a dictionary (or list of 
    dictionaries) to a collection 'collection' in database 'db', first scan 
    through and find any numpy arrays.  Encode those arrays, and store them 
    using gridFS in the collection 'fs' in db.  Then temporarily replace
    the numpy arrays with objectID pointers (ie: gridFS path) and store 
    the original document in 'collection'.
    
    When you want to retrieve objects from the db, you can do this using the
    standard query syntax in pymongo 
    (http://api.mongodb.org/python/2.0/tutorial.html). By default, the 
    documents you retrieve will not contain any numpy arrays they
    originally stored- instead, those keys will point to the objectIDs 
    corresponding to the appropriate gridfs files. These numpy arrays can 
    be loaded with the 'loadFullData' fuction, by passing in 'full=True' 
    into loadExperiment(), or by using loadFullExperiment() instead.  In 
    esscence, this means that the metadata for experiments is the primary 
    document in the collection, with the option of loading the full data 
    for analysis quite easily.
    
    Core methods:
    
    save(dictionary)
    load(query)
    delete(objectId)
    
    Usage:
    
    import imaging_analysis.core.io.mongodb as mdb
    # this creates both test and test_collection if they don't exist
    # Note that this connects, by default, to 'localhost:27017'. 
    # If you want to connect to a remote host, pass in 
    # 'hostname' and 'port' values to these functions (see documentation).    

    db = mdb.MongoWrapper('test','test_collection') 
    objectID = db.save(dictionary)

    # some varient here.  use the meta data to filter.
    experiments = db.load({'odor1name':'tmt'}) 
    
    Conventions:
    
    Each person/project will utilize a seperate database.  Within this db, each
    collection will store a specific general type of experimental data.  While
    the schema-free nature of mongodb makes this unnecessary, it is useful for
    organization purposes. The 'fs' collection is reserved for the gridfs 
    system, and will be shared for the storage of large objects across all 
    collections in a database.
    
    Each document, when inserted, is given a key-value pair containing a 
    TimeDate object that is time of insertion. Each document must have a 
    unique key-value called '_id' which contains an objectId which is unique 
    for that object. If the document lacks this, one is created on insertion.  
    If the document being is inserted already has an '_id' key-value pair, it 
    will update/overwrite any document with such an id.
    
    Note that if you load a document and all it's data, alter the data, and
    re-insert the document, the orignal data will be deleted from the gridFS,
    as, by convention, it has nothing pointing to it.  This is to prevent 
    'leaks' through rounds of processing on data.  Care should be taken to 
    duplicate data when needed.
    """
    def __init__(self, db_name, collection_name, hostname='localhost', port=27017, username="alexbw", password=""):
        self.db_name = db_name
        self.collection_name = collection_name
        self.hostname = hostname
        self.port = port

        self.connection = pymongo.Connection(hostname, port)
        if (username != ""):
            admin_db = self.connection["admin"]
            admin_db = admin_db.authenticate(username, password)

        self.db = self.connection[self.db_name]
        self.fs = gridfs.GridFS(self.db)

        self.collection = self.db[collection_name]

    def _close(self):
        self.connection.close()

    def __del__(self):
        self._close()

    # core methods.  load(), save(), delete()

    def save(self, document):
        """Stores a dictionary or list of dictionaries as as a document in collection.
        The collection is specified in the initialization of the object.

        Note that if the dictionary has an '_id' field, and a document in the
        collection as the same '_id' key-value pair, that object will be
        overwritten.  Any numpy arrays will be stored in the gridFS,
        replaced with ObjectId pointers, and a list of their ObjectIds will be
        also be stored in the 'npObjectID' key-value pair.  If re-saving an
        object- the method will check for old gridfs objects and delete them. 

        :param: document: dictionary of arbitrary size and structure, can contain numpy arrays. Can also be a list of such objects.
        :returns: List of ObjectIds of the inserted object(s).
        """

        # simplfy things below by making even a single document a list
        if not isinstance(document, list):
            document = [document]

        id_values = []
        for doc in document:

            docCopy = copy.deepcopy(doc)

            # make a list of any existing referenced gridfs files
            try:
                self.temp_oldNpObjectIDs = docCopy['_npObjectIDs']
            except KeyError:
                self.temp_oldNpObjectIDs = []

            self.temp_newNpObjectIds = []
            # replace np arrays with either a new gridfs file or a reference to the old gridfs file
            docCopy = self._stashNPArrays(docCopy)

            docCopy['_npObjectIDs'] = self.temp_newNpObjectIds
            doc['_npObjectIDs'] = self.temp_newNpObjectIds
            
            # cleanup any remaining gridfs files (these used to be pointed to by document, but no
            # longer match any np.array that was in the db
            for id in self.temp_oldNpObjectIDs:
                # print 'deleting obj %s' % id
                self.fs.delete(id)
            self.temp_oldNpObjectIDs = []

            # add insertion date field to every document
            docCopy['insertion_date'] = datetime.datetime.now()
            doc['insertion_date'] = datetime.datetime.now()
            
            # insert into the collection and restore full data into original document object
            new_id = self.collection.save(docCopy)
            doc['_id'] = new_id
            id_values.append(new_id)

        print 'Successfully inserted document(s)'
        return id_values

    def loadFromIds(self, Ids):
        """Conveience function to load from a list of ObjectIds or from their string
        representations.  Takes a singleton or a list of either type.

        :param Ids: can be an ObjectId, string representation of an ObjectId, or a list containing items of either type.
        :returns: List of documents from the DB.  If a document w/the object did not exist, a None object is returned instead.
        """
        if type(Ids) is not list:
            Ids = [Ids]

        out = []

        for id in Ids:
            if type(id) is ObjectId:
                obj_id = id
            elif (type(id) is str or type(id) is unicode):
                try:
                    obj_id = ObjectId(id)
                except:
                    obj_id = id
            out.append(self.load({'_id':obj_id}))

        return out
    
    def load(self, query, getarrays=True):
        """Preforms a search using the presented query. For examples, see:
        See http://api.mongodb.org/python/2.0/tutorial.html
        The basic idea is to send in a dictionaries which key-value pairs like
        mdb.load({'basename':'ag022012'}).

        :param query: dictionary of key-value pairs to use for querying the mongodb
        :returns: List of full documents from the collection
        """
        
        results = self.collection.find(query)
        
        if getarrays:
            allResults = [self._loadNPArrays(doc) for doc in results]
        else:
            allResults = [doc for doc in results]
        
        if allResults:
            if len(allResults) > 1:
                return allResults
            elif len(allResults) == 1:
                return allResults[0]
            else:
                return None
        else:
            return None

    def delete(self, objectId):
        """Deletes a specific document from the collection based on the objectId.
        Note that it first deletes all the gridFS files pointed to by ObjectIds
        within the document.

        Use with caution, clearly.

        :param objectId: an id of an object in the database.
        """
        # *** Add confirmation?
        documentToDelete= self.collection.find_one({"_id": objectId})
        npObjectIdsToDelete = documentToDelete['_npObjectIDs']
        for npObjectID in npObjectIdsToDelete:
            self.fs.delete(npObjectID)
        self.collection.remove(objectId)

    # utility functions

    def _npArray2Binary(self, npArray):
        """Utility method to turn an numpy array into a BSON Binary string.
        utilizes pickle protocol 2 (see http://www.python.org/dev/peps/pep-0307/
        for more details).

        Called by stashNPArrays.

        :param npArray: numpy array of arbitrary dimension
        :returns: BSON Binary object a pickled numpy array.
        """
        return Binary(pickle.dumps(npArray, protocol=2), subtype=128 )

    def _binary2npArray(self, binary):
        """Utility method to turn a a pickled numpy array string back into
        a numpy array.

        Called by loadNPArrays, and thus by loadFullData and loadFullExperiment.

        :param binary: BSON Binary object a pickled numpy array.
        :returns: numpy array of arbitrary dimension
        """
        return pickle.loads(binary)

    def _loadNPArrays(self, document):
        """Utility method to recurse through a document and gather all ObjectIds and
        replace them one by one with their corresponding data from the gridFS collection

        Skips any entries with a key of '_id'.

        Note that it modifies the document in place.

        :param document: dictionary like-document, storable in mongodb
        :returns: document: dictionary like-document, storable in mongodb
        """
        for (key, value) in document.items():
            if isinstance(value, ObjectId) and key != '_id':
                document[key] = self._binary2npArray(self.fs.get(value).read())
            elif isinstance(value, dict):
                document[key] = self._loadNPArrays(value)
        return document

    # modifies in place
    def _stashNPArrays(self, document):
        """Utility method to recurse through a document and replace all numpy arrays
        and store them in the gridfs, replacing the actual arrays with references to the
        gridfs path.

        Called by save()

        Note that it modifies the document in place, although we return it, too

        :param document: dictionary like-document, storable in mongodb
        :returns: document: dictionary like-document, storable in mongodb
        """
        
        for (key, value) in document.items():
            if isinstance(value, np.ndarray):
                dataBSON = self._npArray2Binary(value)
                dataMD5 = hashlib.md5(dataBSON).hexdigest()
                # does this array match the hash of anything in the object already?
                #                print 'data hash: %s' % dataMD5
                match = False
                for obj in self.temp_oldNpObjectIDs:
                    #                    print 'checking if %s is already in the db.. ' % obj
                    if dataMD5 == self.fs.get(obj).md5:
                        match = True
                        #  print 'yes, replacing np array w/old ojbectid: %s' % obj
                        document[key] = obj
                        self.temp_oldNpObjectIDs.remove(obj)
                        self.temp_newNpObjectIds.append(obj)
                if not match:
                    # print 'np array is not in the db, inserting new gridfs file'
                    obj = self.fs.put(self._npArray2Binary(value))
                    document[key] = obj
                    self.temp_newNpObjectIds.append(obj)

            elif isinstance(value, dict):
                document[key] = self._stashNPArrays(value)
                
            elif isinstance(value, np.number):
                if isinstance(value, np.integer):
                    document[key] = int(value)
                elif isinstance(value, np.inexact):
                    document[key] = float(value)
                
        return document
