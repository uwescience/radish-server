# Setup
In server.js:
* Set hostname and port to the server's hostname and port
* Set environment variable RACO\_PATH to the path of raco/.
* Set environment variable GRAPPA\_PATH to the path of grappa/.

# Inserting datasets
* Create a csv file (1 dataset entry per line)
* csv schema: 
user name, program name, relation name, uri (for clang it is http://hostname:port), number of tuples, backend the data can be used on, the number column names, a comma separated list of column names followed by column types
 * example:
public,adhoc,sp2bench,http://my.radish-server.org:1337,100000000,clang,3,subject,predicate,object,INT_TYPE,INT_TYPE,INT_TYPE
 * Note: user name, program name, relation name, and backend are the primary keys
 * the uri should be user-accessible as it will be used for direct downloads
* Run the command:
```./datastore.py insert_new_dataset -p (filename.csv)```

# Run the clang/grappa server
node server.js
