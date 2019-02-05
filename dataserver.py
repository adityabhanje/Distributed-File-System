#!/usr/bin/env python

import sys, SimpleXMLRPCServer, getopt, pickle, time, threading, xmlrpclib, unittest,shelve,os
from datetime import datetime, timedelta
from xmlrpclib import Binary
from collections import defaultdict
from sys import argv

xy=0            
f=''            
class SimpleHT:
  def __init__(self):
    self.data = defaultdict(list)
    self.data1= defaultdict(list)
    self.r1data = defaultdict(list)
    self.r1data1= defaultdict(list)
    self.r2data = defaultdict(list)
    self.r2data1= defaultdict(list)
    self.cdata = defaultdict(list)
    self.cdata1= defaultdict(list)
    self.r1cdata = defaultdict(list)
    self.r1cdata1= defaultdict(list)
    self.r2cdata = defaultdict(list)
    self.r2cdata1= defaultdict(list)


  def corruptd(self, path):
    path=pickle.loads(path.data)
    db=shelve.open(f)
    self.data=db['data']
    self.data[path][0]='$$$$$$$$'
    db['data']=self.data
    db.close()
    return Binary(pickle.dumps(True))

 
  
  def checkprd(self):
    db=os.path.isfile(f)
    return Binary(pickle.dumps(db))



  def count(self):
    return len(self.data)
  def getd(self, key):
    db = shelve.open(f)
    self.data=db['data']

    key = key.data
    print (key)
    if key in self.data:
        print("True")
    else:
        print("false")
    db.close()
    return Binary(pickle.dumps(self.data[key]))


  def putd(self, key, value):
    db = shelve.open(f)
    value = pickle.loads(value.data)
    self.data[key.data] = value
    self.data1[key.data] = value
    db['data']=self.data
    db['data1']=self.data1
    db.close()
    return Binary(pickle.dumps(True))


  
  def getd3(self):
    
    db = shelve.open(f)
    self.data1=db['data1']
    p = self.data1
    db.close()
    return Binary(pickle.dumps(p))

  # Insert something into the HT
  def putd3(self, value):
    # Remove expired entries
    db=shelve.open(f)
    value = pickle.loads(value.data)
    self.data1 = value
    self.data = value
    db['data']=self.data
    db['data1']=self.data1
    db.close()
    return Binary(pickle.dumps(True))



  # Retrieve something from the HT
  def cgetd(self, key):
    # Default return value
    db = shelve.open(f)
    self.cdata=db['cdata']
    # If the key is in the data structure, return properly formated results
    key = key.data
    print (key)
    if key in self.cdata:
        print("True")
    else:
        print("false")
    db.close()
    return Binary(pickle.dumps(self.cdata[key]))

  # Insert something into the HT
  def cputd(self, key, value):
    # Remove expired entries
    db=shelve.open(f)
    value = pickle.loads(value.data)
    self.cdata[key.data] = value
    self.cdata1[key.data] = value
    db['cdata']=self.cdata
    db['cdata1']=self.cdata1
    db.close()

    return Binary(pickle.dumps(True))


  # Retrieve something from the HT
  def cgetd3(self):
    # Default return value
    db = shelve.open(f)
    self.cdata1=db['cdata1']
    p = self.cdata1
    db.close()
    return Binary(pickle.dumps(p))

  # Insert something into the HT
  def cputd3(self, value):
    # Remove expired entries
    db=shelve.open(f)
    value = pickle.loads(value.data)
    self.cdata1 = value
    self.cdata = value
    db['cdata']=self.cdata
    db['cdata1']=self.cdata1
    db.close()
    return Binary(pickle.dumps(True))






  def r1getd(self, key):
    # Default return value
    db = shelve.open(f)
    self.r1data=db['r1data']
    
    # If the key is in the data structure, return properly formated results
    key = key.data
    print (key)
    if key in self.r1data:
        print("True")
    else:
        print("false")
    db.close()
    return Binary(pickle.dumps(self.r1data[key]))

  # Insert something into the HT
  def r1putd(self, key, value):
    # Remove expired entries
    db=shelve.open(f)
    value = pickle.loads(value.data)
    self.r1data[key.data] = value
    self.r1data1[key.data] = value
    db['r1data']=self.r1data
    db['r1data1']=self.r1data1
    db.close()
    return Binary(pickle.dumps(True))



  def r1getd3(self):
    db = shelve.open(f)
    # Default return value
    self.r1data1=db['r1data1']
    p = self.r1data1
    db.close()
    return Binary(pickle.dumps(p))

  # Insert something into the HT
  def r1putd3(self, value):
    # Remove expired entries
    db=shelve.open(f)
    value = pickle.loads(value.data)
    self.r1data1 = value
    self.r1data = value
    db['r1data']=self.r1data
    db['r1data1']=self.r1data1
    db.close()
    return Binary(pickle.dumps(True))






  def r1cgetd(self, key):
    # Default return value
    db = shelve.open(f)
    self.r1cdata=db['r1cdata']
    # If the key is in the data structure, return properly formated results
    key = key.data
    print (key)
    if key in self.r1cdata:
        print("True")
    else:
        print("false")
    db.close()
    return Binary(pickle.dumps(self.r1cdata[key]))

  # Insert something into the HT
  def r1cputd(self, key, value):
    # Remove expired entries
    db=shelve.open(f)
    value = pickle.loads(value.data)
    self.r1cdata[key.data] = value
    self.r1cdata1[key.data] = value
    db['r1cdata']=self.r1cdata
    db['r1cdata1']=self.r1cdata1
    db.close()
    return Binary(pickle.dumps(True))



  def r1cgetd3(self):
    db = shelve.open(f)
    self.r1cdata1=db['r1cdata1']
    # Default return value
    p = self.r1cdata1
    db.close()
    return Binary(pickle.dumps(p))

  # Insert something into the HT
  def r1cputd3(self, value):
    # Remove expired entries
    db=shelve.open(f)
    value = pickle.loads(value.data)
    self.r1cdata1 = value
    self.r1cdata = value
    db['r1cdata']=self.r1cdata
    db['r1cdata1']=self.r1cdata1
    db.close()
    return Binary(pickle.dumps(True))





  def r2getd(self, key):
    db = shelve.open(f)
    self.r2data=db['r2data']
    # Default return value
    # If the key is in the data structure, return properly formated results
    key = key.data
    print (key)
    if key in self.r2data:
        print("True")
    else:
        print("false")
    db.close()
    return Binary(pickle.dumps(self.r2data[key]))

  # Insert something into the HT
  def r2putd(self, key, value):
    
    # Remove expired entries
    db=shelve.open(f)
    value = pickle.loads(value.data)
    self.r2data[key.data] = value
    self.r2data1[key.data] = value
    db['r2data']=self.r2data
    db['r2data1']=self.r2data1
    db.close()
    return Binary(pickle.dumps(True))



  def r2getd3(self):
    db = shelve.open(f)
    self.r2data1=db['r2data1']
    # Default return value
    p = self.r2data1
    db.close()
    return Binary(pickle.dumps(p))

  # Insert something into the HT
  def r2putd3(self, value):
    # Remove expired entries
    db=shelve.open(f)
    value = pickle.loads(value.data)
    self.r2data1 = value
    self.r2data = value
    db['r2data']=self.r2data
    db['r2data1']=self.r2data1
    db.close()
    return Binary(pickle.dumps(True))





  def r2cgetd(self, key):
    # Default return value
    db = shelve.open(f)
    self.r2cdata=db['r2cdata']
    # If the key is in the data structure, return properly formated results
    key = key.data
    print (key)
    if key in self.r2cdata:
        print("True")
    else:
        print("false")
    db.close()
    return Binary(pickle.dumps(self.r2cdata[key]))

  # Insert something into the HT
  def r2cputd(self, key, value):
    # Remove expired entries
    db=shelve.open(f)
    value = pickle.loads(value.data)
    self.r2cdata[key.data] = value
    self.r2cdata1[key.data] = value
    db['r2cdata']=self.r2cdata
    db['r2cdata1']=self.r2cdata1
    db.close()
    return Binary(pickle.dumps(True))



  def r2cgetd3(self):
    # Default return value
    db = shelve.open(f)
    self.r2cdata1=db['r2cdata1']
    p = self.r2cdata1
    db.close()
    return Binary(pickle.dumps(p))

  # Insert something into the HT
  def r2cputd3(self, value):
    # Remove expired entries
    db=shelve.open(f)
    value = pickle.loads(value.data)
    self.r2cdata1 = value
    self.r2cdata = value
    db['r2cdata']=self.r2cdata
    db['r2cdata1']=self.r2cdata1
    db.close()
    return Binary(pickle.dumps(True))





  def remove(self, path):
    path = pickle.loads(path.data)
    del self.data[path]
    return Binary(pickle.dumps(True))
    
  # Load contents from a file
  def read_file(self, filename):
    f = open(filename.data, "rb")
    self.data = pickle.load(f)
    f.close()
    return True

  # Write contents to a file
  def write_file(self, filename):
    f = open(filename.data, "wb")
    pickle.dump(self.data, f)
    f.close()
    return True

  # Print the contents of the hashtable
  def print_content(self):
    print self.data
    return True

def main():
  global xy
  xy=int(argv[1])
  port = int(argv[xy+2])
  global f
  f='data store'+str(xy)
  serve(port)

# Start the xmlrpc server
def serve(port):
  file_server = SimpleXMLRPCServer.SimpleXMLRPCServer(('', port))
  file_server.register_introspection_functions()
  sht = SimpleHT()
  file_server.register_function(sht.getd)
  file_server.register_function(sht.putd)
  file_server.register_function(sht.getd3)
  file_server.register_function(sht.putd3)
  file_server.register_function(sht.r1getd)
  file_server.register_function(sht.r1putd)
  file_server.register_function(sht.r1getd3)
  file_server.register_function(sht.r1putd3)
  file_server.register_function(sht.r2getd)
  file_server.register_function(sht.r2putd)
  file_server.register_function(sht.r2getd3)
  file_server.register_function(sht.r2putd3)
  
  file_server.register_function(sht.cgetd)
  file_server.register_function(sht.cputd)
  file_server.register_function(sht.cgetd3)
  file_server.register_function(sht.cputd3)
  file_server.register_function(sht.r1cgetd)
  file_server.register_function(sht.r1cputd)
  file_server.register_function(sht.r1cgetd3)
  file_server.register_function(sht.r1cputd3)
  file_server.register_function(sht.r2cgetd)
  file_server.register_function(sht.r2cputd)
  file_server.register_function(sht.r2cgetd3)
  file_server.register_function(sht.r2cputd3)





  file_server.register_function(sht.corruptd)
  file_server.register_function(sht.checkprd)
  file_server.register_function(sht.remove)
  file_server.register_function(sht.print_content)
  file_server.register_function(sht.read_file)
  file_server.register_function(sht.write_file)
  file_server.serve_forever()

# Execute the xmlrpc in a thread ... needed for testing
class serve_thread:
  def __call__(self, port):
    serve(port)

# Wrapper functions so the tests don't need to be concerned about Binary blobs
class Helper:
  def __init__(self, caller):
    self.caller = caller

  def put(self, key, val, ttl):
    return self.caller.put(Binary(key), Binary(val), ttl)

  def get(self, key):
    return self.caller.get(Binary(key))

  def write_file(self, filename):
    return self.caller.write_file(Binary(filename))

  def read_file(self, filename):
    return self.caller.read_file(Binary(filename))

class SimpleHTTest(unittest.TestCase):
  def test_direct(self):
    helper = Helper(SimpleHT())
    self.assertEqual(helper.get("test"), {}, "DHT isn't empty")
    self.assertTrue(helper.put("test", "test", 10000), "Failed to put")
    self.assertEqual(helper.get("test")["value"], "test", "Failed to perform single get")
    self.assertTrue(helper.put("test", "test0", 10000), "Failed to put")
    self.assertEqual(helper.get("test")["value"], "test0", "Failed to perform overwrite")
    self.assertTrue(helper.put("test", "test1", 2), "Failed to put" )
    self.assertEqual(helper.get("test")["value"], "test1", "Failed to perform overwrite")
    time.sleep(2)
    self.assertEqual(helper.get("test"), {}, "Failed expire")
    self.assertTrue(helper.put("test", "test2", 20000))
    self.assertEqual(helper.get("test")["value"], "test2", "Store new value")

    helper.write_file("test")
    helper = Helper(SimpleHT())

    self.assertEqual(helper.get("test"), {}, "DHT isn't empty")
    helper.read_file("test")
    self.assertEqual(helper.get("test")["value"], "test2", "Load unsuccessful!")
    self.assertTrue(helper.put("some_other_key", "some_value", 10000))
    self.assertEqual(helper.get("some_other_key")["value"], "some_value", "Different keys")
    self.assertEqual(helper.get("test")["value"], "test2", "Verify contents")

  # Test via RPC
  def test_xmlrpc(self):
    output_thread = threading.Thread(target=serve_thread(), args=(12345, ))
    output_thread.setDaemon(True)
    output_thread.start()

    time.sleep(1)
    helper = Helper(xmlrpclib.Server("http://127.0.0.1:51234"))
    self.assertEqual(helper.get("test"), {}, "DHT isn't empty")
    self.assertTrue(helper.put("test", "test", 10000), "Failed to put")
    self.assertEqual(helper.get("test")["value"], "test", "Failed to perform single get")
    self.assertTrue(helper.put("test", "test0", 10000), "Failed to put")
    self.assertEqual(helper.get("test")["value"], "test0", "Failed to perform overwrite")
    self.assertTrue(helper.put("test", "test1", 2), "Failed to put" )
    self.assertEqual(helper.get("test")["value"], "test1", "Failed to perform overwrite")
    time.sleep(2)
    self.assertEqual(helper.get("test"), {}, "Failed expire")
    self.assertTrue(helper.put("test", "test2", 20000))
    self.assertEqual(helper.get("test")["value"], "test2", "Store new value")

if __name__ == "__main__":
    main()
