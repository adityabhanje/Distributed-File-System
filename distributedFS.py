#!/usr/bin/env python

from __future__ import print_function, absolute_import, division
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn
import logging, xmlrpclib, pickle
from xmlrpclib import Binary
import os, hashlib,socket, time

from collections import defaultdict
from errno import ENOENT, ENOTEMPTY
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time
from time import sleep

if not hasattr(__builtins__, 'bytes'):
    bytes = str

def checkf(server,s):
    return pickle.loads((servers[s].checkprd()).data)  
##################ogi copy meta##################
def putval1(mserver,key,val):
    return mserver.putd(Binary(key),Binary(pickle.dumps(val)))           

def getval1(mserver,key):
    return pickle.loads((mserver.getd(Binary(key))).data)

##################ogi copy data(By key only)##################
def putval2(dserver,key,val,s):
    return servers[s].putd(Binary(key),Binary(pickle.dumps(val)))          

def getval2(dserver,key,s):
    return pickle.loads((servers[s].getd(Binary(key))).data)


##################ogi copy data(entire)##################
def getval3(dserver,s):
    return pickle.loads((servers[s].getd3()).data)                  

def putval3(dserver,val,s):
    return servers[s].putd3(Binary(pickle.dumps(val)))         

#################replica 1 copy data(By key only)##################
def rep1putval2(dserver,key,val,s):
    return servers[s].r1putd(Binary(key),Binary(pickle.dumps(val)))          

def rep1getval2(dserver,key,s):
    return pickle.loads((servers[s].r1getd(Binary(key))).data)


##################replica1 copy data(entire)##################
def rep1getval3(dserver,s):
    return pickle.loads((servers[s].r1getd3()).data)                  

def rep1putval3(dserver,val,s):
    return servers[s].r1putd3(Binary(pickle.dumps(val)))         


#################replica 2 copy data(By key only)##################
def rep2putval2(dserver,key,val,s):
    return servers[s].r2putd(Binary(key),Binary(pickle.dumps(val)))          

def rep2getval2(dserver,key,s):
    return pickle.loads((servers[s].r2getd(Binary(key))).data)


##################replica2 copy data(entire)##################
def rep2getval3(dserver,s):
    return pickle.loads((servers[s].r2getd3()).data)                  

def rep2putval3(dserver,val,s):
    return servers[s].r2putd3(Binary(pickle.dumps(val)))         


#*****************************************************************************************************************
##################ogi copy checksum(By key only)##################
def cputval2(dserver,key,val,s):
    return servers[s].cputd(Binary(key),Binary(pickle.dumps(val)))          

def cgetval2(dserver,key,s):
    return pickle.loads((servers[s].cgetd(Binary(key))).data)


##################ogi copy checksum(entire)##################
def cgetval3(dserver,s):
    return pickle.loads((servers[s].cgetd3()).data)                  

def cputval3(dserver,val,s):
    return servers[s].cputd3(Binary(pickle.dumps(val)))         

#################replica 1 copy checksum(By key only)##################
def crep1putval2(dserver,key,val,s):
    return servers[s].r1cputd(Binary(key),Binary(pickle.dumps(val)))          

def crep1getval2(dserver,key,s):
    return pickle.loads((servers[s].r1cgetd(Binary(key))).data)


##################replica1 copy checksum(entire)##################
def crep1getval3(dserver,s):
    return pickle.loads((servers[s].r1cgetd3()).data)                  

def crep1putval3(dserver,val,s):
    return servers[s].r1cputd3(Binary(pickle.dumps(val)))         


#################replica 2 copy checksum(By key only)##################
def crep2putval2(dserver,key,val,s):
    return servers[s].r2cputd(Binary(key),Binary(pickle.dumps(val)))          

def crep2getval2(dserver,key,s):
    return pickle.loads((servers[s].r2cgetd(Binary(key))).data)


##################replica2 copy checksum(entire)##################
def crep2getval3(dserver,s):
    return pickle.loads((servers[s].r2cgetd3()).data)                  

def crep2putval3(dserver,val,s):
    return servers[s].r2cputd3(Binary(pickle.dumps(val)))         


def corruptf(dserver,path,s):
    return servers[s].corruptd(Binary(pickle.dumps(path)))         









def servstat(sport):
    a = xmlrpclib.ServerProxy("http://localhost:"+str(int(sport)))

    try:
       	a._()   # Call a fictive method.
    except xmlrpclib.Fault:
        # connected to the server and the method doesn't exist which is expected.
        pass
    except socket.error:
        # Not connected ; socket error mean that the service is unreachable.
        return False, None
	    	# Just in case the method is registered in the XmlRPC server
    return True, a

#servers=[]
#readserv=[]
#pnum=[]
#servercount=0
def writecheck():
    global servers, servercount
    servers=[None]*servercount
    global pnum,servercount
    conn=[0]*servercount
    salive=[0]*servercount
    while 1:
        i=0
        servtotal=0
        while i<servercount:
            conn[i],servers[i]= servstat(pnum[i])
            i+=1
        i=0
        while i<servercount:
            if conn[i]==True:
                print ("server"+str(i)+" is alive")
                salive[i]=1
            if conn[i]!=True:
                print("Waiting for server"+str(i))
            i+=1
        for every in salive:
            servtotal+=every 
        if servtotal==servercount:
            print ("All servers are ready for contact")
	    if xyz==1:
		dbrest()
            break

#------------------------------------------------------ persistence starts -----------------------------------
xyz=0
def dbrest():
    global servercount
    i=0
    while i<servercount:
    	dbstat=checkf(servers[i], i)
	if not dbstat:
	    org=i
	    rep1=(org+1)%servercount
	    rep2=(org-1)%servercount
	    d=rep1getval3(servers[rep1], rep1)
	    d1=rep2getval3(servers[rep1], rep1)
	    d2=rep1getval3(servers[rep2], rep2)
	    putval3(servers[org], d, org)
	    rep1putval3(servers[org], d1, org)
	    rep2putval3(servers[org], d2, org)
	    d=crep1getval3(servers[rep1], rep1)
	    d1=crep2getval3(servers[rep1], rep1)
	    d2=crep1getval3(servers[rep2], rep2)
	    cputval3(servers[org], d, org)
	    crep1putval3(servers[org], d1, org)
	    crep2putval3(servers[org], d2, org)
	i+=1
	    
#----------------------------------------------------------------- presistence ends------------------        

def checkread():
    global servers
    global pnum,servercount, xyz
    global serv_read
    global readserv
    readserv=[0]*servercount 
    conn=[None]*servercount
    while 1:
        i=0
        servtotal=0
        while i<servercount:
            conn[i],servers[i]= servstat(pnum[i])
            i+=1
        i=0
        while i<servercount:
            if conn[i]==True:
                print ("server"+str(i)+" is alive")
                readserv[i]=1
    		check=checkf(servers[i], i)
		if not check:
			dbrest()
            if conn[i]!=True:
                print("Waiting for server"+str(i))
            i+=1
        for every in readserv:
            servtotal+=every
	
        if servtotal>=(servercount-2):
            print ("atleast "+ str(servtotal) +" servers are ready for contact")
            break
    return servtotal
    


class Memory(LoggingMixIn, Operations):

    def fn1(self,s):
        L=[]
        while s!="":
            L.append(s[0:8])
            s=s[8:]
        return L


    def fn2(self,l):
        s=''.join(l)
        return s

    def mysplit(self,path):                      #Fn. to obtain child and parent path
        cout=path.count('/')  
        if cout==1:
            parent="/"
            child=path[path.find('/')+1:]
        else:
            parent=path[:path.rfind('/')]
            child=path[path.rfind('/')+1:]
        return parent,child
    
    def __init__(self):
        self.meta = {}        
        self.meta['files'] = {}
	self.meta['child'] = defaultdict(list)
	self.fd = 0
        now = time()
        self.meta['files']['/'] = dict(st_mode=(S_IFDIR | 0o755), st_ctime=now,
                               st_mtime=now, st_atime=now, st_nlink=2)
        putval1(server_meta,"files",self.meta['files'])
        putval1(server_meta,"child",self.meta['child'])

    def chmod(self, path, mode):
        x1 = getval1(server_meta,"files")                      
        x1[path]['st_mode'] &= 0o770000
        x1[path]['st_mode'] |= mode
        putval1(server_meta,"files",x1)
        return 0


    def chown(self, path, uid, gid):
        x1 = getval1(server_meta,"files")
        x1[path]['st_uid'] = uid
        x1[path]['st_gid'] = gid
        putval1(server_meta,"files",x1)

    def create(self, path, mode):
	global servers, servercount, xyz
        writecheck()
	xyz=1
        x1 = getval1(server_meta,"files")
        x2 = getval1(server_meta,"child")
        x1[path] = dict(st_mode=(S_IFREG | mode), st_nlink=1,
                                st_size=0, st_ctime=time(), st_mtime=time(),
                                st_atime=time())
	i=0	
	while(i<servercount):
	        putval2(servers[i], path , [],i)
	        rep1putval2(servers[i], path , [],i)
	        rep2putval2(servers[i], path , [],i)
	        cputval2(servers[i], path , [],i)
	        crep1putval2(servers[i], path , [],i)
	        crep2putval2(servers[i], path , [],i)
		i+=1
        ppath,cpath=self.mysplit(path)
	x2[ppath].append(cpath)            
        self.fd += 1
	putval1(server_meta,"files",x1)
	putval1(server_meta,"child",x2)
        return self.fd

    def getattr(self, path, fh=None):
        x1 = getval1(server_meta,"files")
        if path not in x1:
            raise FuseOSError(ENOENT)
        return x1[path]

    def getxattr(self, path, name, position=0):
        x1 = getval1(server_meta,"files")
        attrs = x1[path].get('attrs', {})
        try:
            return attrs[name]
        except KeyError:
            return ''       

    def listxattr(self, path):
        x1 = getval1(server_meta,"files")
        attrs = x1[path].get('attrs', {})
        return attrs.keys()

    def mkdir(self, path, mode):
        x1 = getval1(server_meta,"files")
        x2 = getval1(server_meta,"child")
        x1[path] = dict(st_mode=(S_IFDIR | mode), st_nlink=2,
                                st_size=0, st_ctime=time(), st_mtime=time(),
                                st_atime=time())
        ppath,cpath=self.mysplit(path)
        x2[ppath].append(cpath)                  
        x2[path]=[]                             
        x1[ppath]['st_nlink'] += 1
        putval1(server_meta,"files",x1)
        putval1(server_meta,"child",x2)


    def open(self, path, flags):
        self.fd += 1
	print("--------------------------------------- chutiya ------------------------")
        return self.fd
#----------------------------------------------read starts -----------------------------------------------

    def read(self, path, size, offset, fh):
	global readserv, servercount
        servtotal=checkread()
	if servtotal>=(servercount-2):
	    org=hash(path)%servercount
	    rep1=(org+1)%servercount
	    rep2=(org+2)%servercount
            a1=path
            x1 = getval1(server_meta,"files")
            l= (x1[path]['st_size'])
	    if l%8 == 0:
	        l=int(l/8)
	    else:
	    	l=int((l/8)+1)
	    pos=0
	    pos1=0
	    d=[]
	    data2=[]
	    while l!=0:
		f=0
		f1=0
		f2=0
		f3=0
		if readserv[org] == 1 and f==0:
			csum=cgetval2(servers[org],path,org)
			ecsum=csum[pos]
			check=getval2(servers[org], path,org)
			checkelem=check[pos]
                        if ecsum == hash(checkelem):
	    			data2=checkelem
				f=1
				f1=1
		if readserv[rep1] == 1 and f==0:
			rep1csum=crep1getval2(servers[rep1],path,rep1)
			erep1csum=rep1csum[pos]
			rep1check=rep1getval2(servers[rep1], path,rep1)
                        erep1checkelem=rep1check[pos]
			if erep1csum == hash(erep1checkelem):
	    			data2=erep1checkelem
				f=1
				f2=1
		if readserv[rep2] == 1 and f==0:
			rep2csum=crep2getval2(servers[rep2],path,rep2)
			erep2csum=rep2csum[pos]
			rep2check=rep2getval2(servers[rep2], path,rep2)
			erep2checkelem=rep2check[pos]
			if erep2csum == hash(erep2checkelem):
	    			data2=erep2checkelem
				f=1
				f3=1
#############################got the correct data,now start putting in the wrong servers#########################3

		if readserv[org] == 1 and readserv[rep1] == 1:
			if f1==0 and f2==0 and f3==1:
                                check[pos]=data2
			        csum[pos]=hash(data2)
                                rep1check[pos]=data2
			        rep1csum[pos]=hash(data2)
				cputval2(servers[org],path,csum,org)
				crep1putval2(servers[rep1],path,rep1csum,rep1)
				putval2(servers[org],path,data2,org)
				rep1putval2(servers[rep1],path,data2,rep1)
			if f1==0 and f2==1:
                                check[pos]=data2
			        csum[pos]=hash(data2)
				cputval2(servers[org],path,csum,org)
				putval2(servers[org],path,check,org)
		if readserv[org] == 1 and readserv[rep1] == 0:
			if f1==0 and f3==1:
				check[pos]=data2
			        csum[pos]=hash(data2)
				cputval2(servers[org],path,csum,org)
				putval2(servers[org],path,check,org)
		if readserv[org] == 0 and readserv[rep1] == 1:
			if f2==0 and f3==1:
                                rep1check[pos]=data2
			        rep1csum[pos]=hash(data2)
				crep1putval2(servers[rep1],path,rep1csum,rep1)
				rep1putval2(servers[rep1],path,rep1check,rep1)
	  

	    	d.append(data2)
	    	pos1=(pos1+1)%servercount
	    	if pos1 == 0:
	    		pos+=1
	        org=(org+1)%servercount
		rep1=(rep1+1)%servercount
		rep2=(rep2+1)%servercount
	    	l-=1
            str1 = d[int(offset/8):int(((offset+size)/8) + 1)]    
            str1 = self.fn2(str1)
            str1 = str1[offset%8 : ] + str1[-((offset+size)%8):]
            return  str1                                            
#---------------------------------------------read ends -------------------------------------------------

    def readdir(self, path, fh):
        x2 = getval1(server_meta,"child")
        ppath,cpath=self.mysplit(path)
        for x in x2: 
            if x==path: 
                return ['.', '..']+x2[path]     

    def readlink(self, path):
	global readserv, servercount
        servtotal=checkread()
	if servtotal>=(servercount-2):
	    org=hash(path)%servercount
	    rep1=(org+1)%servercount
	    rep2=(org+2)%servercount
            d = getval2(servers[org], path,org)
        return self.fn2(d)

    def removexattr(self, path, name):
        x1 = getval1(server_meta,"files")
        attrs = x1[path].get('attrs', {})
        try:
            del attrs[name]
            putval1(server_meta,"files",x1)
        except KeyError:
            pass        



    def rename(self, old, new):
	global servers, servercount
        writecheck()
        x1 = getval1(server_meta,"files")
        x2 = getval1(server_meta,"child")
	org = hash(old)%servercount
	rep1 = (org+1)%servercount
	rep2 = (org+2)%servercount
	y = hash(new)%servercount
	y1 = (y+1)%servercount
	y2 = (y+2)%servercount
	d = getval3(servers[org],org)
	ppathold,cpathold=self.mysplit(old)
	ppathnew,cpathnew=self.mysplit(new)
	if cpathold == cpathnew:
	   	x2[ppathnew].append(cpathnew)
	   	x2[ppathold].remove(cpathold)
	    	for i in x2.keys():
			listnew=i.split('/')		
			if cpathold in listnew:
				a_po=i.find(cpathold)
				print(a_po)
				attach=i[a_po:]
				print(attach)
				newkey=ppathnew + '/' + attach
				print(newkey)
				print(x2)
				x2[newkey]=x2.pop(i)
				x1[newkey]=x1.pop(i)
				print(x2)

		for i in d.keys():
			listnew=i.split('/')
			if cpathold in listnew:
				a_po=i.find(cpathold)
				print(a_po)
				attach=i[a_po:]
				print(attach)
				newkey=ppathnew + '/' + attach
		   		x1[newkey]=x1.pop(i)
				n=0
				org = hash(i)%servercount
				rep1 = (org+1)%servercount
				rep2 = (org+2)%servercount
				y = hash(newkey)%servercount
				y1 = (y+1)%servercount
				y2 = (y+2)%servercount	
				while(n<servercount):
					t=[]
					t1=[]
					t2=[]
					t3=[]
					t4=[]
					t5=[]
	        			d = getval3(servers[org], org)
	        			d1 = rep1getval3(servers[rep1], rep1)
	        			d2 = rep2getval3(servers[rep2], rep2)
	        			d3 = cgetval3(servers[org],org)
	        			d4 = crep1getval3(servers[rep1],rep1)
	        			d5 = crep2getval3(servers[rep2],rep2)
					t = d[i]
					t1 = d1[i]
					t2 = d2[i]
					t3 = d3[i]
					t4 = d4[i]
					t5 = d5[i]
					d.pop(i)
					d1.pop(i)
					d2.pop(i)
					d3.pop(i)
					d4.pop(i)
					d5.pop(i)
					putval3(servers[org], d,org)
	        			rep1putval3(servers[rep1], d1,rep1)
	        			rep2putval3(servers[rep2], d2,rep2)
	        			cputval3(servers[org], d3,org)
	        			crep1putval3(servers[rep1], d4,rep1)
	        			crep2putval3(servers[rep2], d5,rep2)

	        			d = getval3(servers[y], y)
	        			d1 = rep1getval3(servers[y1], y1)
	        			d2 = rep2getval3(servers[y2], y2)
	        			d3 = cgetval3(servers[y],y)
	        			d4 = crep1getval3(servers[y1],y1)
	        			d5 = crep2getval3(servers[y2],y2)
					d[newkey]=t
					d1[newkey]=t1
					d2[newkey]=t2
					d3[newkey]=t3
					d4[newkey]=t4
					d5[newkey]=t5
					putval3(servers[y], d,y)
	        			rep1putval3(servers[y1], d1,y1)
	        			rep2putval3(servers[y2], d2,y2)
	        			cputval3(servers[y], d3,y)
	        			crep1putval3(servers[y1], d4,y1)
	        			crep2putval3(servers[y2], d5,y2)
					n+=1
					org=(org+1)%servercount
					rep1=(rep1+1)%servercount
					rep2=(rep2+1)%servercount
					y=(y+1)%servercount
					y1=(y1+1)%servercount
					y2=(y2+1)%servercount					

		
	else:
		for i in d.keys():		
			oldkey=i
			listnew=i.split('/')
			for n,k in enumerate(listnew):
				if k==cpathold:
					listnew[n]=cpathnew                                                         
       					newkey = "/".join(listnew)
					x1[newkey] =x1.pop(oldkey)
					n=0
					org = hash(oldkey)%servercount
					rep1 = (org+1)%servercount
					rep2 = (org+2)%servercount
					y = hash(newkey)%servercount
					y1 = (y+1)%servercount
					y2 = (y+2)%servercount	
					while(n<servercount):
						t=[]
						t1=[]
						t2=[]
						t3=[]
						t4=[]
						t5=[]
	        				d = getval3(servers[org], org)
	        				d1 = rep1getval3(servers[rep1], rep1)
	        				d2 = rep2getval3(servers[rep2], rep2)
	        				d3 = cgetval3(servers[org],org)
	        				d4 = crep1getval3(servers[rep1],rep1)
	        				d5 = crep2getval3(servers[rep2],rep2)
						t = d[oldkey]
						t1 = d1[oldkey]
						t2 = d2[oldkey]
						t3 = d3[oldkey]
						t4 = d4[oldkey]
						t5 = d5[oldkey]
						d.pop(oldkey)
						d1.pop(oldkey)
						d2.pop(oldkey)
						d3.pop(oldkey)
						d4.pop(oldkey)
						d5.pop(oldkey)
						putval3(servers[org], d,org)
	        				rep1putval3(servers[rep1], d1,rep1)
	        				rep2putval3(servers[rep2], d2,rep2)
	        				cputval3(servers[org], d3,org)
	        				crep1putval3(servers[rep1], d4,rep1)
	        				crep2putval3(servers[rep2], d5,rep2)
	
	        				d = getval3(servers[y], y)
	        				d1 = rep1getval3(servers[y1], y1)
	        				d2 = rep2getval3(servers[y2], y2)
	        				d3 = cgetval3(servers[y],y)
	        				d4 = crep1getval3(servers[y1],y1)
	        				d5 = crep2getval3(servers[y2],y2)
						d[newkey]=t
						d1[newkey]=t1
						d2[newkey]=t2
						d3[newkey]=t3
						d4[newkey]=t4
						d5[newkey]=t5	
						putval3(servers[y], d,y)
	        				rep1putval3(servers[y1], d1,y1)
	        				rep2putval3(servers[y2], d2,y2)
	        				cputval3(servers[y], d3,y)
	        				crep1putval3(servers[y1], d4,y1)
	        				crep2putval3(servers[y2], d5,y2)
						n+=1
						org=(org+1)%servercount
						rep1=(rep1+1)%servercount
						rep2=(rep2+1)%servercount
						y=(y+1)%servercount
						y1=(y1+1)%servercount
						y2=(y2+1)%servercount

		for i in x2.keys():		
			oldkey=i
			listnew=i.split('/')
			for n,k in enumerate(listnew):
				if k==cpathold:
					listnew[n]=cpathnew                                                         
       					newkey = "/".join(listnew)
					x2[newkey]=x2.pop(oldkey)
					x1[newkey] =x1.pop(oldkey)

		pnon,cnon=self.mysplit(new)
		x2[pnon].append(cpathnew)			        
		x2[pnon].remove(cpathold)

	putval1(server_meta,"files",x1)
        putval1(server_meta,"child",x2)
	

#---------------------------------------------------------------------- rename ends --------------------------------------------------

    def rmdir(self, path):
	global servers, servercount
        writecheck() 
        x1 = getval1(server_meta,"files")
        x2 = getval1(server_meta,"child")
	print("--------------------------------------------")
	print(x1[path])
	if x1[path]['st_nlink'] <= 2:
            ppath,cpath=self.mysplit(path)
	    x1.pop(path)
            x2.pop(path)
            x2[ppath].remove(cpath)
            x1[ppath]['st_nlink'] -= 1
            putval1(server_meta,"files",x1)
            putval1(server_meta,"child",x2)
	else:
	    raise FuseOSError(ENOTEMPTY)	
		
    def setxattr(self, path, name, value, options, position=0):
        x1 = getval1(server_meta,"files")
        attrs = x1[path].setdefault('attrs', {})
        attrs[name] = value
        putval1(server_meta,"files",x1)


    def statfs(self, path):
        return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)

    def symlink(self, target, source):
	global servers, servercount
        writecheck() 
        x1 = getval1(server_meta,"files")
	x2 = getval1(server_meta,"child")	
        x1[target] = dict(st_mode=(S_IFLNK | 0o777), st_nlink=1, st_size=len(source))
	ppath,cpath=self.mysplit(target)
	x2[ppath].append(cpath)
        putval1(server_meta,"files",x1)
	putval1(server_meta,"child",x2)
	org=hash(target)%servercount
	rep1=(org+1)%servercount
	rep2=(org+2)%servercount
	d = getval2(servers[org], target,org)
	d = self.fn1(source)
        putval2(servers[org], target, d,org)
        rep1putval2(servers[rep1], target, d,rep1)
        rep2putval2(servers[rep2], target, d,rep2)
	cd=hash(source)
        cputval2(servers[org], target, d,org)
        crep1putval2(servers[rep1], target, d,rep1)
        crep2putval2(servers[rep2], target, d,rep2)
	

#------------------------------------------------------- truncate --------------------------------

    def truncate(self, path, length, fh=None): 
	global servers, servercount
        writecheck() 
#----------get the complete data------------------
	org=hash(path)%servercount
	rep1=(org+1)%servercount
	rep2=(org+2)%servercount
        x1 = getval1(server_meta,"files") 
        l= (x1[path]['st_size'])
	if length > l:
            x1[path]['st_size'] = l
            putval1(server_meta,"files",x1)
	else:
	    if l%8 == 0:
	        l=int(l/8)
	    else:
	    	l=int((l/8)+1)
	    pos=0
	    pos1=0
	    d=[]
	    rep1d=[]
	    rep2d=[]
	    while l!=0:
	    	data2=getval2(servers[org], path,org)
	    	rep1data2=rep1getval2(servers[rep1], path,rep1)
	    	rep2data2=rep2getval2(servers[rep2], path,rep2)
	    	d.append(data2[pos])
	    	rep1d.append(rep1data2[pos])
	    	rep2d.append(rep2data2[pos])
	    	pos1=(pos1+1)%servercount
	    	if pos1 == 0:
	    		pos+=1
	    	org=(org+1)%servercount
	    	rep1=(rep1+1)%servercount
	    	rep2=(rep2+1)%servercount
	    	l-=1
            str1 = d
            str1 = self.fn2(str1)
	    str1 = str1[0:length]
            rep1str1 = rep1d
            rep1str1 = self.fn2(rep1str1)
	    rep1str1 = rep1str1[0:length]
            rep2str1 = rep2d
            rep2str1 = self.fn2(rep2str1)
	    rep2str1 = rep2str1[0:length]	
#------------------------------------delete it completely starts------------------------    
	    i=0
            org=hash(path)%servercount
            rep1=(org+1)%servercount
            rep2=(org+2)%servercount
	    while i!=servercount:
	    	d = getval3(servers[org],org)
		cd = cgetval3(servers[org],org)
		print("---------------------++++++++++========================////////////////////",cd)
	    	if path not in d.keys():
	    		i+=1
                        org=(org+1)%servercount
			continue
             	d.pop(path)
	    	putval3(servers[org], d,org)
             	cd.pop(path)
	    	cputval3(servers[org], cd,org)
	    	i+=1
	    	org=(org+1)%servercount
	    i=0
	    while i!=servercount:
	    	rep1d = rep1getval3(servers[rep1],rep1)
	    	crep1d = crep1getval3(servers[rep1],rep1)
	    	if path not in rep1d.keys():
	    		i+=1
                        rep1=(rep1+1)%servercount
			continue
            	rep1d.pop(path)
	    	rep1putval3(servers[rep1], rep1d,rep1)
            	crep1d.pop(path)
	    	crep1putval3(servers[rep1], crep1d,rep1)
	    	i+=1
	    	rep1=(rep1+1)%servercount
	    i=0
	    while i!=servercount:
	    	rep2d = rep2getval3(servers[rep2],rep2)
	    	crep2d = crep2getval3(servers[rep2],rep2)
	    	if path not in rep2d.keys():
	    		i+=1
                        rep2=(rep2+1)%servercount
			continue
            	rep2d.pop(path)
	    	rep2putval3(servers[rep2], rep2d,rep2)
            	crep2d.pop(path)
	    	crep2putval3(servers[rep2], crep2d,rep2)
	    	i+=1
	    	rep2=(rep2+1)%servercount
#--------------------------------write again starts-----------------
	    l=len(str1)
	    if l%8 == 0:
	    	l=int(l/8)
	    else:
	    	l=int((l/8)+1)
	    data1=self.fn1(str1)
            org=hash(path)%servercount
	    i=0
	    while l!=0:
	    	d = getval2(servers[org],path,org)
            	d.append(data1[i])
	    	putval2(servers[org],path,d,org)
	    	cd = cgetval2(servers[org],path,org)
            	cd.append(hash(data1[i]))
	    	cputval2(servers[org],path,cd,org)
	    	org=(org+1)%servercount
	    	i+=1
	    	l-=1
#----------------------------starting rep1------------------

	    rep1l=len(rep1str1)
	    if rep1l%8 == 0:
	    	rep1l=int(rep1l/8)
	    else:
	    	rep1l=int((rep1l/8)+1)
	    r1data1=self.fn1(rep1str1)
            org=hash(path)%servercount
	    rep1=(org+1)%servercount
	    i=0
	    while rep1l!=0:
	    	rep1d = rep1getval2(servers[rep1],path,rep1)
            	rep1d.append(r1data1[i])
	    	rep1putval2(servers[rep1],path,rep1d,rep1)
	    	crep1d = crep1getval2(servers[rep1],path,rep1)
            	crep1d.append(hash(r1data1[i]))
	    	crep1putval2(servers[rep1],path,crep1d,rep1)
	    	rep1=(rep1+1)%servercount
	    	i+=1
	    	rep1l-=1
#------------------------starting rep 2=--------------------

	    rep2l=len(rep2str1)
	    if rep2l%8 == 0:
	    	rep2l=int(rep2l/8)
	    else:
	    	rep2l=int((rep2l/8)+1)
	    r2data1=self.fn1(rep2str1)
            org=hash(path)%servercount
	    rep2=(org+2)%servercount
	    i=0
	    while rep2l!=0:
	    	rep2d = rep2getval2(servers[rep2],path,rep2)
            	rep2d.append(r2data1[i])
	    	rep2putval2(servers[rep2],path,rep2d,rep2)
	    	crep2d = crep2getval2(servers[rep2],path,rep2)
            	crep2d.append(hash(r2data1[i]))
	    	crep2putval2(servers[rep2],path,crep2d,rep2)
	    	rep2=(rep2+1)%servercount
	    	i+=1
	    	rep2l-=1
	    
            x1[path]['st_size'] = length
            putval1(server_meta,"files",x1)

#------------------------------------------------------- truncate ends -------------------------------	


#-------------------------------------------------------remove file---------------------------------    
    def unlink(self, path):
	global servers, servercount
        writecheck() 
        x1 = getval1(server_meta,"files")
        x2 = getval1(server_meta,"child")
	x1.pop(path)
        ppath,cpath=self.mysplit(path)
	x2[ppath].remove(cpath)
        putval1(server_meta,"files",x1)
        putval1(server_meta,"child",x2)
        org=hash(path)%servercount
	rep1=(org+1)%servercount
	rep2=(org+2)%servercount
	i=0
	while i!=servercount:
	    d = getval3(servers[org],org)
	    rep1d = rep1getval3(servers[rep1],rep1)
	    rep2d = rep2getval3(servers[rep2],rep2)
            d.pop(path)
            rep1d.pop(path)
            rep2d.pop(path)
	    putval3(servers[org], d,org)
	    rep1putval3(servers[rep1], rep1d,rep1)
	    rep2putval3(servers[rep2], rep2d,rep2)
#================================== cs start ================================
	    cd = cgetval3(servers[org],org)
	    crep1d = crep1getval3(servers[rep1],rep1)
	    crep2d = crep2getval3(servers[rep2],rep2)
            cd.pop(path)
            crep1d.pop(path)
            crep2d.pop(path)
	    cputval3(servers[org], cd,org)
	    crep1putval3(servers[rep1], crep1d,rep1)
	    crep2putval3(servers[rep2], crep2d,rep2)
#================================ cs end =================================
	    i+=1
	    org=(org+1)%servercount
	    rep1=(rep1+1)%servercount
	    rep2=(rep2+1)%servercount
	
#--------------------------------------------------remove file ends----------------------------------------	

    def utimens(self, path, times=None):
        x1 = getval1(server_meta,"files")
        now = time()
        atime, mtime = times if times else (now, now)
        x1[path]['st_atime'] = atime
        x1[path]['st_mtime'] = mtime
        putval1(server_meta,"files",x1)

#------------------------------------------------ write ----------------------------------------------------------------------------------------

    def write(self, path, data, offset, fh): 
	global servers, servercount
        writecheck() 
        x1 = getval1(server_meta,"files") 
        l=x1[path]['st_size']
	if l%8 == 0:
	    l=int(l/8)
	else:
	    l=int((l/8)+1)

        if l==0: 
            data1=self.fn1(data)
	    org=hash(path)%servercount
	    rep1=(org+1)%servercount
	    rep2=(org+2)%servercount
	    length = len(data1)
	    i=0
	    while length!=0:
		d = getval2(servers[org],path,org)
		rep1d = rep1getval2(servers[rep1],path,rep1)
		rep2d = rep2getval2(servers[rep2],path,rep2)		
		d.append(data1[i])
		rep1d.append(data1[i])
		rep2d.append(data1[i])
		putval2(servers[org],path,d,org)
		rep1putval2(servers[rep1],path,rep1d,rep1)
		rep2putval2(servers[rep2],path,rep2d,rep2)
#==================================================== cs ===========================================================
		cs = cgetval2(servers[org],path,org)
		r1cs = crep1getval2(servers[rep1],path,rep1)
		r2cs = crep2getval2(servers[rep2],path,rep2)
		cs.append(hash(data1[i]))
		r1cs.append(hash(data1[i]))
		r2cs.append(hash(data1[i]))
		cputval2(servers[org],path,cs,org)
		crep1putval2(servers[rep1],path,r1cs,rep1)
		crep2putval2(servers[rep2],path,r2cs,rep2)
#==================================================== cs ends ============================================
		org=(org+1)%servercount
		rep1=(rep1+1)%servercount
		rep2=(rep2+1)%servercount
		i+=1
		length-=1
  
#+++++++++++++++++++++++++++++++++++++++++++++++++ append starts here +++++++++++++++++++++++++++++++++++++++++++++++++          

        else:    
            org=hash(path)%servercount
	    rep1=(org+1)%servercount
	    rep2=(org+2)%servercount
	    data1=self.fn1(data)
	    y=((l+org)%servercount)
	    y1=((l+rep1)%servercount)
	    y2=((l+rep2)%servercount)
	    ll=getval2(servers[y-1],path,(y-1))
	    ll1=rep1getval2(servers[y1-1],path,(y1-1))
	    ll2=rep2getval2(servers[y2-1],path,(y2-1))
	    cll=cgetval2(servers[y-1],path,(y-1))
	    cll1=crep1getval2(servers[y1-1],path,(y1-1))
	    cll2=crep2getval2(servers[y2-1],path,(y2-1))
            if len(ll[-1]) == 8:
                i=0
	        length = len(data1)
	    	while length!=0:
			d = getval2(servers[y],path,y)
			d.append(data1[i])
			putval2(servers[y],path,d,y)
#------------------------ cs for og -------------------------
			cd = cgetval2(servers[y],path,y)
			cd.append(hash(data1[i]))
			cputval2(servers[y],path,cd,y)
#------------------------ cs for og -------------------------
			y=(y+1)%servercount
			i+=1
			length-=1
            if len(ll1[-1]) == 8:
                i=0
	        length = len(data1)
	    	while length!=0:
			rep1d = rep1getval2(servers[y1],path,y1)
			rep1d.append(data1[i])
			rep1putval2(servers[y1],path,rep1d,y1)
#------------------------ cs for r1 -------------------------
			crep1d = crep1getval2(servers[y1],path,y1)
			crep1d.append(hash(data1[i]))
			crep1putval2(servers[y1],path,crep1d,y1)
#------------------------ cs for r1 -------------------------
			y1=(y1+1)%servercount
			i+=1
			length-=1
            if len(ll2[-1]) == 8:
                i=0
	        length = len(data1)
	    	while length!=0:
			rep2d = rep2getval2(servers[y2],path,y2)
			rep2d.append(data1[i])
			rep2putval2(servers[y2],path,rep2d,y2)
#------------------------ cs for r2 -------------------------
			crep2d = crep2getval2(servers[y2],path,y2)
			crep2d.append(hash(data1[i]))
			crep2putval2(servers[y2],path,crep2d,y2)
#------------------------ cs for r2 -------------------------
			y2=(y2+1)%servercount
			i+=1
			length-=1

            if len(ll[-1])!=8:
                i=0
		y=y-1
		data1 = self.fn1(ll[-1] + data)
		del ll[-1]
		putval2(servers[y],path,ll,y)
#------------------------ cs for og -------------------------
		del cll[-1]
		cputval2(servers[y],path,cll,y)
#------------------------ cs  -------------------------
		length=len(data1)
	    	while length!=0:
			d = getval2(servers[y],path,y)
			d.append(data1[i])
			putval2(servers[y],path,d,y)
#------------------------ cs  -------------------------
			cd = cgetval2(servers[y],path,y)
			cd.append(hash(data1[i]))
			cputval2(servers[y],path,cd,y)
#------------------------ cs for og -------------------------
			y=(y+1)%servercount
			i+=1
			length-=1

            if len(ll1[-1])!=8:
                i=0
		y1=y1-1
		data1 = self.fn1(ll1[-1] + data)
		del ll1[-1]
		rep1putval2(servers[y1],path,ll1,y1)
#------------------------ cs for r1 -------------------------
		del cll1[-1]
		crep1putval2(servers[y1],path,cll1,y1)
#------------------------ cs  -------------------------
		length=len(data1)
	    	while length!=0:
			rep1d = rep1getval2(servers[y1],path,y1)
			rep1d.append(data1[i])
			rep1putval2(servers[y1],path,rep1d,y1)
#------------------------ cs  -------------------------
			crep1d = crep1getval2(servers[y1],path,y1)
			crep1d.append(hash(data1[i]))
			crep1putval2(servers[y1],path,crep1d,y1)
#------------------------ cs for r1 -------------------------
			y1=(y1+1)%servercount
			i+=1
			length-=1

            if len(ll2[-1])!=8:
                i=0
		y2=y2-1
		data1 = self.fn1(ll2[-1] + data)
		del ll2[-1]
		rep2putval2(servers[y2],path,ll2,y2)
#------------------------ cs for r2 -------------------------
		del cll2[-1]
		crep2putval2(servers[y2],path,cll2,y2)
#------------------------ cs  -------------------------
		length=len(data1)
	    	while length!=0:
			rep2d = rep2getval2(servers[y2],path,y2)
			rep2d.append(data1[i])
			rep2putval2(servers[y2],path,rep2d,y2)
#------------------------ cs  -------------------------
			crep2d = crep2getval2(servers[y2],path,y2)
			crep2d.append(hash(data1[i]))
			crep2putval2(servers[y2],path,crep2d,y2)
#------------------------ cs for r2 -------------------------
			y2=(y2+1)%servercount
			i+=1
			length-=1

        x1[path]['st_size'] += len(data)              
        putval1(server_meta,"files",x1)
        return len(data)
#------------------------------------------------------------------------
#----------------------------------------------------write ends--------------------------------------------------------------------------------------

if __name__ == '__main__':
    mport=argv[2]
    '''dport0=argv[3]
    dport1=argv[4]
    dport2=argv[5]
    dport3=argv[6]'''
    global servercount
    servercount = (len(argv)-3)
    global pnum
    pnum=[]
    i=0
    while i<servercount:
        pnum.append(int(argv[i+3]))
        i+=1
    print (pnum)
    server_meta = xmlrpclib.ServerProxy("http://localhost:"+str(int(mport)))
    '''dserver0 = xmlrpclib.ServerProxy("http://localhost:"+str(int(dport0)))
    dserver1 = xmlrpclib.ServerProxy("http://localhost:"+str(int(dport1)))
    dserver2 = xmlrpclib.ServerProxy("http://localhost:"+str(int(dport2)))
    dserver3 = xmlrpclib.ServerProxy("http://localhost:"+str(int(dport3)))
    #server2 = xmlrpclib.ServerProxy("http://localhost:"+str(int(dport)))'''
    if len(argv) > 8:
        print('usage: %s <mountpoint>' % argv[0])
        exit(1)

    logging.basicConfig(level=logging.DEBUG)
    fuse = FUSE(Memory(), argv[1], foreground=True)
