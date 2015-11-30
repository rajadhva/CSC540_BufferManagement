package simpledb.server;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import simpledb.buffer.BufferMgr;
import simpledb.file.Block;
import simpledb.remote.RemoteDriver;
import simpledb.remote.RemoteDriverImpl;

public class Startup {
   public static void main(String args[]) throws Exception {
      // configure and initialize the database
      SimpleDB.init(args[0]);
      
      // create a registry specific for the server on the default port
      Registry reg = LocateRegistry.createRegistry(1099);
      
      // and post the server entry in it
      RemoteDriver d = new RemoteDriverImpl();
      reg.rebind("simpledb", d);
      
      System.out.println("database server ready");
      
      //----------------------Sample Run as per test given in moodle--------------------------
      //pin(1), pin(2), pin(3), read(1), write(2), write(3), unpin(1), unpin(2), unpin(3).
      
      BufferMgr bfr;
      bfr=SimpleDB.bufferMgr();
      
      Block blk1=new Block("Number1",1);
      Block blk2=new Block("Number2",2);
      Block blk3=new Block("Number3",3);
      Block blk4=new Block("Number4",4);
      
      bfr.pin(blk1);
      bfr.bufferMgr.getStatistics();
      bfr.pin(blk2);
      bfr.bufferMgr.getStatistics();
      bfr.pin(blk3);
      bfr.bufferMgr.getStatistics();
      
      int a = bfr.bufferMgr.bufferPoolMap.get(blk1).getBufferIndex();
      int b = bfr.bufferMgr.bufferPoolMap.get(blk2).getBufferIndex();
      int c = bfr.bufferMgr.bufferPoolMap.get(blk3).getBufferIndex();
    
      
      bfr.bufferMgr.bufferpool[a].getInt(1);
      bfr.bufferMgr.bufferpool[b].setInt(2, 1, 5, 10);    
      bfr.bufferMgr.bufferpool[c].setInt(2, 1, 15, 30);      
      
      bfr.unpin(bfr.bufferMgr.bufferpool[a]);
      bfr.bufferMgr.getStatistics();
      bfr.unpin(bfr.bufferMgr.bufferpool[b]);
      bfr.bufferMgr.getStatistics();
      bfr.unpin(bfr.bufferMgr.bufferpool[c]);
      bfr.bufferMgr.getStatistics();
      
      bfr.pin(blk4);
      bfr.bufferMgr.getStatistics(); 
      
   }
}
