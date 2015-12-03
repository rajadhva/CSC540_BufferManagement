package simpledb.server;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import simpledb.buffer.Buffer;
import simpledb.buffer.BufferMgr;
import simpledb.file.Block;
import simpledb.remote.RemoteDriver;
import simpledb.remote.RemoteDriverImpl;

public class Test{
   public static void main(String args[]) throws Exception {
      // configure and initialize the database
      SimpleDB.init(args[0]);
      
      // create a registry specific for the server on the default port
      Registry reg = LocateRegistry.createRegistry(1099);
      
      // and post the server entry in it
      RemoteDriver d = new RemoteDriverImpl();
      reg.rebind("simpledb", d);
      
      System.out.println("database server ready");
      
  //----------------------Sample Run as per test given in moodle Test 1--------------------------
  //pin(1), pin(2), pin(3), read(1), write(2), write(3), unpin(1), unpin(2), unpin(3).
      
      /*BufferMgr bfr;
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
      
      
      bfr.bufferMgr.bufferPoolMap.get(blk1).getInt(1);
      bfr.bufferMgr.getStatistics();
      bfr.bufferMgr.bufferPoolMap.get(blk2).setInt(2, 1, 5, 10);
      bfr.bufferMgr.getStatistics();
      
      bfr.bufferMgr.bufferPoolMap.get(blk3).setInt(2, 1, 15, 30);
      bfr.bufferMgr.getStatistics();
      
      bfr.unpin(bfr.bufferMgr.bufferpool[a]);
      bfr.bufferMgr.getStatistics();
      bfr.unpin(bfr.bufferMgr.bufferpool[b]);
      bfr.bufferMgr.getStatistics();
      bfr.unpin(bfr.bufferMgr.bufferpool[c]);
      bfr.bufferMgr.getStatistics();
      
      bfr.unpin(bfr.bufferMgr.bufferPoolMap.get(blk1));
      bfr.bufferMgr.getStatistics();
      bfr.unpin(bfr.bufferMgr.bufferPoolMap.get(blk2));
      bfr.bufferMgr.getStatistics();
      bfr.unpin(bfr.bufferMgr.bufferPoolMap.get(blk3));
      bfr.bufferMgr.getStatistics();
      
      bfr.pin(blk4);
      bfr.bufferMgr.getStatistics(); 
      */
      
 //----------------------Test Scenario 2--------------------------
 
      BufferMgr bfr;
      bfr=SimpleDB.bufferMgr();
     
      
      
      System.out.println(bfr.bufferMgr.freeBuffers.size()); //Initial free buffers
      
      Block blk1=new Block("Number1",1); //Creating file-blocks
      Block blk2=new Block("Number2",2);
      Block blk3=new Block("Number3",3);
      Block blk4=new Block("Number4",4);
     
     //Pin blocks to buffers 
     try
     {
      bfr.pin(blk1);
      bfr.bufferMgr.getStatistics();
      System.out.println(bfr.bufferMgr.freeBuffers.size());
     
      bfr.pin(blk2);
      bfr.bufferMgr.getStatistics();
      System.out.println(bfr.bufferMgr.freeBuffers.size());
      
      bfr.pin(blk3);
      bfr.bufferMgr.getStatistics();
      System.out.println(bfr.bufferMgr.freeBuffers.size());
      
      bfr.pin(blk4); //Exception is thrown when we try to pin this when max buffer size is 3
      bfr.bufferMgr.getStatistics(); 
     }
     
     catch(Exception e){
    	 System.out.println("All Buffers are currently pinned");
    	 System.out.println();
     }
      
      
    
      
      //Use the getInt and setInt methods to increase the read and write count 
      bfr.bufferMgr.bufferPoolMap.get(blk1).getInt(1);
      bfr.bufferMgr.getStatistics();
      //bfr.bufferMgr.bufferpool[b].setInt(2, 1, 5, 10); 
      bfr.bufferMgr.bufferPoolMap.get(blk2).setInt(2, 1, 5, 10);
      bfr.bufferMgr.getStatistics();
      //bfr.bufferMgr.bufferpool[c].setInt(2, 1, 15, 30); 
      bfr.bufferMgr.bufferPoolMap.get(blk3).setInt(2, 1, 15, 30);
      bfr.bufferMgr.getStatistics();
      
      //unpin the buffers
      bfr.unpin(bfr.bufferMgr.bufferPoolMap.get(blk1));
      bfr.bufferMgr.getStatistics();
      bfr.unpin(bfr.bufferMgr.bufferPoolMap.get(blk2));
      bfr.bufferMgr.getStatistics();
      
      //Replacement strategy will be tested here
      bfr.pin(blk4);
      bfr.bufferMgr.getStatistics(); 
      
      //Returns the buffer for current mapping of the blocks
      System.out.println(bfr.bufferMgr.getMapping(blk1));
      System.out.println(bfr.bufferMgr.getMapping(blk2));
      System.out.println(bfr.bufferMgr.getMapping(blk3));
     
     
      
   }
}
