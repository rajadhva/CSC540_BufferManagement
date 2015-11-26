package simpledb.buffer;

import java.util.*;
import simpledb.file.*;

/**
 * Manages the pinning and unpinning of buffers to blocks.
 * 
 * @author Edward Sciore
 *
 */
class BasicBufferMgr {
	private Buffer[] bufferpool;
	private int numAvailable;
	private Hashtable<Block, Integer> bufferPoolMap; // for id'ing blocks
														// assigned to buffers
	private LinkedList<Integer> freeBuffers; // to identify free blocks

	/**
	 * Creates a buffer manager having the specified number of buffer slots.
	 * This constructor depends on both the {@link FileMgr} and
	 * {@link simpledb.log.LogMgr LogMgr} objects that it gets from the class
	 * {@link simpledb.server.SimpleDB}. Those objects are created during system
	 * initialization. Thus this constructor cannot be called until
	 * {@link simpledb.server.SimpleDB#initFileAndLogMgr(String)} or is called
	 * first.
	 * 
	 * @param numbuffs
	 *            the number of buffer slots to allocate
	 */
	BasicBufferMgr(int numbuffs) {
		bufferpool = new Buffer[numbuffs];
		numAvailable = numbuffs;
		bufferPoolMap = new Hashtable<Block, Integer>(numbuffs); // initialize
																	// hash
																	// table.
		freeBuffers = new LinkedList<Integer>(); // initialize free buffers.
		for (int i = 0; i < numbuffs; i++) {
			bufferpool[i] = new Buffer();
			freeBuffers.add(i); // initialize all to be free buffers
		}
	}
   //display the contents of all the buffers.
	public String toString(){
		   String buffInfo = new String();
		   for (Buffer buff : bufferpool){
			   buffInfo += buff.toString() + System.getProperty("line.separator"); // Use system newline
		   }
		   return buffInfo;
	   }
	
	/**
	 * Flushes the dirty buffers modified by the specified transaction.
	 * 
	 * @param txnum
	 *            the transaction's id number
	 */
	synchronized void flushAll(int txnum) {
		for (Buffer buff : bufferpool)
			if (buff.isModifiedBy(txnum))
				buff.flush();
	}

	/**
	 * Pins a buffer to the specified block. If there is already a buffer
	 * assigned to that block then that buffer is used; otherwise, an unpinned
	 * buffer from the pool is chosen. Returns a null value if there are no
	 * available buffers.
	 * 
	 * @param blk
	 *            a reference to a disk block
	 * @return the pinned buffer
	 */
	synchronized Buffer pin(Block blk) {
		Buffer buff = findExistingBuffer(blk);
		if (buff == null) {
			buff = chooseUnpinnedBuffer();
			if (buff == null)
				return null;
			buff.assignToBlock(blk);
		}
		if (!buff.isPinned())
			bufferPoolMap.put(blk, buff.getBufferIndex()); // adding blk,
															// bufferindex;
		numAvailable--;
		buff.pin();
		return buff;
	}

	/**
	 * Allocates a new block in the specified file, and pins a buffer to it.
	 * Returns null (without allocating the block) if there are no available
	 * buffers.
	 * 
	 * @param filename
	 *            the name of the file
	 * @param fmtr
	 *            a pageformatter object, used to format the new block
	 * @return the pinned buffer
	 */
	synchronized Buffer pinNew(String filename, PageFormatter fmtr) {
		Buffer buff = chooseUnpinnedBuffer();
		if (buff == null)
			return null;
		buff.assignToNew(filename, fmtr);
		bufferPoolMap.put(buff.block(), buff.getBufferIndex()); // Get block
																// using block
																// method..adding
																// blk,
																// bufferindex;
		numAvailable--;
		buff.pin();
		return buff;
	}

	/**
	 * Unpins the specified buffer.
	 * 
	 * @param buff
	 *            the buffer to be unpinned
	 */
	synchronized void unpin(Buffer buff) {
		buff.unpin();
		if (!buff.isPinned())
			numAvailable++;
	}

	/**
	 * Returns the number of available (i.e. unpinned) buffers.
	 * 
	 * @return the number of available buffers
	 */
	int available() {
		return numAvailable;
	}

	private Buffer findExistingBuffer(Block blk) {
		/*
		 * for (Buffer buff : bufferpool) { Block b = buff.block(); if (b !=
		 * null && b.equals(blk)) return buff; }
		 */

		Integer x = bufferPoolMap.get(blk);

		if (x != null) {
			System.out.println(bufferpool[x]); //For Debug
			return bufferpool[x];

		} else {
			return null;
		}
	}

	private Buffer chooseUnpinnedBuffer() {
		/*for (Buffer buff : bufferpool)
			if (!buff.isPinned())
				return buff;
		return null;*/
		int index=-1;
		int size= freeBuffers.size();
		if(size!=0)
		{
			index=freeBuffers.getFirst(); //Temporary Code .Need to modify it based on replacement policy. 
			return bufferpool[index]; 
		}
		else
		{
			return null;
		}
		
	}
}
