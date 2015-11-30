package simpledb.buffer;

import java.util.Date;
import java.util.Hashtable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;

import simpledb.file.Block;
import simpledb.file.FileMgr;

/**
 * Manages the pinning and unpinning of buffers to blocks.
 * 
 * @author Edward Sciore
 * 
 */
class BasicBufferMgr {
	private Buffer[] bufferpool;
	private int numAvailable;
	private HashMap<Block, Integer> bufferPoolMap; // for id'ing blocks
														// assigned to buffers
	private LinkedList<Buffer> freeBuffers; // to identify free blocks

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
		bufferPoolMap = new HashMap<Block, Integer>(numbuffs); // initialize
																	// hash
																	// table.
		freeBuffers = new LinkedList<Buffer>(); // initialize free buffers.
		for (int i = 0; i < numbuffs; i++) {
			bufferpool[i] = new Buffer(i);
			freeBuffers.add(bufferpool[i]); // initialize all to be free buffers
		}
	}

	// display the contents of all the buffers.
	public String toString() {
		String buffInfo = new String();
		for (Buffer buff : bufferpool) {
			buffInfo += buff.toString() + System.getProperty("line.separator"); // Use
																				// system
																				// newline
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
		Block lastBlock = null;
		if (buff == null) {
			buff = chooseUnpinnedBuffer();
			if (buff == null)
				return null;
			lastBlock = buff.block();
			// if last block not null remove it from hashMap
			if(lastBlock != null)
				bufferPoolMap.remove(lastBlock);
			bufferPoolMap.put(blk, buff.getBufferIndex());
			buff.assignToBlock(blk);
		}
		if (!buff.isPinned()) {
			numAvailable--;
		}
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
		Block lastBlock = null;
		if (buff == null)
			return null;
		lastBlock = buff.block();
		if (lastBlock != null)
				bufferPoolMap.remove(lastBlock);
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
			System.out.println(bufferpool[x]); // For Debug
			return bufferpool[x];

		} else {
			return null;
		}
	}

	// Uses Least Recently Modified Buffer Replacement Policy
	private Buffer chooseUnpinnedBuffer() {
		//Date leastRecentlyModifiedTime = new Date(); // (Date)Integer.MAX_VALUE;
		for (Buffer buff1 : bufferpool) {
			System.out.println("Buffer " + buff1.getBufferIndex() + " LSN="
					+ buff1.logSequenceNumber + "Modified By" + buff1.modifiedBy
					+ "isPinned " + buff1.isPinned()+"");
		}
		System.out.println();
		
		int maxLSN = Integer.MAX_VALUE;
		int loopVariable = 0;
		Buffer buff = null;
		int index=-1;

		// If any buffer is still unallocated then allocate it
		if (!freeBuffers.isEmpty()) {
			buff = freeBuffers.getFirst();
			freeBuffers.removeFirst();
			System.out.println("New Buffer " + buff.getBufferIndex()
					+ "Allocated");
			return buff;
			/*
			 * for (loopVariable = 0; loopVariable < bufferpool.length;
			 * loopVariable++) { buff = bufferpool[loopVariable]; if
			 * (!buff.isPinned() && !buff.alreadyAssigned) { return buff; } }
			 */
		}
		
		Iterator<Block> iterator1 = bufferPoolMap.keySet().iterator();
		while(iterator1.hasNext()){
			Block bkey = iterator1.next();
			loopVariable = bufferPoolMap.get(bkey);
			if (!bufferpool[loopVariable].isPinned()
					&& bufferpool[loopVariable].modifiedBy >= 0
					&& bufferpool[loopVariable].logSequenceNumber < maxLSN) {
				buff = bufferpool[loopVariable];
				maxLSN = buff.logSequenceNumber;
				index = loopVariable;
				// return buff;
			}
			
		}
		/*//Chooses modified  page with lowest LSN
		for (loopVariable = 0; loopVariable < bufferpool.length; loopVariable++) {
			if (!bufferpool[loopVariable].isPinned()
					&& bufferpool[loopVariable].modifiedBy >= 0
					&& bufferpool[loopVariable].logSequenceNumber < maxLSN) {
				buff = bufferpool[loopVariable];
				maxLSN = buff.logSequenceNumber;
				// return buff;
			}
		}
		// if none modified then choose unpinned page with lowest LSN
		/* if (buff == null) {
			maxLSN = Integer.MAX_VALUE;
			for (loopVariable = 0; loopVariable < bufferpool.length; loopVariable++) {
				if (!bufferpool[loopVariable].isPinned()
						&& bufferpool[loopVariable].logSequenceNumber < maxLSN && bufferpool[loopVariable].logSequenceNumber >= 0) {
					buff = bufferpool[loopVariable];
					maxLSN = buff.logSequenceNumber;
					// return buff;
				}
			}
		}
		return buff; */

        if (buff == null) {
		Iterator<Block> iterator2 = bufferPoolMap.keySet().iterator();
		while(iterator2.hasNext()){
			Block bkey = iterator2.next();
			loopVariable = bufferPoolMap.get(bkey);

				if (!bufferpool[loopVariable].isPinned()) {
					buff = bufferpool[loopVariable];
					//maxLSN = buff.logSequenceNumber;
					System.out.println("Buffer Choosen For Replacement "
							+ buff.getBufferIndex());
					return buff;
				}
			}
		}
		System.out.println("Buffer Choosen For Replacement " + index);
		return buff;
		/*
		 * int index = -1; int size = freeBuffers.size(); if (size != 0) { index
		 * = freeBuffers.getFirst(); // Temporary Code .Need to modify it //
		 * based on replacement policy. return bufferpool[index]; } else {
		 * return null; }
		 */ 
		 

	}
	
	public void getStatistics() {
		
		for (Buffer buff : bufferpool) {
            
			int bufferReadCount = buff.getReadCount();
            System.out.println("The read count of buffer "+buff.getBufferIndex()+" "+ bufferReadCount);
            int bufferWriteCount = buff.getWriteCount();
            System.out.println("The write count of buffer "+buff.getBufferIndex()+" "+ bufferWriteCount);
		}
	}
}