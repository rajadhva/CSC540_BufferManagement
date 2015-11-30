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
public class BasicBufferMgr {
	public Buffer[] bufferpool;
	private int numAvailable;
	// CSC-540 Buffer Management Map to store block buffer mapping(Task1)
	public HashMap<Block, Buffer> bufferPoolMap;
	// CSC-540 Buffer Management List to store the current free buffers(No block
	// allocated yet).
	private LinkedList<Buffer> freeBuffers;

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
		// initialize hash Map
		bufferPoolMap = new HashMap<Block, Buffer>(numbuffs);
		// initialize free buffers.
		freeBuffers = new LinkedList<Buffer>();
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
			if (lastBlock != null)
				bufferPoolMap.remove(lastBlock);
			// Allocating a new block to the buffer
			bufferPoolMap.put(blk, buff);
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
		// if last block not null remove it from hashMap
		if (lastBlock != null)
			bufferPoolMap.remove(lastBlock);
		// Allocating a new block to the buffer
		buff.assignToNew(filename, fmtr);
		bufferPoolMap.put(buff.block(), buff);
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

	// CSC-540 Buffer Management- Uses Map to determine whether block is in
	// buffer currently
	private Buffer findExistingBuffer(Block blk) {

		Buffer buff = bufferPoolMap.get(blk);

		if (buff!= null) {
			//System.out.println("Block " + blk.number() + " already in buffer " + x); // For
																						// Debug
			return buff;

		} else {
			return null;
		}
	}

	// CSC-540 Buffer Management-Uses Least Recently Modified Buffer Replacement
	// Policy
	private Buffer chooseUnpinnedBuffer() {
		//getStatistics();(Can be used to get the statistics whenever a unpinned buffer is chosen)

		int maxLSN = Integer.MAX_VALUE;
		Buffer currentBuffer = null;
		Buffer buff = null;

		// If any buffer is still unallocated then allocate it
		if (!freeBuffers.isEmpty()) {
			buff = freeBuffers.getFirst();
			freeBuffers.removeFirst();
			System.out.println("New Buffer " + buff.getBufferIndex() + "Allocated");
			return buff;

		}
		// Chooses modified page with lowest non-negative LSN
		Iterator<Block> iterator1 = bufferPoolMap.keySet().iterator();
		while (iterator1.hasNext()) {
			Block bkey = iterator1.next();
			currentBuffer = bufferPoolMap.get(bkey);
			if (!currentBuffer.isPinned() && currentBuffer.modifiedBy >= 0
					&& currentBuffer.logSequenceNumber < maxLSN
					&& currentBuffer.logSequenceNumber >= 0) {
				buff = currentBuffer;
				maxLSN = buff.logSequenceNumber;

			}

		}

		// If no buffer is modified we look for the first unpinned,un-modified
		// buffer and choose it for replacement.
		if (buff == null) {
			Iterator<Block> iterator2 = bufferPoolMap.keySet().iterator();
			while (iterator2.hasNext()) {
				Block bkey = iterator2.next();
				currentBuffer = bufferPoolMap.get(bkey);

				if (!currentBuffer.isPinned()) {
					buff =currentBuffer;

					System.out.println("Buffer Choosen For Replacement " + buff.getBufferIndex());
					return buff;
				}
			}
		}
		System.out.println("Buffer Choosen For Replacement " + buff.getBufferIndex());
		return buff;

	}

	// Get the statistics for the buffer.
	public void getStatistics() {
		System.out.println("Status of the buffer pool");
		for (Buffer buff1 : bufferpool) {

			int bufferReadCount = buff1.getReadCount();
			int bufferWriteCount = buff1.getWriteCount();
			System.out.println("Buffer " + buff1.getBufferIndex() + " LSN=" + buff1.logSequenceNumber + "Modified By"
					+ buff1.modifiedBy + " Pin Count " + buff1.pins + " isPinned=" + buff1.isPinned() + " Read Count " + bufferReadCount + " Write Count" + bufferWriteCount);

			/*int bufferReadCount = buff1.getReadCount();
			System.out.println("The read count of buffer " + buff1.getBufferIndex() + " " + bufferReadCount);
			int bufferWriteCount = buff1.getWriteCount();
			System.out.println("The write count of buffer " + buff1.getBufferIndex() + " " + bufferWriteCount);*/
		}
		System.out.println();
	}

	//CSC-540 Buffer Management- Test method given in the project description
	boolean containsMapping(Block blk)
	{
		return bufferPoolMap.containsKey(blk);
	}
	
	////CSC-540 Buffer Management - Test method given in the project description
	Buffer getMapping(Block blk)
	{
		return bufferPoolMap.get(blk);
	}
	
	
}