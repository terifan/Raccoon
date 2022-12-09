package org.terifan.raccoon.io.managed;

import org.terifan.bundle.Document;
import org.terifan.raccoon.io.IBlockDevice;


public interface IManagedBlockDevice extends IBlockDevice
{
	/**
	 * @return true if the block device has pending changes requiring a commit.
	 */
	boolean isModified();


	/**
	 * Allocate a sequence of blocks on the device.
	 */
	long allocBlock(int aBlockCount);


	/**
	 * Free a block from the device.
	 *
	 * @param aBlockIndex the index of the block being freed.
	 */
	void freeBlock(long aBlockIndex, int aBlockCount);


	/**
	 * Commit any pending blocks.
	 */
	void commit();


	/**
	 * Rollback any pending blocks.
	 */
	void rollback();


	void clear();


	/**
	 * Returns a Document containing information to load the application using the block device.
	 */
	Document getApplicationHeader();


	long getTransactionId();


	/**
	 * Return the maximum available space this block device can theoretically allocate. This value may be greater than what the underlying block device can support.
	 */
	long getMaximumSpace();


	/**
	 * Return the size of the underlying block device, ie. size of a file acting as a block storage.
	 */
	long getAllocatedSpace();


	/**
	 * Return the number of free blocks within the allocated space.
	 */
	long getFreeSpace();


	/**
	 * Return the number of blocks actually used.
	 */
	long getUsedSpace();
}
