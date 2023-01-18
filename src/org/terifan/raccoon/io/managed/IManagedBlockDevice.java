package org.terifan.raccoon.io.managed;

import org.terifan.raccoon.document.Document;
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
	 * @return a Document containing information about the application using the block device.
	 */
	Document getApplicationMetadata();


	long getTransactionId();


	/**
	 * @return the maximum available space this block device can theoretically allocate. This value may be greater than what the underlying block device can support.
	 */
	long getMaximumSpace();


	/**
	 * @return the size of the underlying block device, ie. size of a file acting as a block storage.
	 */
	long getAllocatedSpace();


	/**
	 * @return the number of free blocks within the allocated space.
	 */
	long getFreeSpace();


	/**
	 * @return the number of blocks actually used.
	 */
	long getUsedSpace();
}
