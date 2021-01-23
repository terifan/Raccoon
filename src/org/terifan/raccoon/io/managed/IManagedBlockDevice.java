package org.terifan.raccoon.io.managed;

import org.terifan.raccoon.io.IBlockDevice;


public interface IManagedBlockDevice extends IBlockDevice
{
	final static int APPLICATION_POINTER_MAX_SIZE = 255;


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


	DeviceHeader getApplicationHeader();


	void setApplicationHeader(DeviceHeader aApplicationHeader);


	DeviceHeader getTenantHeader();


	void setTenantHeader(DeviceHeader aTenantHeader);


	byte[] getApplicationPointer();


	void setApplicationPointer(byte[] aApplicationPointer);


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
