package org.terifan.raccoon.io.managed;

import java.io.IOException;
import org.terifan.raccoon.io.IBlockDevice;


public interface IManagedBlockDevice extends IBlockDevice
{
	final static int APPLICATION_POINTER_MAX_SIZE = 255;


	/**
	 * @return
	 *   true if the block device has pending changes requiring a commit.
	 */
	boolean isModified();


	/**
	 * Allocate a sequence of blocks on the device.
	 */
	long allocBlock(int aBlockCount) throws IOException;


	/**
	 * Free a block from the device.
	 *
	 * @param aBlockIndex
	 *   the index of the block being freed.
	 */
	void freeBlock(long aBlockIndex, int aBlockCount) throws IOException;


	/**
	 * Commit any pending blocks.
	 */
	void commit() throws IOException;


	/**
	 * Rollback any pending blocks.
	 */
	void rollback() throws IOException;


	void clear() throws IOException;


	DeviceHeader getApplicationHeader();


	void setApplicationHeader(DeviceHeader aApplicationHeader);


	DeviceHeader getTenantHeader();


	void setTenantHeader(DeviceHeader aTenantHeader);


	byte[] getApplicationPointer();


	void setApplicationPointer(byte[] aApplicationPointer);


	long getTransactionId();
}
