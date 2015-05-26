package org.terifan.raccoon.io;

import java.io.IOException;


public interface IManagedBlockDevice extends IPhysicalBlockDevice
{
	/**
	 * @return
	 *   true if the block device has pending changes requiring a commit.
	 */
	boolean isModified();


	/**
	 * @return
	 *   the "extra data" block.
	 */
	byte[] getExtraData();


	/**
	 * Sets the "extra data" block of this block device.
	 *
	 * The block device must be able to store an "extra data" block with minimum 384 bytes. The block I/O must be transactional safe and the block is written as the block device changes are committed.
	 *
	 * @param aExtra
	 *   the extra data.
	 */
	void setExtraData(byte[] aExtra);


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
}
