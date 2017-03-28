package org.terifan.raccoon.io.managed;

import java.io.IOException;


public interface IManagedBlockDevice extends AutoCloseable //IPhysicalBlockDevice
{
	final static int EXTRA_DATA_LIMIT = 256;

	/**
	 * Read a single block from the device.
	 *
	 * @param aBlockIndex
	 *   the index of the block.
	 * @param aBuffer
	 *   destination array for block the be read
	 * @param aBufferOffset
	 *   offset in the block array where block data is stored
	 * @param aBufferLength
	 *   length of buffer to write, must be dividable by block size
	 * @param aBlockKey
	 *   64 bit seed value that may be used by block device implementations performing cryptography
	 */
	void readBlock(long aBlockIndex, byte[] aBuffer, int aBufferOffset, int aBufferLength, long aBlockKey) throws IOException;


	/**
	 * Write a single block to the device.
	 *
	 * @param aBlockIndex
	 *   the index of the block.
	 * @param aBuffer
	 *   data to be written to the device
	 * @param aBufferOffset
	 *   offset in the block array where block data is stored
	 * @param aBufferLength
	 *   length of buffer to write, must be dividable by block size
	 * @param aBlockKey
	 *   64 bit seed value that may be used by block device implementations performing cryptography
	 */
	void writeBlock(long aBlockIndex, byte[] aBuffer, int aBufferOffset, int aBufferLength, long aBlockKey) throws IOException;


	/**
	 * Attempt to flush any changes made to blocks
	 *
	 * @param aMetadata
	 *   force update of metadata
	 */
	void commit(boolean aMetadata) throws IOException;


	/**
	 * Close the block device. Information not previously committed will be lost.
	 */
	@Override
	void close() throws IOException;


	/**
	 * Return number of blocks available on this device.
	 */
	long length() throws IOException;


	/**
	 * @return
	 *   the size of each block on this device.
	 */
	int getBlockSize() throws IOException;


	/**
	 * Truncates this block device to the number of blocks specified.
	 *
	 * @param aNewLength
	 *   number of blocks
	 */
	void setLength(long aNewLength) throws IOException;


	/**
	 * Close the block device not permitting any future changes to happen. Invoked when an error has occurred that may jeopardize the integrity of the block device.
	 *
	 * Default implementation calls close.
	 */
	default void forceClose() throws IOException
	{
		close();
	}


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


	void clear() throws IOException;
}
