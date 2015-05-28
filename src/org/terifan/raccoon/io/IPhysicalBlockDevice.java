package org.terifan.raccoon.io;

import java.io.IOException;


public interface IPhysicalBlockDevice extends AutoCloseable
{
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
}