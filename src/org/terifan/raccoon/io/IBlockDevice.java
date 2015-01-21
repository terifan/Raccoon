package org.terifan.raccoon.io;

import java.io.IOException;


public interface IBlockDevice extends AutoCloseable
{
	boolean isModified();
	
	
	/**
	 * @return
	 *   the size of each block on this device.
	 */
	int getBlockSize();


	/**
	 * Allocate a sequence of blocks on the device.
	 */
	long allocBlock(int aBlockCount) throws IOException;


	/**
	 * Allocate a sequence of blocks on the device. The sequence allocated may be shorter
	 * than what is requested and number of blocks allocated is returned in the Result object.
	 * 
	 * @param aBlockCount 
	 *   number of blocks requested to be allocated. Is updated to contain actual number of blocks allocated.
	 * @return
	 *   position of sequence or -1 if no allocation was possible
	 */
//	long allocBlock(Result<Integer> aBlockCount) throws IOException;


	/**
	 * Free a block from the device.
	 *
	 * @param aBlockIndex
	 *   the index of the block being freed.
	 */
	void freeBlock(long aBlockIndex, int aBlockCount) throws IOException;


	/**
	 * Return the size of this device.
	 */
	long length() throws IOException;


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
	 */
	void writeBlock(long aBlockIndex, byte[] aBuffer, int aBufferOffset, int aBufferLength, long aBlockKey) throws IOException;


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
	 */
	void readBlock(long aBlockIndex, byte[] aBuffer, int aBufferOffset, int aBufferLength, long aBlockKey) throws IOException;


	/**
	 * Commit any pending blocks.
	 */
	void commit() throws IOException;


	/**
	 * Rollback any pending blocks.
	 */
	void rollback() throws IOException;


	/**
	 * Rollback any pending blocks and close this device.
	 */
	@Override
	void close() throws IOException;
}
