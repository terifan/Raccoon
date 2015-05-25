package org.terifan.raccoon.io;

import java.io.IOException;


public interface IBlockDevice extends AutoCloseable
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
	 * @return
	 *   the size of each block on this device.
	 */
	int getBlockSize();


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
	 * Return number of blocks in this device.
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
	 * @param aBlockKey 
	 *   64 bit seed value that may be used by block device implementations performing cryptography
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
	 * @param aBlockKey 
	 *   64 bit seed value that may be used by block device implementations performing cryptography
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
