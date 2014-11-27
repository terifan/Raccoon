package org.terifan.v1.raccoon.io;

import java.io.IOException;


public interface IPhysicalBlockDevice extends AutoCloseable
{
	/**
	 * Read one or more blocks.
	 * 
	 * @param aPosition 
	 *   block index to start reading from
	 * @param aBuffer 
	 *   destination buffer
	 * @param aBufferOffset
	 *   offset in destination buffer to start writing
	 * @param aBufferLength 
	 *   number of bytes to read, must be a multiple of the block size
	 */
	public void readBlock(long aPosition, byte[] aBuffer, int aBufferOffset, int aBufferLength, long aBlockKey) throws IOException;


	/**
	 * Write one or more blocks.
	 * 
	 * @param aPosition 
	 *   block index to start writing to
	 * @param aBuffer 
	 *   source buffer
	 * @param aBufferOffset
	 *   offset in source buffer to start reading
	 * @param aBufferLength 
	 *   number of bytes to write, must be a multiple of the block size
	 */
	public void writeBlock(long aPosition, byte[] aBuffer, int aBufferOffset, int aBufferLength, long aBlockKey) throws IOException;


	/**
	 * Attempt to flush any changes made to blocks
	 */
	public void commit(boolean aMetadata) throws IOException;
	

	/**
	 * Close the block device. Information not previously committed will be lost.
	 */
	public void close() throws IOException;


	/**
	 * Return number of blocks available on this device. If the device is implemented as a file then the length can be the length of the file divided by block size
	 */
	public long length() throws IOException;


	/**
	 * Return number of bytes in a single block
	 */
	public int getBlockSize();
}
