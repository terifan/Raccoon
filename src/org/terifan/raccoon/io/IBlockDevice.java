package org.terifan.raccoon.io;


public interface IBlockDevice extends AutoCloseable
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
	 * @param aIV
	 *   128 bit seed value that may be used by block device implementations performing cryptography
	 */
	void readBlock(long aBlockIndex, byte[] aBuffer, int aBufferOffset, int aBufferLength, long[] aIV);


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
	 *   128 bit seed value that may be used by block device implementations performing cryptography
	 */
	void writeBlock(long aBlockIndex, byte[] aBuffer, int aBufferOffset, int aBufferLength, long[] aIV);


	/**
	 * Attempt to flush any changes made to blocks
	 *
	 * @param aMetadata
	 *   force update of metadata
	 */
	void commit(boolean aMetadata);


	/**
	 * Close the block device. Information not previously committed will be lost.
	 */
	@Override
	void close();


	/**
	 * Return number of blocks available on this device.
	 */
	long length();


	/**
	 * @return
	 *   the size of each block on this device.
	 */
	int getBlockSize();


	/**
	 * Truncates this block device to the number of blocks specified.
	 *
	 * @param aNewLength
	 *   number of blocks
	 */
	void setLength(long aNewLength);


	/**
	 * Close the block device not permitting any future changes to happen. Invoked when an error has occurred that may jeopardize the integrity of the block device.
	 *
	 * Default implementation calls close.
	 */
	default void forceClose()
	{
		close();
	}
}
