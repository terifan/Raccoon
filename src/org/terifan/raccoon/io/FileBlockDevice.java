package org.terifan.raccoon.io;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;
import org.terifan.raccoon.util.Log;


public class FileBlockDevice implements IPhysicalBlockDevice
{
	protected RandomAccessFile mFile;
	protected int mBlockSize;
	protected FileLock mFileLock;


	public FileBlockDevice(File aFile, int aBlockSize) throws IOException
	{
		mFile = new RandomAccessFile(aFile, "rw");
		mBlockSize = aBlockSize;

		try
		{
			mFileLock = mFile.getChannel().lock();
		}
		catch (Exception e)
		{
			throw new FileAlreadyOpenException("Failed to lock file: " + aFile, e);
		}
	}


	public FileBlockDevice(RandomAccessFile aFile, int aBlockSize) throws IOException
	{
		mFile = aFile;
		mBlockSize = aBlockSize;
	}


	@Override
	public void readBlock(long aBlockIndex, byte[] aBuffer, int aBufferOffset, int aBufferLength, long aBlockKey) throws IOException
	{
		Log.v("read block %d +%d", aBlockIndex, aBufferLength / mBlockSize);

		synchronized (this)
		{
			mFile.seek(aBlockIndex * mBlockSize);
			mFile.readFully(aBuffer, aBufferOffset, aBufferLength);
		}
	}


	@Override
	public void writeBlock(long aBlockIndex, byte[] aBuffer, int aBufferOffset, int aBufferLength, long aBlockKey) throws IOException
	{
		Log.v("write block %d +%d", aBlockIndex, aBufferLength / mBlockSize);

		synchronized (this)
		{
			while (aBlockIndex > length())
			{
				mFile.seek(mBlockSize * length());
				mFile.write(new byte[mBlockSize]);
			}

			mFile.seek(aBlockIndex * mBlockSize);
			mFile.write(aBuffer, aBufferOffset, aBufferLength);
		}
	}


	@Override
	public void close() throws IOException
	{
		Log.v("close");

		synchronized (this)
		{
			if (mFileLock != null)
			{
				try
				{
					mFileLock.release();
					mFileLock = null;
				}
				catch (Exception e)
				{
					e.printStackTrace(Log.out);
				}
			}
			if (mFile != null)
			{
				mFile.close();
				mFile = null;
			}
		}
	}


	@Override
	public long length() throws IOException
	{
		return mFile.length() / mBlockSize;
	}


	@Override
	public void commit(boolean aMetadata) throws IOException
	{
		Log.v("commit");

		synchronized (this)
		{
			mFile.getChannel().force(aMetadata);
		}
	}


	@Override
	public int getBlockSize()
	{
		return mBlockSize;
	}


	@Override
	public void setLength(long aNewLength) throws IOException
	{
		synchronized (this)
		{
			mFile.setLength(aNewLength * mBlockSize);
		}
	}
}
