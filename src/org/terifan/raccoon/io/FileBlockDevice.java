package org.terifan.raccoon.io;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.StandardOpenOption;
import org.terifan.raccoon.util.Log;


public class FileBlockDevice implements IPhysicalBlockDevice
{
	protected FileChannel mFile;
	protected FileLock mFileLock;
	protected int mBlockSize;


	public FileBlockDevice(File aFile, int aBlockSize, boolean aReadOnly) throws IOException
	{
		if (aReadOnly)
		{
			mFile = FileChannel.open(aFile.toPath(), StandardOpenOption.CREATE, StandardOpenOption.READ);
		}
		else
		{
			mFile = FileChannel.open(aFile.toPath(), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
		}

		mBlockSize = aBlockSize;

		if (!aReadOnly)
		{
			try
			{
				mFileLock = mFile.tryLock();
			}
			catch (Exception e)
			{
				throw new FileAlreadyOpenException("Failed to lock file: " + aFile, e);
			}
		}
	}


	public void readBlock(long aBlockIndex, ByteBuffer aBuffer, long aBlockKey) throws IOException
	{
		Log.v("read block %d +%d", aBlockIndex, (aBuffer.limit() - aBuffer.position()) / mBlockSize);

		mFile.read(aBuffer, aBlockIndex * mBlockSize);
	}


	public void writeBlock(long aBlockIndex, ByteBuffer aBuffer, long aBlockKey) throws IOException
	{
		Log.v("write block %d +%d", aBlockIndex, (aBuffer.limit() - aBuffer.position()) / mBlockSize);

		mFile.write(aBuffer, aBlockIndex * mBlockSize);
	}


	@Override
	public void readBlock(long aBlockIndex, byte[] aBuffer, int aBufferOffset, int aBufferLength, long aBlockKey) throws IOException
	{
		Log.v("read block %d +%d", aBlockIndex, aBufferLength / mBlockSize);

		ByteBuffer buf = ByteBuffer.wrap(aBuffer, aBufferOffset, aBufferLength);
		mFile.read(buf, aBlockIndex * mBlockSize);
	}


	@Override
	public void writeBlock(long aBlockIndex, byte[] aBuffer, int aBufferOffset, int aBufferLength, long aBlockKey) throws IOException
	{
		Log.v("write block %d +%d", aBlockIndex, aBufferLength / mBlockSize);

		ByteBuffer buf = ByteBuffer.wrap(aBuffer, aBufferOffset, aBufferLength);
		mFile.write(buf, aBlockIndex * mBlockSize);
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
					mFileLock.close();
					mFileLock = null;
				}
				catch (Throwable e)
				{
					Log.e("Unhandled error when releasing file lock", e);
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
		return mFile.size() / mBlockSize;
	}


	@Override
	public void commit(boolean aMetadata) throws IOException
	{
		Log.v("commit");

		synchronized (this)
		{
			mFile.force(aMetadata);
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
		mFile.truncate(aNewLength * mBlockSize);
	}
}
