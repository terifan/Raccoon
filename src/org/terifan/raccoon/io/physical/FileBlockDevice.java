package org.terifan.raccoon.io.physical;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.AccessDeniedException;
import java.nio.file.StandardOpenOption;
import org.terifan.raccoon.io.DatabaseIOException;
import org.terifan.raccoon.util.Log;


public class FileBlockDevice implements IPhysicalBlockDevice
{
	protected FileChannel mFile;
	protected FileLock mFileLock;
	protected int mBlockSize;


	public FileBlockDevice(File aFile)
	{
		this(aFile, 4096, false);
	}


	public FileBlockDevice(File aFile, int aBlockSize, boolean aReadOnly)
	{
		try
		{
			if (aReadOnly)
			{
				mFile = FileChannel.open(aFile.toPath(), StandardOpenOption.CREATE, StandardOpenOption.READ);
			}
			else
			{
				try
				{
					mFile = FileChannel.open(aFile.toPath(), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
				}
				catch (AccessDeniedException e)
				{
					throw new FileAlreadyOpenException("Failed to open file: " + aFile, e);
				}

				try
				{
					mFileLock = mFile.tryLock();
				}
				catch (IOException | OverlappingFileLockException e)
				{
					throw new FileAlreadyOpenException("Failed to lock file: " + aFile, e);
				}
			}

			mBlockSize = aBlockSize;
		}
		catch (IOException e)
		{
			throw new DatabaseIOException(e);
		}
	}


	public void readBlock(long aBlockIndex, ByteBuffer aBuffer, long[] aBlockKey)
	{
		Log.d("read block %d +%d", aBlockIndex, (aBuffer.limit() - aBuffer.position()) / mBlockSize);

		try
		{
			mFile.read(aBuffer, aBlockIndex * mBlockSize);
		}
		catch (IOException e)
		{
			throw new DatabaseIOException(e);
		}
	}


	public void writeBlock(long aBlockIndex, ByteBuffer aBuffer, long[] aBlockKey)
	{
		Log.d("write block %d +%d", aBlockIndex, (aBuffer.limit() - aBuffer.position()) / mBlockSize);

		try
		{
			mFile.write(aBuffer, aBlockIndex * mBlockSize);
		}
		catch (IOException e)
		{
			throw new DatabaseIOException(e);
		}
	}


	@Override
	public void readBlock(long aBlockIndex, byte[] aBuffer, int aBufferOffset, int aBufferLength, long[] aBlockKey)
	{
		Log.d("read block %d +%d", aBlockIndex, aBufferLength / mBlockSize);

		try
		{
			ByteBuffer buf = ByteBuffer.wrap(aBuffer, aBufferOffset, aBufferLength);
			mFile.read(buf, aBlockIndex * mBlockSize);
		}
		catch (IOException e)
		{
			throw new DatabaseIOException(e);
		}
	}


	@Override
	public void writeBlock(long aBlockIndex, byte[] aBuffer, int aBufferOffset, int aBufferLength, long[] aBlockKey)
	{
		Log.d("write block %d +%d", aBlockIndex, aBufferLength / mBlockSize);

		try
		{
			ByteBuffer buf = ByteBuffer.wrap(aBuffer, aBufferOffset, aBufferLength);
			mFile.write(buf, aBlockIndex * mBlockSize);
		}
		catch (IOException e)
		{
			throw new DatabaseIOException(e);
		}
	}


	@Override
	public void close()
	{
		Log.d("close");

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
				try
				{
					mFile.close();
				}
				catch (IOException e)
				{
					throw new DatabaseIOException(e);
				}
				mFile = null;
			}
		}
	}


	@Override
	public long length()
	{
		try
		{
			return mFile.size() / mBlockSize;
		}
		catch (IOException e)
		{
			throw new DatabaseIOException(e);
		}
	}


	@Override
	public void commit(boolean aMetadata)
	{
		Log.d("commit");

		try
		{
			mFile.force(aMetadata);
		}
		catch (IOException e)
		{
			throw new DatabaseIOException(e);
		}
	}


	@Override
	public int getBlockSize()
	{
		return mBlockSize;
	}


	@Override
	public void setLength(long aNewLength)
	{
		try
		{
			mFile.truncate(aNewLength * mBlockSize);
		}
		catch (IOException e)
		{
			throw new DatabaseIOException(e);
		}
	}
}
