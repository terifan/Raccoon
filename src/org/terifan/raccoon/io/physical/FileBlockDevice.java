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
	protected File mFile;
	protected FileChannel mFileChannel;
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
			mFile = aFile;

			if (aReadOnly)
			{
				mFileChannel = FileChannel.open(aFile.toPath(), StandardOpenOption.CREATE, StandardOpenOption.READ);
			}
			else
			{
				try
				{
					mFileChannel = FileChannel.open(aFile.toPath(), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
				}
				catch (AccessDeniedException e)
				{
					throw new FileAlreadyOpenException("Failed to open file: " + aFile, e);
				}

				try
				{
					mFileLock = mFileChannel.tryLock();
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
			mFileChannel.read(aBuffer, aBlockIndex * mBlockSize);
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
			mFileChannel.write(aBuffer, aBlockIndex * mBlockSize);
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
			mFileChannel.read(buf, aBlockIndex * mBlockSize);
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
			mFileChannel.write(buf, aBlockIndex * mBlockSize);
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

			if (mFileChannel != null)
			{
				try
				{
					mFileChannel.close();
				}
				catch (IOException e)
				{
					throw new DatabaseIOException(e);
				}
				mFileChannel = null;
			}
		}
	}


	@Override
	public long length()
	{
		if (mFileChannel == null)
		{
			return mFile.length() / mBlockSize;
		}
		try
		{
			return mFileChannel.size() / mBlockSize;
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
			mFileChannel.force(aMetadata);
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
			mFileChannel.truncate(aNewLength * mBlockSize);
		}
		catch (IOException e)
		{
			throw new DatabaseIOException(e);
		}
	}
}
