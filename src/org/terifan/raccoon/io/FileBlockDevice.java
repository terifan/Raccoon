package org.terifan.raccoon.io;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import org.terifan.raccoon.util.Log;


public class FileBlockDevice implements IPhysicalBlockDevice
{
	protected RandomAccessFile mFile;
	protected int mBlockSize;


	public FileBlockDevice(File aFile, int aBlockSize) throws IOException
	{
		mFile = new RandomAccessFile(aFile, "rw");
		mBlockSize = aBlockSize;
	}


	public FileBlockDevice(RandomAccessFile aFile, int aBlockSize) throws IOException
	{
		mFile = aFile;
		mBlockSize = aBlockSize;
	}


	@Override
	public void readBlock(long aBlockIndex, byte[] aBuffer, int aBufferOffset, int aBufferLength, long aBlockKey) throws IOException
	{
		Log.v("read block " + aBlockIndex + " +" + aBufferLength/mBlockSize);

		mFile.seek(aBlockIndex * mBlockSize);
		mFile.readFully(aBuffer, aBufferOffset, aBufferLength);
	}


	@Override
	public void writeBlock(long aBlockIndex, byte[] aBuffer, int aBufferOffset, int aBufferLength, long aBlockKey) throws IOException
	{
		Log.v("write block " + aBlockIndex + " +" + aBufferLength/mBlockSize);

		while (aBlockIndex > length())
		{
			mFile.seek(mBlockSize * length());
			mFile.write(new byte[mBlockSize]);
		}

		mFile.seek(aBlockIndex * mBlockSize);
		mFile.write(aBuffer, aBufferOffset, aBufferLength);
	}


	@Override
	public void close() throws IOException
	{
		Log.v("close");

		if (mFile != null)
		{
			mFile.close();
			mFile = null;
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

		mFile.getChannel().force(aMetadata);
	}


	@Override
	public int getBlockSize()
	{
		return mBlockSize;
	}
}