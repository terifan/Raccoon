package org.terifan.raccoon;

import java.io.Externalizable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.Buffer;
import org.terifan.raccoon.io.DatabaseIOException;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Log;


public final class Blob
{
//	private transient TableInstance mTableInstance;
//	private byte[] mHeader;
//
//
//	public Blob()
//	{
//	}
//
//
//	public void read(ByteArrayBuffer aInput)
//	{
//		if (aInput.readBit() != 0)
//		{
//			mHeader = new byte[aInput.readVar32()];
//			aInput.read(mHeader);
//		}
//		else
//		{
//			mHeader = null;
//		}
//	}
//
//
//	public void write(ByteArrayBuffer aOutput)
//	{
//		aOutput.writeBit(mHeader != null);
//		if (mHeader != null)
//		{
//			aOutput.writeVar32(mHeader.length);
//			aOutput.write(mHeader);
//		}
//	}
//
//
//	public void bind(TableInstance aTableInstance)
//	{
//		mTableInstance = aTableInstance;
//	}
//
//
//	public byte[] getHeader()
//	{
//		return mHeader;
//	}
//
//
//	public LobByteChannel open(LobOpenOption aOpenOption) throws IOException
//	{
//		if (mTableInstance == null)
//		{
//			throw new IllegalStateException("This Blob instance doesn't belong to any database.");
//		}
//
//		try
//		{
//			CommitLock lock = new CommitLock();
//
//			if (mHeader != null && aOpenOption == LobOpenOption.CREATE)
//			{
//				throw new IllegalArgumentException("A blob already exists with this key.");
//			}
//
//			if (aOpenOption == LobOpenOption.REPLACE)
//			{
//				LobByteChannelImpl.deleteBlob(mTableInstance.getBlockAccessor(), mHeader);
//				mHeader = null;
//			}
//
//			LobByteChannelImpl out = new LobByteChannelImpl(mTableInstance.getBlockAccessor(), mTableInstance.getDatabase().getTransactionId(), mHeader, aOpenOption)
//			{
//				@Override
//				public void close()
//				{
//					try
//					{
//						try
//						{
//							if (isModified())
//							{
//								Log.d("write blob entry");
//
//								byte[] header = finish();
//
//								mTableInstance.getDatabase().aquireWriteLock();
//								try
//								{
//									mHeader = header;
//
//
//								}
//								catch (DatabaseException e)
//								{
//									mTableInstance.getDatabase().forceClose(e);
//									throw e;
//								}
//								finally
//								{
//									mTableInstance.getDatabase().releaseWriteLock();
//								}
//							}
//						}
//						finally
//						{
//							synchronized (mTableInstance)
//							{
//								mTableInstance.mCommitLocks.remove(lock);
//							}
//
//							super.close();
//						}
//					}
//					catch (IOException e)
//					{
//						throw new DatabaseIOException(e);
//					}
//				}
//			};
//
//			lock.setBlob(out);
//
//			synchronized (this)
//			{
//				mTableInstance.mCommitLocks.add(lock);
//			}
//
//			return out;
//		}
//		catch (IOException e)
//		{
//			throw new DatabaseIOException(e);
//		}
//	}
//
//
//	public byte[] readAllBytes() throws IOException
//	{
//		throw new UnsupportedOperationException();
//	}
//
//
//	public Blob readAllBytes(OutputStream aDst) throws IOException
//	{
//		throw new UnsupportedOperationException();
//	}
//
//
//	public Blob readAllBytes(Buffer aDst) throws IOException
//	{
//		throw new UnsupportedOperationException();
//	}
//
//
//	public Blob consume(byte[] aSrc) throws IOException
//	{
//		throw new UnsupportedOperationException();
//	}
//
//
//	public Blob consume(InputStream aSrc) throws IOException
//	{
//		throw new UnsupportedOperationException();
//	}
//
//
//	public Blob consume(Buffer aSrc) throws IOException
//	{
//		throw new UnsupportedOperationException();
//	}
//
//
//	public InputStream newInputStream()
//	{
//		throw new UnsupportedOperationException();
//	}
//
//
//	public OutputStream newOutputStream()
//	{
//		throw new UnsupportedOperationException();
//	}
//
//
//	public void delete()
//	{
//		throw new UnsupportedOperationException();
//	}
//
//
//	public long size()
//	{
//		if (mTableInstance == null)
//		{
//			throw new IllegalStateException("This Blob instance doesn't belong to any database.");
//		}
//		if (mHeader == null)
//		{
//			return 0;
//		}
//
//		throw new UnsupportedOperationException();
//	}
}
