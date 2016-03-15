package org.terifan.raccoon;

import org.terifan.raccoon.io.BlobInputStream;
import org.terifan.raccoon.io.Streams;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Log;


public class Entry
{
	private HashTable mHashTable;

	protected byte[] mKey;
	protected byte[] mValue;


	public Entry(HashTable aHashTable)
	{
		mHashTable = aHashTable;
	}


	public byte[] getKey()
	{
		return mKey;
	}


	public void setKey(byte[] aKey)
	{
		mKey = aKey;
	}


	public byte[] getValue()
	{
		return mValue;
	}


	public void setValue(byte[] aValue)
	{
		mValue = aValue;
	}


//	public ByteArrayBuffer x()
//	{
//		ByteArrayBuffer buffer = new ByteArrayBuffer(mValue);
//
//		if (buffer.read() == Table.PTR_BLOB)
//		{
//			try
//			{
//				buffer.wrap(Streams.readAll(new BlobInputStream(mHashTable.getBlockAccessor(), buffer)));
//				buffer.position(0);
//			}
//			catch (Exception e)
//			{
//				Log.dec();
//
//				throw new DatabaseException(e);
//			}
//		}
//
//		return buffer;
//	}
}
