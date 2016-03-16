package org.terifan.raccoon;


public final class LeafEntry
{
	static final byte FLAG_BLOB = 1;

	byte mFlags;
	byte[] mKey;
	byte[] mValue;


	LeafEntry()
	{
	}


	LeafEntry(byte[] aKey)
	{
		mKey = aKey;
	}


	LeafEntry(byte[] aKey, byte[] aValue, byte aFlags)
	{
		mKey = aKey;
		mValue = aValue;
		mFlags = aFlags;
	}


	public byte getFlags()
	{
		return mFlags;
	}


	public void setFlags(byte aFlags)
	{
		mFlags = aFlags;
	}


	boolean hasFlag(byte aFlag)
	{
		return (mFlags & aFlag) == aFlag;
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
