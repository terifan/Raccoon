package org.terifan.raccoon;


public class LeafEntry
{
	static final byte FLAG_BLOB = 1;

	byte mFlags;
	byte[] mKey;
	byte[] mValue;


	public LeafEntry()
	{
	}


	public LeafEntry(byte[] aKey)
	{
		mKey = aKey;
	}


	public LeafEntry(byte[] aKey, byte[] aValue, byte aFlags)
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


	public void setValue(byte[] aValue)
	{
		this.mValue = aValue;
	}


	boolean hasFlag(byte aFlag)
	{
		return (mFlags & aFlag) != 0;
	}
}
