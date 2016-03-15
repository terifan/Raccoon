package org.terifan.raccoon;


public class LeafEntry
{
	byte mHeader;
	byte[] mKey;
	byte[] mValue;


	public LeafEntry()
	{
	}


	public LeafEntry(byte[] aKey)
	{
		mKey = aKey;
	}


	public LeafEntry(byte[] aKey, byte[] aValue, int aHeader)
	{
		mKey = aKey;
		mValue = aValue;
		mHeader = (byte)aHeader;
	}


	public byte getHeader()
	{
		return mHeader;
	}


	public void setHeader(byte aHeader)
	{
		this.mHeader = aHeader;
	}


	public byte[] getKey()
	{
		return mKey;
	}


	public void setKey(byte[] aKey)
	{
		this.mKey = aKey;
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
}
