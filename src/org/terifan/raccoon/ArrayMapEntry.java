package org.terifan.raccoon;

import java.util.Arrays;
import org.terifan.raccoon.blockdevice.BlockPointer;
import org.terifan.raccoon.blockdevice.util.Console;
import org.terifan.raccoon.document.Document;


final class ArrayMapEntry
{
	private byte mType;
	private ArrayMapKey mKey;
	private byte[] mValue;


	public ArrayMapEntry()
	{
	}


	public ArrayMapEntry(ArrayMapKey aKey)
	{
		mKey = aKey;
	}


	public ArrayMapEntry(ArrayMapKey aKey, Document aValue, byte aType)
	{
		mKey = aKey;
		mValue = aValue.toByteArray();
		mType = aType;
	}


	public ArrayMapEntry(ArrayMapKey aKey, BlockPointer aValue, byte aType)
	{
		mKey = aKey;
		mValue = aValue.toByteArray();
		mType = aType;
	}


	public ArrayMapEntry(ArrayMapKey aKey, ArrayMapEntry aEntry)
	{
		mKey = aKey;
		mValue = aEntry.mValue;
		mType = aEntry.mType;
	}


	public ArrayMapKey getKey()
	{
		return mKey;
	}


	public void setKey(ArrayMapKey aKey)
	{
		mKey = aKey;
	}


	public Document getValue()
	{
		return new Document().fromByteArray(mValue);
	}


	public BlockPointer getBlockPointer()
	{
		return (BlockPointer)new BlockPointer().fromByteArray(mValue);
	}


	public void setValue(Document aDocument)
	{
		mValue = aDocument.toByteArray();
	}


	public byte getType()
	{
		return mType;
	}


	public void setType(byte aType)
	{
		mType = aType;
	}


	public void marshallValue(byte[] aBuffer, int aOffset)
	{
		aBuffer[aOffset] = mType;
		System.arraycopy(mValue, 0, aBuffer, aOffset + 1, mValue.length);
	}


	public void unmarshallValue(byte[] aBuffer, int aOffset, int aLength)
	{
		mType = aBuffer[aOffset];
		mValue = Arrays.copyOfRange(aBuffer, aOffset + 1, aOffset + aLength);
	}


	public int getMarshalledLength()
	{
		return 1 + mValue.length + mKey.size();
	}


	public int getMarshalledValueLength()
	{
		return 1 + mValue.length;
	}


	public int length()
	{
		return mKey.size() + 1 + mValue.length;
	}


	@Override
	public String toString()
	{
		return Console.format("ArrayMapEntry{mType=%s, mKey=%s, mValue=%s}", mType, (mKey == null ? "null" : "\"" + new String(mKey.array()).replaceAll("[^\\w]*", "") + "\""), (mValue == null ? "null" : "\"" + new String(mValue).replace('\u0000', '.').replaceAll("[^\\w\\.]*", "") + "\""));
	}
}
