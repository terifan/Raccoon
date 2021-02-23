package org.terifan.raccoon;

import java.util.Arrays;


/**
 * [header]
 *   n bytes - part count, variable length encoded
 * [list of part lengths]
 *   (entry 1..n)
 *   n bytes - value length, variable length encoded
 * [list of part values]
 *   (entry 1..n)
 *   n bytes - value
 */
public final class ArrayMapMultipartEntry
{
//	private byte[] mKey;
//	private byte[] mValue;
//
//
//	public ArrayMapMultipartEntry2()
//	{
//	}
//
//
//	public ArrayMapMultipartEntry2(byte[] aKey)
//	{
//		mKey = aKey;
//	}
//
//
//	public ArrayMapMultipartEntry2(byte[] aKey, byte[] aValue)
//	{
//		mKey = aKey;
//		mValue = aValue;
//	}
//
//
//	public byte[] getKey()
//	{
//		return mKey;
//	}
//
//
//	public void setKey(byte[] aKey)
//	{
//		mKey = aKey;
//	}
//
//
//	public byte[] getValue(byte[] aBuffer, int aOffset)
//	{
//		System.arraycopy(mValue, 0, aBuffer, aOffset, mValue.length);
//		return aBuffer;
//	}
//
//
//	public void setValue(byte[] aBuffer, int aOffset, int aLength)
//	{
//		mValue = Arrays.copyOfRange(aBuffer, aOffset, aOffset + aLength);
//	}
//
//
//	public int getValueLength()
//	{
//		return mValue.length;
//	}
}
