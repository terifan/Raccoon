package org.terifan.raccoon.btree;

import java.util.Arrays;
import java.util.UUID;
import org.terifan.raccoon.document.Array;


// long, String, UUID, ObjectId, byte[], Document, Array
public class _ArrayMapKey
{
//	public final static ArrayMapKey EMPTY = new ArrayMapKey("");
//
//	private byte[] mSerialized;
//	private Object mInstance;
//
//
//	public ArrayMapKey()
//	{
//	}
//
//
//	public ArrayMapKey(Object aInstance)
//	{
//		mInstance = aInstance;
//		mSerialized = Array.of(mInstance).toByteArray();
//	}
//
//
//	public ArrayMapKey(byte[] aBuffer, int aOffset, int aLength)
//	{
//		mSerialized = Arrays.copyOfRange(aBuffer, aOffset, aOffset + aLength);
//	}


//	public byte[] array()
//	{
//		return mSerialized;
//	}
//
//
//	public int size()
//	{
//		return mSerialized.length;
//	}
//
//
//	public int compareTo(byte[] aBuffer, int aOffset, int aLength)
//	{
//		Object a = mInstance != null ? mInstance : mSerialized == null ? null : new Array().fromByteArray(mSerialized).get(0);
//		Object b = new Array().fromByteArray(Arrays.copyOfRange(aBuffer, aOffset, aOffset + aLength)).get(0);
//
//		if (a == null)
//		{
//			return -1;
//		}
//		if (a instanceof UUID v && b instanceof UUID w)
//		{
//			return compareUUID(v, w);
//		}
//		if (a.getClass() != b.getClass())
//		{
//			return a.toString().compareTo(b.toString());
//		}
//
//		return ((Comparable)a).compareTo(b);
//	}
//
//
//	public int compareTo(ArrayMapKey aOther)
//	{
//		Object a = get();
//		Object b = aOther.get();
//
//		if (a == null)
//		{
//			return -1;
//		}
//		if (a instanceof UUID v && b instanceof UUID w)
//		{
//			return compareUUID(v, w);
//		}
//		else if (a.getClass() != b.getClass())
//		{
//			return a.toString().compareTo(b.toString());
//		}
//
//		return ((Comparable)a).compareTo(b);
//	}
//
//
//	private static int compareUUID(UUID a, UUID b)
//	{
//		long t;
//		t = Long.compare(a.getMostSignificantBits() >>> 32 & 0xffffffffL, b.getMostSignificantBits() >>> 32 & 0xffffffffL);
//		if (t != 0)
//		{
//			return t < 0 ? -1 : 1;
//		}
//		t = Long.compare(a.getMostSignificantBits() & 0xffffffffL, b.getMostSignificantBits() & 0xffffffffL);
//		if (t != 0)
//		{
//			return t < 0 ? -1 : 1;
//		}
//		t = Long.compare(a.getLeastSignificantBits() >>> 32 & 0xffffffffL, b.getLeastSignificantBits() >>> 32 & 0xffffffffL);
//		if (t != 0)
//		{
//			return t < 0 ? -1 : 1;
//		}
//		t = Long.compare(a.getLeastSignificantBits() & 0xffffffffL, b.getLeastSignificantBits() & 0xffffffffL);
//		if (t != 0)
//		{
//			return t < 0 ? -1 : 1;
//		}
//		return 0;
//	}
//
//
//	@Override
//	public int hashCode()
//	{
//		return Arrays.hashCode(mSerialized);
//	}
//
//
//	@Override
//	public boolean equals(Object aOther)
//	{
//		if (aOther instanceof ArrayMapKey v)
//		{
//			return Arrays.equals(mSerialized, v.mSerialized);
//		}
//		return false;
//	}
//
//
//	@Override
//	public String toString()
//	{
//		Object value = get();
//
//		return value == null ? "null" : value.getClass().getSimpleName() + ": \"" + value + "\"";
//	}
//
//
//	public Object get()
//	{
//		return mInstance = (mInstance != null ? mInstance : mSerialized == null ? "null" : new Array().fromByteArray(mSerialized).get(0));
//	}
}
