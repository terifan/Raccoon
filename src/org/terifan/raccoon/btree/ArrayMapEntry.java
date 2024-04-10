package org.terifan.raccoon.btree;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.UUID;
import org.terifan.raccoon.blockdevice.BlockPointer;
import static org.terifan.raccoon.blockdevice.util.ByteArrayUtil.getInt64;
import static org.terifan.raccoon.blockdevice.util.ByteArrayUtil.putInt64;
import org.terifan.raccoon.document.Array;
import org.terifan.raccoon.document.Document;
import org.terifan.raccoon.document.ObjectId;


public class ArrayMapEntry implements Comparable<ArrayMapEntry>
{
	private Type mKeyType;
	private Type mValueType;
	private byte[] mKey;
	private byte[] mValue;
	private OpState mState;


	public static enum Type
	{
		FIRST,
		OBJECTID,
		LONG,
		STRING,
		DOCUMENT,
		ARRAY,
		UUID,
		DOUBLE,
		BYTEARRAY,
		BLOCKPOINTER
	}


	public ArrayMapEntry()
	{
	}


	public int getMarshalledLength()
	{
		return 2 + mKey.length + mValue.length;
	}


	int getMarshalledKeyLength()
	{
		return 1 + mKey.length;
	}


	int getMarshalledValueLength()
	{
		return 1 + mValue.length;
	}


	public ArrayMapEntry setMarshalledKey(byte[] aBuffer, int aOffset, int aLength)
	{
		mKeyType = Type.values()[aBuffer[aOffset]];
		mKey = Arrays.copyOfRange(aBuffer, aOffset + 1, aOffset + aLength);
		return this;
	}


	public ArrayMapEntry setMarshalledValue(byte[] aBuffer, int aOffset, int aLength)
	{
		mValueType = Type.values()[aBuffer[aOffset]];
		mValue = Arrays.copyOfRange(aBuffer, aOffset + 1, aOffset + aLength);
		return this;
	}


	public byte[] getKey()
	{
		return mKey;
	}


	void getMarshalledKey(byte[] aBuffer, int aOffset)
	{
		aBuffer[aOffset] = (byte)mKeyType.ordinal();
		System.arraycopy(mKey, 0, aBuffer, aOffset + 1, mKey.length);
	}


	void getMarshalledValue(byte[] aBuffer, int aOffset)
	{
		aBuffer[aOffset] = (byte)mValueType.ordinal();
		System.arraycopy(mValue, 0, aBuffer, aOffset + 1, mValue.length);
	}


	public String toKeyString()
	{
		return mKey == null ? "null" : new String(mKey).replaceAll("[^\\w\\-\\_\\:;@\\.,= ]*", "");
	}


	public Document getInstance()
	{
		return ((Document)getValueInstance()).put("_id", getKeyInstance());
	}


	@SuppressWarnings("unchecked")
	public <T> T getKeyInstance()
	{
		switch (mKeyType)
		{
			case OBJECTID: return (T)ObjectId.fromByteArray(mKey);
			case LONG: return (T)(Long)getInt64(mKey, 0);
			case STRING: return (T)new String(mKey, Charset.defaultCharset());
			case DOCUMENT: return (T)new Document().fromByteArray(mKey);
			case ARRAY: return (T)new Array().fromByteArray(mKey);
			case BLOCKPOINTER: return (T)BlockPointer.fromByteArray(mKey);
			case UUID: return (T)new UUID(getInt64(mKey, 0), getInt64(mKey, 8));
			case DOUBLE: return (T)(Double)Double.longBitsToDouble(getInt64(mKey, 0));
			case BYTEARRAY: return (T)mKey;
			default: throw new Error();
		}
	}


	public ArrayMapEntry setKeyInstance(Object aKey)
	{
		if (aKey instanceof ObjectId v) return setKey(v.toByteArray(), Type.OBJECTID);
		if (aKey instanceof Long v) return setKey(putInt64(new byte[8], 0, v), Type.LONG);
		if (aKey instanceof Integer v) return setKey(putInt64(new byte[8], 0, v), Type.LONG);
		if (aKey instanceof Short v) return setKey(putInt64(new byte[8], 0, v), Type.LONG);
		if (aKey instanceof Byte v) return setKey(putInt64(new byte[8], 0, v), Type.LONG);
		if (aKey instanceof Double v) return setKey(putInt64(new byte[8], 0, Double.doubleToLongBits(v)), Type.DOUBLE);
		if (aKey instanceof Float v) return setKey(putInt64(new byte[8], 0, Double.doubleToLongBits(v)), Type.DOUBLE);
		if (aKey instanceof String v) return setKey(v.getBytes(Charset.defaultCharset()), Type.STRING);
		if (aKey instanceof Document v) return setKey(v.toByteArray(), Type.DOCUMENT);
		if (aKey instanceof Array v) return setKey(v.toByteArray(), Type.ARRAY);
		if (aKey instanceof byte[] v) return setKey(v, Type.BYTEARRAY);
		if (aKey instanceof UUID v) return setKey(putInt64(putInt64(new byte[16], 0, v.getMostSignificantBits()), 8, v.getLeastSignificantBits()), Type.UUID);
		if (aKey instanceof BlockPointer v) return setKey(v.toByteArray(), Type.BLOCKPOINTER);
		throw new Error(aKey == null ? "null": aKey.getClass().toString());
	}


	ArrayMapEntry setKey(ArrayMapEntry aEntry)
	{
		mKey = aEntry.mKey.clone();
		mKeyType = aEntry.mKeyType;
		return this;
	}


	public ArrayMapEntry setKey(byte[] aKey, Type aType)
	{
		mKeyType = aType;
		mKey = aKey;
		return this;
	}


	public byte[] getValue()
	{
		return mValue;
	}


	public ArrayMapEntry setValue(byte[] aValue, Type aType)
	{
		mValueType = aType;
		mValue = aValue;
		return this;
	}


	public ArrayMapEntry setValueInstance(Object aValue)
	{
		if (aValue instanceof Document v) setValue(v.toByteArray(), Type.DOCUMENT);
		else if (aValue instanceof BlockPointer v) setValue(v.toByteArray(), Type.BLOCKPOINTER);
//		else if (aValue instanceof byte[] v) setValue(v, Type.BYTEARRAY);
//		else if (aValue instanceof ObjectId v) setValue(v.toByteArray(), Type.OBJECTID);
//		else if (aValue instanceof String v) setValue(v.getBytes(Charset.defaultCharset()), Type.STRING);
//		else if (aValue instanceof Array v) setValue(v.toByteArray(), Type.ARRAY);
//		else if (aValue instanceof Long v) setValue(putInt64(new byte[8], 0, v), Type.LONG);
//		else if (aValue instanceof Integer v) setValue(putInt64(new byte[8], 0, v), Type.LONG);
//		else if (aValue instanceof Short v) setValue(putInt64(new byte[8], 0, v), Type.LONG);
//		else if (aValue instanceof Byte v) setValue(putInt64(new byte[8], 0, v), Type.LONG);
//		else if (aValue instanceof Double v) setValue(putInt64(new byte[8], 0, Double.doubleToLongBits(v)), Type.DOUBLE);
//		else if (aValue instanceof Float v) setValue(putInt64(new byte[8], 0, Double.doubleToLongBits(v)), Type.DOUBLE);
//		else if (aValue instanceof UUID v) setValue(putInt64(putInt64(new byte[16], 0, v.getMostSignificantBits()), 8, v.getLeastSignificantBits()), Type.UUID);
		else throw new Error();
		return this;
	}


	@SuppressWarnings("unchecked")
	public <T> T getValueInstance()
	{
		if (mValueType == null) throw new Error("value type is null");
		switch (mValueType)
		{
			case DOCUMENT: return (T)new Document().fromByteArray(mValue);
			case BLOCKPOINTER: return (T)BlockPointer.fromByteArray(mValue);
//			case BYTEARRAY: return (T)mValue;
//			case OBJECTID: return (T)ObjectId.fromByteArray(mValue);
//			case LONG: return (T)(Long)getInt64(mValue, 0);
//			case STRING: return (T)new String(mValue, Charset.defaultCharset());
//			case ARRAY: return (T)new Array().fromByteArray(mValue);
//			case UUID: return (T)new UUID(getInt64(mValue, 0), getInt64(mValue, 8));
//			case DOUBLE: return (T)(Double)Double.longBitsToDouble(getInt64(mValue, 0));
			default: throw new Error();
		}
	}


	public ArrayMapEntry setKey(Document aDocument)
	{
		setKeyInstance(aDocument.get("_id"));
		return this;
	}


	public ArrayMapEntry setKeyAndValue(Document aDocument)
	{
		setKeyInstance(aDocument.get("_id"));
		mValue = aDocument.toByteArray(field -> !field.equals("_id"));
		mValueType = Type.DOCUMENT;
		return this;
	}


	public Type getKeyType()
	{
		return mKeyType;
	}


	public Type getValueType()
	{
		return mValueType;
	}


	public OpState getState()
	{
		return mState;
	}


	public void setState(OpState aState)
	{
		mState = aState;
	}


	@Override
	public int compareTo(ArrayMapEntry aEntry)
	{
		if (mKeyType != aEntry.mKeyType)
		{
			return mKeyType.ordinal() < aEntry.mKeyType.ordinal() ? -1 : mKeyType.ordinal() > aEntry.mKeyType.ordinal() ? 1 : 0;
		}
		if (mKeyType == Type.FIRST)
		{
			return mKeyType == aEntry.mKeyType ? 0 : -1;
		}
		return compareToImpl(aEntry.mKey, 0, aEntry.mKey.length);
	}


	int compareToIncludingType(byte[] aOtherKey, int aOffset, int aLength)
	{
		byte otherKeyType = aOtherKey[aOffset];
		if (mKeyType.ordinal() != otherKeyType)
		{
			return mKeyType.ordinal() < otherKeyType ? -1 : mKeyType.ordinal() > otherKeyType ? 1 : 0;
		}
		if (mKeyType == Type.FIRST)
		{
			return otherKeyType == Type.FIRST.ordinal() ? 0 : -1;
		}
		return compareToImpl(aOtherKey, aOffset + 1, aLength - 1);
	}


	private int compareToImpl(byte[] aOtherKey, int aOffset, int aLength)
	{
		return switch (mKeyType)
		{
			case FIRST -> throw new Error();
			case OBJECTID -> ObjectId.compare(mKey, 0, aOtherKey, aOffset);
			case LONG -> Long.compare(getInt64(mKey, 0), getInt64(aOtherKey, aOffset));
			case STRING -> new String(mKey).compareTo(new String(aOtherKey, aOffset, aLength, Charset.defaultCharset()));
			case DOCUMENT -> new Document().fromByteArray(mKey).compareTo(new Document().fromByteArray(aOtherKey, aOffset, aLength));
			case ARRAY -> new Array().fromByteArray(mKey).compareTo(new Array().fromByteArray(aOtherKey, aOffset, aLength));
			case BYTEARRAY -> Arrays.compare(mKey, 0, mKey.length, aOtherKey, aOffset, aOffset + aLength);
			case UUID -> Arrays.compare(mKey, 0, 16, aOtherKey, aOffset, aOffset + 16);
			case BLOCKPOINTER -> Arrays.compare(mKey, 0, mKey.length, aOtherKey, aOffset, aOffset + aLength); // same as array of bytes
			case DOUBLE -> Double.compare(Double.longBitsToDouble(getInt64(mKey, 0)), Double.longBitsToDouble(getInt64(aOtherKey, aOffset)));
			default -> throw new Error();
		};
	}


	@Override
	public String toString()
	{
		return mKeyType + "=" + getKeyInstance() + ", " + (mValueType == null ? "null" : mValueType) + "=" + getValueInstance();
	}
}