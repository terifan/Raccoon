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


	public <T> T getKeyInstance()
	{
		return (T)switch (mKeyType)
		{
			case Type.OBJECTID -> ObjectId.fromByteArray(mKey);
			case Type.LONG -> getInt64(mKey, 0);
			case Type.STRING -> new String(mKey, Charset.defaultCharset());
			case Type.DOCUMENT -> new Document().fromByteArray(mKey);
			case Type.ARRAY -> new Array().fromByteArray(mKey);
			case Type.BLOCKPOINTER -> BlockPointer.fromByteArray(mKey);
			case Type.UUID -> new UUID(getInt64(mKey, 0), getInt64(mKey, 8));
			case Type.BYTEARRAY -> mKey;
			default -> throw new Error();
		};
	}


	public ArrayMapEntry setKeyInstance(Object aKey)
	{
		return switch (aKey)
		{
			case ObjectId v -> setKey(v.toByteArray(), Type.OBJECTID);
			case Long v -> setKey(putInt64(new byte[8], 0, v), Type.LONG);
			case String v -> setKey(v.getBytes(Charset.defaultCharset()), Type.STRING);
			case Document v -> setKey(v.toByteArray(), Type.DOCUMENT);
			case Array v -> setKey(v.toByteArray(), Type.ARRAY);
			case BlockPointer v -> setKey(v.toByteArray(), Type.BLOCKPOINTER);
			case UUID v -> setKey(putInt64(putInt64(new byte[16], 0, v.getMostSignificantBits()), 8, v.getLeastSignificantBits()), Type.UUID);
			case byte[] v -> setKey(v, Type.BYTEARRAY);
			default -> throw new Error();
		};
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
		return switch (aValue)
		{
			case Document v -> setValue(v.toByteArray(), Type.DOCUMENT);
			case BlockPointer v -> setValue(v.toByteArray(), Type.BLOCKPOINTER);
			case ObjectId v -> setValue(v.toByteArray(), Type.OBJECTID);
			case Long v -> setValue(putInt64(new byte[8], 0, v), Type.LONG);
			case String v -> setValue(v.getBytes(Charset.defaultCharset()), Type.STRING);
			case Array v -> setValue(v.toByteArray(), Type.ARRAY);
			case UUID v -> setValue(putInt64(putInt64(new byte[16], 0, v.getMostSignificantBits()), 8, v.getLeastSignificantBits()), Type.UUID);
			case byte[] v -> setValue(v, Type.BYTEARRAY);
			default -> throw new Error();
		};
	}


	public <T> T getValueInstance()
	{
		return (T)switch (mValueType)
		{
			case null -> throw new Error("value type is null");
			case Type.OBJECTID -> ObjectId.fromByteArray(mValue);
			case Type.LONG -> getInt64(mValue, 0);
			case Type.STRING -> new String(mValue, Charset.defaultCharset());
			case Type.DOCUMENT -> new Document().fromByteArray(mValue);
			case Type.ARRAY -> new Array().fromByteArray(mValue);
			case Type.BLOCKPOINTER -> BlockPointer.fromByteArray(mValue);
			case Type.UUID -> new UUID(getInt64(mValue, 0), getInt64(mValue, 8));
			case Type.BYTEARRAY -> mValue;
			default -> throw new Error();
		};
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
			case Type.FIRST -> throw new Error();
			case Type.OBJECTID -> ObjectId.compare(mKey, 0, aOtherKey, aOffset);
			case Type.LONG -> Long.compare(getInt64(mKey, 0), getInt64(aOtherKey, aOffset));
			case Type.STRING -> new String(mKey).compareTo(new String(aOtherKey, aOffset, aLength, Charset.defaultCharset()));
			case Type.DOCUMENT -> new Document().fromByteArray(mKey).compareTo(new Document().fromByteArray(aOtherKey, aOffset, aLength));
			case Type.ARRAY -> new Array().fromByteArray(mKey).compareTo(new Array().fromByteArray(aOtherKey, aOffset, aLength));
			case Type.BYTEARRAY -> Arrays.compare(mKey, 0, mKey.length, aOtherKey, aOffset, aOffset + aLength);
			case Type.UUID -> Arrays.compare(mKey, 0, 16, aOtherKey, aOffset, aOffset + 16);
			case Type.BLOCKPOINTER -> Arrays.compare(mKey, 0, mKey.length, aOtherKey, aOffset, aOffset + aLength); // same as array of bytes
			default -> throw new Error();
		};
	}


	@Override
	public String toString()
	{
		return mKeyType + "=" + getKeyInstance() + ", " + (mValueType == null ? "null" : mValueType) + "=" + getValueInstance();
	}
}