package org.terifan.raccoon;

import java.util.Arrays;
import java.util.HashSet;
import java.util.TreeSet;


public class MarshalledKey implements Comparable<MarshalledKey>
{
	private static final byte PREV = (byte)0xfe;
	private static final byte LAST = (byte)0xff;

	private byte[] mBuffer;


	private MarshalledKey()
	{
	}


	public MarshalledKey(byte[] aKey)
	{
		if (aKey[aKey.length - 1] == 0 || aKey[aKey.length - 1] == 1)
		{
			throw new IllegalArgumentException();
		}
		mBuffer = Arrays.copyOfRange(aKey, 0, aKey.length + 1);
		mBuffer[mBuffer.length - 1] = PREV;
	}


	public MarshalledKey(boolean aFinalKey)
	{
		mBuffer = new byte[]{(byte)(aFinalKey ? LAST : PREV)};
	}


	public boolean isFinalKey()
	{
		return mBuffer[mBuffer.length - 1] == LAST;
	}


	public byte[] getContent()
	{
		if (mBuffer[mBuffer.length - 1] != PREV && mBuffer[mBuffer.length - 1] != LAST)
		{
			throw new IllegalArgumentException();
		}
		return Arrays.copyOfRange(mBuffer, 0, mBuffer.length - 1);
	}


	public byte[] marshall()
	{
		if (mBuffer[mBuffer.length - 1] != PREV && mBuffer[mBuffer.length - 1] != LAST)
		{
			throw new IllegalArgumentException();
		}
		return mBuffer.clone();
	}


	public static MarshalledKey unmarshall(byte[] aBuffer)
	{
		if (aBuffer[aBuffer.length - 1] != PREV && aBuffer[aBuffer.length - 1] != LAST)
		{
			throw new IllegalArgumentException();
		}
		MarshalledKey k = new MarshalledKey();
		k.mBuffer = aBuffer;
		return k;
	}


	@Override
	public int compareTo(MarshalledKey aOther)
	{
		byte[] other = aOther.mBuffer;
		byte[] self = mBuffer;

		if (self[self.length - 1] == LAST) {System.out.println("#");return other[other.length - 1] == LAST ? 0 : 1;}
		if (other[other.length - 1] == LAST) {System.out.println("*");return -1;}

		for (int i = 0, len = Math.min(self.length, other.length); i < len; i++)
		{
			int c = self[i] - other[i];
			if (c != 0)
			{
		System.out.println("!");
				return c;
			}
		}
		System.out.println("? " + other.length+" < "+self.length);

//		if (self.length > other.length) return 1;
//		if (self.length < other.length) return -1;

		return 0;
	}


	@Override
	public int hashCode()
	{
		int hash = 7;
		hash = 97 * hash + Arrays.hashCode(mBuffer);
		return hash;
	}


	@Override
	public boolean equals(Object obj)
	{
		if (this == obj)
		{
			return true;
		}
		if (obj == null)
		{
			return false;
		}
		if (getClass() != obj.getClass())
		{
			return false;
		}
		final MarshalledKey other = (MarshalledKey)obj;
		if (!Arrays.equals(this.mBuffer, other.mBuffer))
		{
			return false;
		}
		return true;
	}


	@Override
	public String toString()
	{
		return new String(mBuffer);
	}


	public static void main(String ... args)
	{
		try
		{
			TreeSet<MarshalledKey> db = new TreeSet<>();

			insert(db, "a");
			insert(db, "q");
			insert(db, "l");
			insert(db, "n");
			insert(db, "x");
			insert(db, "i");
			insert(db, "j");
			insert(db, "kkk");
			insert(db, "m");
			insert(db, "b");
			insert(db, "ddd");
			insert(db, "e");
			insert(db, "f");
			insert(db, "g");
			insert(db, "r");
			insert(db, "z");
			insert(db, "y");
			insert(db, "h");
			insert(db, "w");
			insert(db, "s");
			insert(db, "t");
			insert(db, "u");
			insert(db, "v");
			insert(db, "c");
			insert(db, "p");
			insert(db, "o");

//			insert(db, "a");
//			insert(db, "q");
//			insert(db, "l");
//			insert(db, "n");
//			insert(db, "x");
//			insert(db, "i");
//			insert(db, "j");
//			insert(db, "kkk");
//			insert(db, "m");
//			insert(db, "b");
//			insert(db, "ddd");
//			insert(db, "e");
//			insert(db, "f");
//			insert(db, "g");
//			insert(db, "r");
//			insert(db, "z");
//			insert(db, "y");
//			insert(db, "h");
//			insert(db, "w");
//			insert(db, "s");
//			insert(db, "t");
//			insert(db, "u");
//			insert(db, "v");
//			insert(db, "c");
//			insert(db, "p");
//			insert(db, "o");

//			insert(db, "Apple");
//			insert(db, "Banana");
//			insert(db, "Circus");
//			insert(db, "Dove");
//			insert(db, "Ear");
//			insert(db, "Female");
//			insert(db, "Gloves");
//			insert(db, "Head");
//			insert(db, "Internal");
//			insert(db, "Jalapeno");
//			insert(db, "Japanese");
//			insert(db, "Knife");
//			insert(db, "Leap");
//			insert(db, "Mango");
//			insert(db, "Nose");
//			insert(db, "Open");
//			insert(db, "Quality");
//			insert(db, "Rupee");
//			insert(db, "Silver");
//			insert(db, "Turquoise");
//			insert(db, "Urban");
//			insert(db, "Vapor");
//			insert(db, "Whale");
//			insert(db, "Xenon");
//			insert(db, "Yellow");
//			insert(db, "Zebra");
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.out);
		}
	}


	private static void insert(TreeSet<MarshalledKey> aMap, String aKey)
	{
		aMap.add(new MarshalledKey(aKey.getBytes()));
		System.out.println(aMap);
	}
}
