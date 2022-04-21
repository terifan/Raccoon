package org.terifan.raccoon;

import java.util.Arrays;
import java.util.TreeSet;


public class MarshalledKey implements Comparable<MarshalledKey>
{
	private static final byte REAL = (byte)0xfe;
	private static final byte FIRST = (byte)0xff;

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
		mBuffer[mBuffer.length - 1] = REAL;
	}


	public MarshalledKey(boolean aFirstKey)
	{
		mBuffer = new byte[]{(byte)(aFirstKey ? FIRST : REAL)};
	}


	public boolean isFirst()
	{
		return mBuffer[mBuffer.length - 1] == FIRST;
	}


	public byte[] getContent()
	{
		if (mBuffer[mBuffer.length - 1] != REAL && mBuffer[mBuffer.length - 1] != FIRST)
		{
			throw new IllegalArgumentException();
		}
		return Arrays.copyOfRange(mBuffer, 0, mBuffer.length - 1);
	}


	public byte[] marshall()
	{
		if (mBuffer[mBuffer.length - 1] != REAL && mBuffer[mBuffer.length - 1] != FIRST)
		{
			throw new IllegalArgumentException();
		}
		return mBuffer.clone();
	}


	public static MarshalledKey unmarshall(byte[] aBuffer)
	{
		if (aBuffer[aBuffer.length - 1] != REAL && aBuffer[aBuffer.length - 1] != FIRST)
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

		if (self[self.length - 1] == FIRST) {System.out.println("#");return other[other.length - 1] == FIRST ? 0 : 1;}
		if (other[other.length - 1] == FIRST) {System.out.println("*");return -1;}

		for (int i = 0, len = Math.min(self.length, other.length); i < len; i++)
		{
			int c = self[i] - other[i];
			if (c != 0)
			{
				return c;
			}
		}

		if (self.length > other.length) return 1;
		if (self.length < other.length) return -1;

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
			ArrayMap arrayMap = new ArrayMap(1000);
			TreeSet<MarshalledKey> db = new TreeSet<>();

//			insert(arrayMap, db, "n");
//			insert(arrayMap, db);

//			insert(arrayMap, db, "a");
//			insert(arrayMap, db, "q");
//			insert(arrayMap, db, "l");
//			insert(arrayMap, db, "n");
//			insert(arrayMap, db, "x");
//			insert(arrayMap, db, "i");
//			insert(arrayMap, db, "j");
//			insert(arrayMap, db, "kkk");
//			insert(arrayMap, db, "m");
//			insert(arrayMap, db, "b");
//			insert(arrayMap, db, "ddd");
//			insert(arrayMap, db, "e");
//			insert(arrayMap, db, "f");
//			insert(arrayMap, db, "g");
//			insert(arrayMap, db, "r");
//			insert(arrayMap, db, "z");
//			insert(arrayMap, db, "y");
//			insert(arrayMap, db, "h");
//			insert(arrayMap, db, "w");
//			insert(arrayMap, db, "s");
//			insert(arrayMap, db, "t");
//			insert(arrayMap, db, "u");
//			insert(arrayMap, db, "v");
//			insert(arrayMap, db, "c");
//			insert(arrayMap, db, "p");
//			insert(arrayMap, db, "o");

//

			insert(arrayMap, db, "Banana");
			insert(arrayMap, db, "Nose");
			insert(arrayMap, db, "Urban");
			insert(arrayMap, db, "Vapor");
			insert(arrayMap, db, "Gloves");
			insert(arrayMap, db, "Female");
			insert(arrayMap, db, "Mango");
			insert(arrayMap, db, "Xenon");
			insert(arrayMap, db, "Yellow");
			insert(arrayMap, db, "Open");
			insert(arrayMap, db, "Japanese");
			insert(arrayMap, db, "Knife");
			insert(arrayMap, db, "Apple");
			insert(arrayMap, db, "Dove");
			insert(arrayMap, db, "Ear");
			insert(arrayMap, db, "Leap");
			insert(arrayMap, db, "Quality");
			insert(arrayMap, db, "Head");
			insert(arrayMap, db, "Rupee");
			insert(arrayMap, db, "Whale");
			insert(arrayMap, db, "Turquoise");
			insert(arrayMap, db, "Circus");
			insert(arrayMap, db, "Internal");
			insert(arrayMap, db, "Jalapeno");
			insert(arrayMap, db, "Silver");
			insert(arrayMap, db, "Zebra");
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.out);
		}
	}


	private static void insert(ArrayMap aArrayMap, TreeSet<MarshalledKey> aMap, String aKey)
	{
		aArrayMap.put(new ArrayMapEntry(new MarshalledKey(aKey.getBytes()).marshall(), new byte[0], (byte)0), null);
		aMap.add(new MarshalledKey(aKey.getBytes()));
		System.out.println(aArrayMap);
		System.out.println(aMap);
	}


	private static void insert(ArrayMap aArrayMap, TreeSet<MarshalledKey> aMap)
	{
		aArrayMap.put(new ArrayMapEntry(new MarshalledKey(true).marshall(), new byte[0], (byte)0), null);
		aMap.add(new MarshalledKey(true));
		System.out.println(aArrayMap);
		System.out.println(aMap);
	}
}
