package org.terifan.raccoon;

import java.io.Serializable;
import java.security.SecureRandom;
import java.util.concurrent.atomic.AtomicInteger;


/*
 *   0                   1                   2                   3
 *   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |                             time                              |
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |                           constant                            |
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |                           sequence                            |
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *
 * time      - 32 bits - time in seconds since midnight January 1, 1970 UTC
 * constant  - 32 bits - random value used in all intances per JVM instance
 * sequence  - 32 bits - incrementing counter, initialized to a random value
 */
public final class ObjectId implements Serializable, Comparable<ObjectId>
{
	private final static long serialVersionUID = 1;
	public final static int LENGTH = 12;

	private final int mTime;
	private final int mConstant;
	private final int mSequence;


	private static class Holder
	{
		final static SecureRandom PRNG = new SecureRandom();
		final static int CONSTANT = PRNG.nextInt();
		final static AtomicInteger SEQUENCE = new AtomicInteger(PRNG.nextInt());
	}


	private ObjectId(int aTime, int aConstant, int aSequence)
	{
		mTime = aTime;
		mConstant = aConstant;
		mSequence = aSequence;
	}


	public static ObjectId fromParts(int aTime, int aConstant, int aSequence)
	{
		return new ObjectId(aTime, aConstant, aSequence);
	}


	public static ObjectId randomId()
	{
		return new ObjectId((int)(System.currentTimeMillis() / 1000), Holder.CONSTANT, Holder.SEQUENCE.getAndIncrement());
	}


	public static ObjectId fromBytes(byte[] aBuffer)
	{
		if (aBuffer == null || aBuffer.length != LENGTH)
		{
			throw new IllegalArgumentException("data must be " + LENGTH + " bytes in length");
		}

		return new ObjectId(getInt(aBuffer, 0), getInt(aBuffer, 4), getInt(aBuffer, 8));
	}


	public long time()
	{
		return mTime * 1000L;
	}


	public int sequence()
	{
		return mSequence;
	}


	public int constant()
	{
		return mConstant;
	}


	public byte[] toByteArray()
	{
		byte[] buffer = new byte[LENGTH];
		toBytes(buffer, mTime, 0);
		toBytes(buffer, mConstant, 4);
		toBytes(buffer, mSequence, 8);
		return buffer;
	}


	public static ObjectId fromString(String aName)
	{
		return new ObjectId(Integer.parseUnsignedInt(aName.substring(0, 8), 16), Integer.parseUnsignedInt(aName.substring(8, 16), 16), Integer.parseUnsignedInt(aName.substring(16, 24), 16));
	}


	@Override
	public String toString()
	{
		return String.format("%08x%08x%08x", mTime, mConstant, mSequence);
	}


	@Override
	public int hashCode()
	{
		return mTime ^ mConstant ^ mSequence;
	}


	@Override
	public boolean equals(Object aOther)
	{
		if (aOther instanceof ObjectId)
		{
			ObjectId other = (ObjectId)aOther;
			return (mTime == other.mTime && mConstant == other.mConstant && mSequence == other.mSequence);
		}
		return false;
	}


	@Override
	public int compareTo(ObjectId aOther)
	{
		return
			mTime < aOther.mTime ? -1 :
				mTime > aOther.mTime ? 1 :
					mConstant < aOther.mConstant ? -1 :
						mConstant > aOther.mConstant ? 1 :
							mSequence < aOther.mSequence ? -1 :
								mSequence > aOther.mSequence ? 1 : 0;
	}


	private static int getInt(byte[] aBuffer, int aOffset)
	{
		int value = 0;
		for (int i = 0; i < 4; i++)
		{
			value = (value << 8) | (0xFF & aBuffer[aOffset++]);
		}
		return value;
	}


	private static void toBytes(byte[] aBuffer, int aValue, int aOffset)
	{
		aOffset += 4;
		for (int i = 0; i < 4; i++, aValue >>>= 8)
		{
			aBuffer[--aOffset] = (byte)aValue;
		}
	}


//	public static void main(String... args)
//	{
//		try
//		{
//			long t = System.currentTimeMillis();
//			for (int i = 0; i < 1000; i++)
//			{
//				ObjectId objectId = ObjectId.randomId();
//
//				System.out.printf("%s %10d %10d %s%n", objectId, 0xffffffffL&objectId.constant(), 0xffffffffL&objectId.sequence(), new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(objectId.time()));
//
//				if (!ObjectId.fromString(objectId.toString()).equals(objectId)) System.out.println("#");
//				if (!ObjectId.fromBytes(objectId.toByteArray()).equals(objectId)) System.out.println("#");
//			}
//			System.out.println(System.currentTimeMillis() - t);
//		}
//		catch (Throwable e)
//		{
//			e.printStackTrace(System.out);
//		}
//	}
}
