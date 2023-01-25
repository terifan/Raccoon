package org.terifan.raccoon;

import java.io.Serializable;
import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Random;
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
		final static SecureRandom numberGenerator = new SecureRandom();
		final static int constant = numberGenerator.nextInt();
		final static AtomicInteger sequence = new AtomicInteger(numberGenerator.nextInt());
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
		return new ObjectId((int)(System.currentTimeMillis() / 1000), Holder.constant, Holder.sequence.getAndIncrement());
	}


	public static ObjectId fromBytes(byte[] aData)
	{
		if (aData == null || aData.length != LENGTH)
		{
			throw new IllegalArgumentException("data must be " + LENGTH + " bytes in length");
		}

		int ts = 0;
		int cst = 0;
		int seq = 0;
		for (int i = 0; i < 4; i++)
		{
			ts = (ts << 8) | (0xFF & aData[i]);
		}
		for (int i = 4; i < 8; i++)
		{
			cst = (cst << 8) | (0xFF & aData[i]);
		}
		for (int i = 8; i < 12; i++)
		{
			seq = (seq << 8) | (0xFF & aData[i]);
		}

		return new ObjectId(ts, cst, seq);
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
		for (int i = 0; i < 4; i++)
		{
			buffer[i] = (byte)(mTime >>> (8 * (3 - i)));
		}
		for (int i = 4; i < 8; i++)
		{
			buffer[i] = (byte)(mConstant >>> (8 * (7 - i)));
		}
		for (int i = 8; i < 12; i++)
		{
			buffer[i] = (byte)(mSequence >>> (8 * (11 - i)));
		}
		return buffer;
	}


	public static ObjectId fromString(String aName)
	{
		int[] values = StrongBase62.decode(aName);

		return new ObjectId(values[0], values[1], values[2]);

//		return new ObjectId(Integer.parseUnsignedInt(aName.substring(0, 8), 16), Integer.parseUnsignedInt(aName.substring(8, 16), 16), Integer.parseUnsignedInt(aName.substring(16, 24), 16));
	}


	@Override
	public String toString()
	{
		return StrongBase62.encode(mTime, mConstant, mSequence);

//		return String.format("%08x%08x%08x", mTime, mConstant, mSequence);
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


	public static void main(String... args)
	{
		try
		{
//			BigInteger m = new BigInteger("4294967296").multiply(new BigInteger("4294967296")).multiply(new BigInteger("4294967296"));
//			for (int z = 0; z < 37; z++)
//			{
//			BigInteger n = m.multiply(BigInteger.valueOf(37)).add(BigInteger.valueOf(z));
////			System.out.println(m);
////			System.out.println(n);
////			System.out.println(new BigInteger("62").pow(17));
////			System.out.println(n.divide(BigInteger.valueOf(37)));
//			System.out.println(n.mod(BigInteger.valueOf(37)));
//			}

			long t = System.currentTimeMillis();
			for (int i = 0; i < 100; i++)
			{
				ObjectId objectId = ObjectId.randomId();

				System.out.printf("%s %10d %10d %s%n", objectId, 0xffffffffL&objectId.constant(), 0xffffffffL&objectId.sequence(), new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(objectId.time()));

				if (!ObjectId.fromString(objectId.toString()).equals(objectId)) System.out.println("#");
//				if (!ObjectId.fromBytes(objectId.toByteArray()).equals(objectId)) System.out.println("#");
			}
			System.out.println(System.currentTimeMillis() - t);
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.out);
		}
	}
}
