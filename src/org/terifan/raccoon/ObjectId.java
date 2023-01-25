package org.terifan.raccoon;

import java.io.Serializable;
import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;
import static javafx.scene.input.KeyCode.H;


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


	private final static String chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
	private final static BigInteger shift = new BigInteger("4294967296");
	private final static BigInteger scale = new BigInteger("62");
	private final static HashMap<Character,BigInteger> lookup = new HashMap<>();
	static
	{
		for (char i = '0'; i <= '9'; i++)
		{
			lookup.put(i, BigInteger.valueOf(i-'0'));
		}
		for (char i = 'A'; i <= 'Z'; i++)
		{
			lookup.put(i, BigInteger.valueOf(10+i-'A'));
		}
		for (char i = 'a'; i <= 'z'; i++)
		{
			lookup.put(i, BigInteger.valueOf(10+26+i-'a'));
		}
	}


	public static ObjectId fromString(String aName)
	{
		BigInteger bi = BigInteger.ZERO;

		for (int i = 0; i < 17; i++)
		{
			bi = bi.multiply(scale).add(lookup.get(aName.charAt(i)));
		}

		BigInteger aa = bi.divide(shift);
		int c = bi.mod(shift).intValue();
		int b = aa.mod(shift).intValue();
		int a = aa.divide(shift).mod(shift).intValue();

		return new ObjectId(a, b, c);

//		return new ObjectId(Integer.parseUnsignedInt(aName.substring(0, 8), 16), Integer.parseUnsignedInt(aName.substring(8, 16), 16), Integer.parseUnsignedInt(aName.substring(16, 24), 16));
	}


	@Override
	public String toString()
	{
		StringBuilder out = new StringBuilder();

		BigInteger b = BigInteger.valueOf(0xffffffffL & mTime)
			.multiply(shift)
			.add(BigInteger.valueOf(0xffffffffL & mConstant))
			.multiply(shift)
			.add(BigInteger.valueOf(0xffffffffL & mSequence));

		for (int i = 0; i < 17; i++)
		{
			out.insert(0, chars.charAt(b.mod(scale).intValue()));
			b = b.divide(scale);
		}

		return out.toString();

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
			long t = System.currentTimeMillis();
			for (int i = 0; i < 1000_000; i++)
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
