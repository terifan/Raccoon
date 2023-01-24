package org.terifan.raccoon;

import java.io.Serializable;
import java.security.SecureRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;


/*
 * UUID Version 7
 *
 *   0                   1                   2                   3
 *   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |                              time                             |
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |              time             |  ver  |       sequence        |
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |var|                        random                             |
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |                            random                             |
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *
 * time     - 48 bits - time in milliseconds since midnight January 1, 1970 UTC
 * ver      -  4 bits - constant 7
 * sequence - 12 bits - incrementing counter, initialized to a random value
 * var      -  2 bits - constant 2
 * random   - 62 bits - random value
 */
public final class ObjectId implements Serializable, Comparable<ObjectId>
{
	private final static long serialVersionUID = 1;

	private final long mMostSigBits;
	private final long mLeastSigBits;


	private static class Holder
	{
		final static SecureRandom numberGenerator = new SecureRandom();
		final static AtomicInteger sequence = new AtomicInteger(numberGenerator.nextInt());
		final static Pattern pattern = Pattern.compile("[0-9a-zA-Z]{6}\\-[0-9a-zA-Z]{4}\\-[0-9a-zA-Z]{4}\\-[0-9a-zA-Z]{4}\\-[0-9a-zA-Z]{14}");
	}


	public ObjectId(long aMostSigBits, long aLeastSigBits)
	{
		if (((int)(aMostSigBits >> 12) & 0xF) != 7)
		{
			throw new IllegalArgumentException("not a type 7 UUID");
		}
		if (((int)(aLeastSigBits >>> 62)) != 2)
		{
			throw new IllegalArgumentException("not valid variant");
		}

		mMostSigBits = aMostSigBits;
		mLeastSigBits = aLeastSigBits;
	}


	public static ObjectId randomId()
	{
		return new ObjectId((System.currentTimeMillis() << 16) | 0x7000 | (0xFFF & Holder.sequence.getAndIncrement()), 0xA000000000000000L | (Holder.numberGenerator.nextLong() & 0x3FFFFFFFFFFFFFFFL));
	}


	public static ObjectId fromBytes(byte[] aData)
	{
		if (aData == null || aData.length != 16)
		{
			throw new IllegalArgumentException("data must be 16 bytes in length");
		}

		long msb = 0;
		long lsb = 0;
		for (int i = 0; i < 8; i++)
		{
			msb = (msb << 8) | (0xFF & aData[i]);
		}
		for (int i = 8; i < 16; i++)
		{
			lsb = (lsb << 8) | (0xFF & aData[i]);
		}

		return new ObjectId(msb, lsb);
	}


	public static ObjectId fromString(String aName)
	{
		if (!Holder.pattern.matcher(aName).find())
		{
			throw new IllegalArgumentException("invalid format");
		}

		return new ObjectId(Long.parseUnsignedLong(aName.substring(0, 6) + aName.substring(7, 11) + aName.substring(12, 16) + aName.substring(17, 19), 16), Long.parseUnsignedLong(aName.substring(19, 21) + aName.substring(22), 16));
	}


	public long getLeastSignificantBits()
	{
		return mLeastSigBits;
	}


	public long getMostSignificantBits()
	{
		return mMostSigBits;
	}


	public int version()
	{
		return (int)(mMostSigBits >> 12) & 0xF;
	}


	public int variant()
	{
		return (int)(mLeastSigBits >>> 62);
	}


	public long timestamp()
	{
		return mMostSigBits >>> 16;
	}


	public int sequence()
	{
		return (int)mMostSigBits & 0xFFF;
	}


	public long random()
	{
		return mLeastSigBits & 0x3FFFFFFFFFFFFFFFL;
	}


	public byte[] toByteArray()
	{
		byte[] buffer = new byte[16];
		for (int i = 0; i < 8; i++)
		{
			buffer[i] = (byte)(mMostSigBits >>> (8 * (7 - i)));
		}
		for (int i = 8; i < 16; i++)
		{
			buffer[i] = (byte)(mLeastSigBits >>> (8 * (15 - i)));
		}
		return buffer;
	}


	@Override
	public String toString()
	{
		String s = String.format("%016x%016x", mMostSigBits, mLeastSigBits);
		return String.format("%s-%s-%s-%s-%s", s.substring(0, 6), s.substring(6, 10), s.substring(10, 14), s.substring(14, 18), s.substring(18));
	}


	@Override
	public int hashCode()
	{
		long hilo = mMostSigBits ^ mLeastSigBits;
		return ((int)(hilo >> 32)) ^ (int)hilo;
	}


	@Override
	public boolean equals(Object aOther)
	{
		if ((null == aOther) || (aOther.getClass() != ObjectId.class))
		{
			return false;
		}
		ObjectId id = (ObjectId)aOther;
		return (mMostSigBits == id.mMostSigBits && mLeastSigBits == id.mLeastSigBits);
	}


	@Override
	public int compareTo(ObjectId aOther)
	{
		return (mMostSigBits < aOther.mMostSigBits ? -1 : (mMostSigBits > aOther.mMostSigBits ? 1 : (mLeastSigBits < aOther.mLeastSigBits ? -1 : (mLeastSigBits > aOther.mLeastSigBits ? 1 : 0))));
	}


//	public static void main(String... args)
//	{
//		try
//		{
//			long t = System.currentTimeMillis();
//			for (int i = 0; i < 100; i++)
//			{
//				ObjectId objectId = ObjectId.randomId();
//
//				System.out.printf("%s %4d %19d %s %d %d%n", objectId, objectId.sequence(), objectId.random(), new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(objectId.timestamp()), objectId.version(), objectId.variant());
//			}
//			System.out.println(System.currentTimeMillis() - t);
//
////			ObjectId objectId = new ObjectId(0x0fffffffffff7000L, 0xA000000000000000L);
////			ObjectId objectId = new ObjectId();
////
////			System.out.println(objectId);
////			System.out.println(objectId.sequence());
////			System.out.println(objectId.random());
////			System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(objectId.timestamp()));
////			System.out.println(objectId.version());
////			System.out.println(objectId.variant());
////			System.out.println();
//		}
//		catch (Throwable e)
//		{
//			e.printStackTrace(System.out);
//		}
//	}
}
