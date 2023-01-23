package org.terifan.raccoon;

import java.io.Serializable;
import java.security.SecureRandom;
import java.util.concurrent.atomic.AtomicInteger;


/*
 * UUID Version 7
 *
 *   0                   1                   2                   3
 *   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |                           unix_ts_ms                          |
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |          unix_ts_ms           |  ver  |       sequence        |
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |var|                        random                             |
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |                            random                             |
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *
 * time     - 48 bits - time in millis
 * sequence - 12 bits - counter incremented each time an instance is created
 * ver      -  4 bits - constant 7
 * var      -  2 bits - constant 2
 * random   - 62 bits - random
 */
public final class ObjectId implements Serializable, Comparable<ObjectId>
{
	private final static long serialVersionUID = 1;

	private final static AtomicInteger SEQUENCE = new AtomicInteger();
	private final long mMostSigBits;
	private final long mLeastSigBits;


	private static class Holder
	{
		static final SecureRandom numberGenerator = new SecureRandom();
	}


	public ObjectId()
	{
		mMostSigBits = (System.currentTimeMillis() << 16) | 0x7000 | (0xFFF & SEQUENCE.getAndIncrement());
		mLeastSigBits = 0xA000000000000000L | (Holder.numberGenerator.nextLong() & 0x3FFFFFFFFFFFFFFFL);
	}


	public ObjectId(byte[] aData)
	{
		if (aData.length != 16)
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

		if (((int)(msb >> 12) & 0x0F) != 7)
		{
			throw new IllegalArgumentException();
		}
		if (((int)(lsb >>> 62)) != 2)
		{
			throw new IllegalArgumentException();
		}

		mMostSigBits = msb;
		mLeastSigBits = lsb;
	}


	public ObjectId(long aMostSigBits, long aLeastSigBits)
	{
		if (((int)(aMostSigBits >> 12) & 0x0F) != 7)
		{
			throw new IllegalArgumentException();
		}
		if (((int)(aLeastSigBits >>> 62)) != 2)
		{
			throw new IllegalArgumentException();
		}

		mMostSigBits = aMostSigBits;
		mLeastSigBits = aLeastSigBits;
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
//			for (int i = 0; i < 10; i++)
//			{
//				ObjectId objectId = new ObjectId();
//
//				System.out.println(objectId);
//				System.out.println(objectId.sequence());
//				System.out.println(objectId.random());
//				System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(objectId.timestamp()));
//				System.out.println(objectId.version());
//				System.out.println(objectId.variant());
//				System.out.println();
//
//				new ObjectId(objectId.getMostSignificantBits(), objectId.getLeastSignificantBits());
//			}
//		}
//		catch (Throwable e)
//		{
//			e.printStackTrace(System.out);
//		}
//	}
}
