package org.terifan.raccoon;

import java.io.Serializable;
import java.security.SecureRandom;
import java.util.concurrent.atomic.AtomicInteger;
import static org.terifan.raccoon.util.ByteArrayBuffer.readInt32;
import static org.terifan.raccoon.util.ByteArrayBuffer.writeInt32;


/*
 *   0                   1                   2                   3
 *   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |                             time                              |
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |                           session                             |
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |                           sequence                            |
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *
 * time      - 32 bits - time in seconds since midnight January 1, 1970 UTC
 * session   - 32 bits - random value used in all intances per JVM instance
 * sequence  - 32 bits - incrementing counter, initialized to a random value
 */
public final class ObjectId implements Serializable, Comparable<ObjectId>
{
	private final static long serialVersionUID = 1;
	public final static int LENGTH = 12;

	private final int mTime;
	private final int mSession;
	private final int mSequence;


	private static class Holder
	{
		final static SecureRandom PRNG = new SecureRandom();
		final static int SESSION = PRNG.nextInt();
		final static AtomicInteger SEQUENCE = new AtomicInteger(PRNG.nextInt());
	}


	private ObjectId(int aTime, int aSession, int aSequence)
	{
		mTime = aTime;
		mSession = aSession;
		mSequence = aSequence;
	}


	public long time()
	{
		return mTime * 1000L;
	}


	public int sequence()
	{
		return mSequence;
	}


	public int session()
	{
		return mSession;
	}


	public static ObjectId randomId()
	{
		return new ObjectId((int)(System.currentTimeMillis() / 1000), Holder.SESSION, Holder.SEQUENCE.getAndIncrement());
	}


	public static ObjectId fromParts(int aTime, int aSession, int aSequence)
	{
		return new ObjectId(aTime, aSession, aSequence);
	}


	public static ObjectId fromBytes(byte[] aBuffer)
	{
		if (aBuffer == null || aBuffer.length != LENGTH)
		{
			throw new IllegalArgumentException("data must be " + LENGTH + " bytes in length");
		}

		return new ObjectId(readInt32(aBuffer, 0), readInt32(aBuffer, 4), readInt32(aBuffer, 8));
	}


	public static ObjectId fromString(String aName)
	{
		return new ObjectId(Integer.parseUnsignedInt(aName.substring(0, 8), 16), Integer.parseUnsignedInt(aName.substring(8, 16), 16), Integer.parseUnsignedInt(aName.substring(16, 24), 16));
	}


	public byte[] toByteArray()
	{
		byte[] buffer = new byte[LENGTH];
		writeInt32(buffer, 0, mTime);
		writeInt32(buffer, 4, mSession);
		writeInt32(buffer, 8, mSequence);
		return buffer;
	}


	@Override
	public String toString()
	{
		return String.format("%08x%08x%08x", mTime, mSession, mSequence);
	}


	@Override
	public int hashCode()
	{
		return mTime ^ mSession ^ mSequence;
	}


	@Override
	public boolean equals(Object aOther)
	{
		if (aOther instanceof ObjectId)
		{
			ObjectId other = (ObjectId)aOther;
			return (mTime == other.mTime && mSession == other.mSession && mSequence == other.mSequence);
		}
		return false;
	}


	@Override
	public int compareTo(ObjectId aOther)
	{
		return
			mTime < aOther.mTime ? -1 :
				mTime > aOther.mTime ? 1 :
					mSession < aOther.mSession ? -1 :
						mSession > aOther.mSession ? 1 :
							mSequence < aOther.mSequence ? -1 :
								mSequence > aOther.mSequence ? 1 : 0;
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
//				System.out.printf("%s %10d %10d %s%n", objectId, 0xffffffffL&objectId.session(), 0xffffffffL&objectId.sequence(), new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(objectId.time()));
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
