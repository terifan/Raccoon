package org.terifan.raccoon.security;

import java.util.Arrays;
import java.util.concurrent.Callable;
import org.terifan.raccoon.security.HMAC;


/**
 * This class implements the PBKDF2 function (password-based-key-derivation-function-2)
 * from the PKCS#5 v2.0 Password-Based Cryptography Standard.
 */
public final class PBKDF2
{
	private PBKDF2()
	{
	}


	public static SecretKey generateKey(HMAC aPassword, byte [] aSalt, int aIterationCount, int aKeyLengthBytes)
	{
		return new SecretKey(generateKeyBytes(aPassword, aSalt, aIterationCount, aKeyLengthBytes));
	}


//	public static byte [] generateKeyBytes(HMAC aHMAC, byte [] aSalt, int aIterationCount, int aKeyLengthBytes)
//	{
//		return generateKeyBytes(aHMAC, aSalt, aIterationCount, aKeyLengthBytes, ManagementFactory.getOperatingSystemMXBean().getAvailableProcessors());
//	}


//	public static byte [] generateKeyBytes(HMAC aHMAC, byte [] aSalt, int aIterationCount, int aKeyLengthBytes, int aNumberOfThreads)
	public static byte [] generateKeyBytes(HMAC aHMAC, byte [] aSalt, int aIterationCount, int aKeyLengthBytes)
	{
		if (aIterationCount < 1)
		{
			throw new IllegalArgumentException("aIterationCount < 1");
		}
		if (aKeyLengthBytes < 1)
		{
			throw new IllegalArgumentException("aKeyLength < 1");
		}
		if (aSalt == null)
		{
			throw new IllegalArgumentException("aSalt is null");
		}
		if (aHMAC == null)
		{
			throw new IllegalArgumentException("aHMAC is null");
		}

//		try
//		{
			int hashLen = aHMAC.getMessageDigest().getDigestLength();
			int blockCount = (aKeyLengthBytes + hashLen - 1) / hashLen;

			byte [] buffer = new byte[blockCount * hashLen];
			byte [] salt = Arrays.copyOfRange(aSalt, 0, aSalt.length + 4);

//			ExecutorService pool = Executors.newFixedThreadPool(aNumberOfThreads);
//			ArrayList<Callable<Boolean>> tasks = new ArrayList<>();

			for (int blockIndex = 1, offset = 0; blockIndex <= blockCount; blockIndex++, offset += hashLen)
			{
//				tasks.add(new Processor(aHMAC.clone(), salt.clone(), aIterationCount, blockIndex, buffer, offset));
				new Processor(aHMAC, salt.clone(), aIterationCount, blockIndex, buffer, offset).call();
			}

//			pool.invokeAll(tasks);
//			pool.shutdown();

			return Arrays.copyOfRange(buffer, 0, aKeyLengthBytes);
//		}
//		catch(CloneNotSupportedException e)
//		{
//			throw new RuntimeException(e);
//		}
	}


	private static class Processor implements Callable<Boolean>
	{
		private HMAC mHMAC;
		private byte [] mSalt;
		private int mIterationCount;
		private int mBlockIndex;
		private byte [] mBuffer;
		private int mOffset;


		Processor(HMAC aHMAC, byte [] aSalt, int aIterationCount, int aBlockIndex, byte [] aBuffer, int aOffset)
		{
			mHMAC = aHMAC;
			mSalt = aSalt;
			mIterationCount = aIterationCount;
			mBlockIndex = aBlockIndex;
			mBuffer = aBuffer;
			mOffset = aOffset;
		}


		@Override
		public Boolean call()
		{
			mSalt[mSalt.length-4] = (byte)(mBlockIndex >>> 24);
			mSalt[mSalt.length-3] = (byte)(mBlockIndex >>  16);
			mSalt[mSalt.length-2] = (byte)(mBlockIndex >>   8);
			mSalt[mSalt.length-1] = (byte)(mBlockIndex       );

			byte [] u = mHMAC.digest(mSalt);

			System.arraycopy(u, 0, mBuffer, mOffset, u.length);

			for (int j = 1; j < mIterationCount; j++)
			{
				u = mHMAC.digest(u);

				for (int i = 0; i < u.length; i++)
				{
					mBuffer[mOffset+i] ^= u[i];
				}
			}

			return Boolean.TRUE;
		}
	}
}