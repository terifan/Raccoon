package org.terifan.security.messagedigest;


/**
 * The MurmurHash3 algorithm was created by Austin Appleby. This java port was authored by Yonik Seeley and is placed into the public
 * domain. The author hereby disclaims copyright to this source code.
 * <p>
 * This produces exactly the same hash values as the final C++ version of MurmurHash3 and is thus suitable for producing the same hash
 * values across platforms.
 * <p>
 * The 32 bit x86 version of this hash should be the fastest variant for relatively short keys like ids.
 * <p>
 * Note - The x86 and x64 versions do _not_ produce the same results, as the algorithms are optimized for their respective platforms.
 * <p>
 * See http://code.google.com/p/smhasher/source/browse/trunk/MurmurHash3.cpp See http://github.com/yonik/java_util
 */
public class MurmurHash3
{
	/**
	 * Compute the hash value of buffer provided.
	 *
	 * @param aData buffer to process
	 * @param aSeed seed value
	 * @return the hash value
	 */
	public static int hash32(byte[] aData, int aSeed)
	{
		return hash32(aData, 0, aData.length, aSeed);
	}


	/**
	 * Compute the hash value of buffer provided.
	 *
	 * @param aData buffer to process
	 * @param aOffset offset in buffer
	 * @param aLength number of bytes to process
	 * @param aSeed seed value
	 * @return the hash value
	 */
	public static int hash32(byte[] aData, int aOffset, int aLength, int aSeed)
	{
		int h1 = aSeed;
		int c1 = 0xcc9e2d51;
		int c2 = 0x1b873593;
		int roundedEnd = aOffset + (aLength & 0xfffffffc); // round down to 4 byte block

		for (int i = aOffset; i < roundedEnd; i += 4)
		{
			// little endian load order
			int k1 = (aData[i] & 0xff) | ((aData[i + 1] & 0xff) << 8) | ((aData[i + 2] & 0xff) << 16) | (aData[i + 3] << 24);
			k1 *= c1;
			k1 = (k1 << 15) | (k1 >>> 17);
			k1 *= c2;

			h1 ^= k1;
			h1 = (h1 << 13) | (h1 >>> 19);
			h1 = h1 * 5 + 0xe6546b64;
		}

		// tail
		int k1 = 0;

		switch (aLength & 0x03)
		{
			case 3:
				k1 = (aData[roundedEnd + 2] & 0xff) << 16;
			// fallthrough
			case 2:
				k1 |= (aData[roundedEnd + 1] & 0xff) << 8;
			// fallthrough
			case 1:
				k1 |= (aData[roundedEnd] & 0xff);
				k1 *= c1;
				k1 = (k1 << 15) | (k1 >>> 17);
				k1 *= c2;
				h1 ^= k1;
				break;
			case 0:
				break;
			default:
				throw new IllegalStateException();
		}

		// finalization
		h1 ^= aLength;

		h1 ^= h1 >>> 16;
		h1 *= 0x85ebca6b;
		h1 ^= h1 >>> 13;
		h1 *= 0xc2b2ae35;
		h1 ^= h1 >>> 16;

		return h1;
	}


	public static long hash64(byte[] aData, long aSeed)
	{
		return hash64(aData, 0, aData.length, aSeed);
	}


	public static long hash64(byte[] aData, int aOffset, int aLength, long aSeed)
	{
		int nblocks = aLength / 16;

		long h1 = aSeed;
		long h2 = aSeed;
		long c1 = 0x87c37b91114253d5L;
		long c2 = 0x4cf5ad432745937fL;

		for (int i = 0; i < nblocks; i++)
		{
			long k1 = getLong(aData, aOffset + 16 * i);
			long k2 = getLong(aData, aOffset + 16 * i + 8);

			k1 *= c1;
			k1 = Long.rotateLeft(k1, 31);
			k1 *= c2;
			h1 ^= k1;

			h1 = Long.rotateLeft(h1, 27);
			h1 += h2;
			h1 = h1 * 5 + 0x52dce729;

			k2 *= c2;
			k2 = Long.rotateLeft(k2, 33);
			k2 *= c1;
			h2 ^= k2;

			h2 = Long.rotateLeft(h2, 31);
			h2 += h1;
			h2 = h2 * 5 + 0x38495ab5;
		}

		long k1 = 0;
		long k2 = 0;
		int tail = aOffset + nblocks * 16;

		switch (aLength & 15)
		{
			case 15:
				k2 |= (0xffL & aData[tail + 14]) << 48;
			case 14:
				k2 |= (0xffL & aData[tail + 13]) << 40;
			case 13:
				k2 |= (0xffL & aData[tail + 12]) << 32;
			case 12:
				k2 |= (0xffL & aData[tail + 11]) << 24;
			case 11:
				k2 |= (0xffL & aData[tail + 10]) << 16;
			case 10:
				k2 |= (0xffL & aData[tail + 9]) << 8;
			case 9:
				k2 |= (0xffL & aData[tail + 8]);
				k2 *= c2;
				k2 = Long.rotateLeft(k2, 33);
				k2 *= c1;
				h2 ^= k2;
			case 8:
				k1 |= (0xffL & aData[tail + 7]) << 56;
			case 7:
				k1 |= (0xffL & aData[tail + 6]) << 48;
			case 6:
				k1 |= (0xffL & aData[tail + 5]) << 40;
			case 5:
				k1 |= (0xffL & aData[tail + 4]) << 32;
			case 4:
				k1 |= (0xffL & aData[tail + 3]) << 24;
			case 3:
				k1 |= (0xffL & aData[tail + 2]) << 16;
			case 2:
				k1 |= (0xffL & aData[tail + 1]) << 8;
			case 1:
				k1 |= (0xffL & aData[tail + 0]);
				k1 *= c1;
				k1 = Long.rotateLeft(k1, 31);
				k1 *= c2;
				h1 ^= k1;
				break;
			case 0:
				break;
			default:
				throw new IllegalStateException();
		}

		h1 ^= aLength;
		h2 ^= aLength;

		h1 += h2;
		h2 += h1;

		h1 = fmix64(h1);
		h2 = fmix64(h2);

		h1 += h2;

		return h1;
	}


	private static long fmix64(long k)
	{
		k ^= k >>> 33;
		k *= 0xff51afd7ed558ccdL;
		k ^= k >>> 33;
		k *= 0xc4ceb9fe1a85ec53L;
		k ^= k >>> 33;

		return k;
	}


	private static long getLong(byte[] aBuffer, int aPosition)
	{
		return (((long)(aBuffer[aPosition + 7]) << 56)
			+ ((long)(aBuffer[aPosition + 6] & 255) << 48)
			+ ((long)(aBuffer[aPosition + 5] & 255) << 40)
			+ ((long)(aBuffer[aPosition + 4] & 255) << 32)
			+ ((long)(aBuffer[aPosition + 3] & 255) << 24)
			+ ((aBuffer[aPosition + 2] & 255) << 16)
			+ ((aBuffer[aPosition + 1] & 255) << 8)
			+ ((aBuffer[aPosition] & 255)));
	}


	/**
	 * Compute the hash value of buffer provided.
	 *
	 * @param aData buffer to process
	 * @param aOffset offset in buffer
	 * @param aLength number of bytes to process
	 * @param aSeed seed value
	 * @return an array with two longs
	 */
	public static long[] hash128(byte[] aData, int aOffset, int aLength, long aSeed)
	{
		int nblocks = aLength / 16;

		long h1 = aSeed;
		long h2 = aSeed;
		long c1 = 0x87c37b91114253d5L;
		long c2 = 0x4cf5ad432745937fL;

		for (int i = 0; i < nblocks; i++)
		{
			long k1 = getLong(aData, aOffset + 16 * i);
			long k2 = getLong(aData, aOffset + 16 * i + 8);

			k1 *= c1;
			k1 = Long.rotateLeft(k1, 31);
			k1 *= c2;
			h1 ^= k1;

			h1 = Long.rotateLeft(h1, 27);
			h1 += h2;
			h1 = h1 * 5 + 0x52dce729;

			k2 *= c2;
			k2 = Long.rotateLeft(k2, 33);
			k2 *= c1;
			h2 ^= k2;

			h2 = Long.rotateLeft(h2, 31);
			h2 += h1;
			h2 = h2 * 5 + 0x38495ab5;
		}

		long k1 = 0;
		long k2 = 0;
		int tail = aOffset + nblocks * 16;

		switch (aLength & 15)
		{
			case 15:
				k2 |= (0xffL & aData[tail + 14]) << 48;
			case 14:
				k2 |= (0xffL & aData[tail + 13]) << 40;
			case 13:
				k2 |= (0xffL & aData[tail + 12]) << 32;
			case 12:
				k2 |= (0xffL & aData[tail + 11]) << 24;
			case 11:
				k2 |= (0xffL & aData[tail + 10]) << 16;
			case 10:
				k2 |= (0xffL & aData[tail + 9]) << 8;
			case 9:
				k2 |= (0xffL & aData[tail + 8]);
				k2 *= c2;
				k2 = Long.rotateLeft(k2, 33);
				k2 *= c1;
				h2 ^= k2;
			case 8:
				k1 |= (0xffL & aData[tail + 7]) << 56;
			case 7:
				k1 |= (0xffL & aData[tail + 6]) << 48;
			case 6:
				k1 |= (0xffL & aData[tail + 5]) << 40;
			case 5:
				k1 |= (0xffL & aData[tail + 4]) << 32;
			case 4:
				k1 |= (0xffL & aData[tail + 3]) << 24;
			case 3:
				k1 |= (0xffL & aData[tail + 2]) << 16;
			case 2:
				k1 |= (0xffL & aData[tail + 1]) << 8;
			case 1:
				k1 |= (0xffL & aData[tail + 0]);
				k1 *= c1;
				k1 = Long.rotateLeft(k1, 31);
				k1 *= c2;
				h1 ^= k1;
				break;
			case 0:
				break;
			default:
				throw new IllegalStateException();
		}

		h1 ^= aLength;
		h2 ^= aLength;

		h1 += h2;
		h2 += h1;

		h1 = fmix64(h1);
		h2 = fmix64(h2);

		h1 += h2;
		h2 += h1;

		long[] output = new long[2];
		output[0] = h1;
		output[1] = h2;

		return output;
	}


	/**
	 * Compute the hash value of buffer provided.
	 *
	 * WARNING: this is an extension of the 128 bit implementation to 256 bits. The extension is not verified to be any good.
	 *
	 * @param aData buffer to process
	 * @param aOffset offset in buffer
	 * @param aLength number of bytes to process
	 * @param aSeed seed value
	 * @return an array with four longs
	 */
	public static long[] hash256(byte[] aData, int aOffset, int aLength, long aSeed)
	{
		int nblocks = aLength / 16;

		long h1 = aSeed;
		long h2 = aSeed;
		long h3 = aSeed; // extension
		long h4 = aSeed; // extension
		long c1 = 0x87c37b91114253d5L;
		long c2 = 0x4cf5ad432745937fL;

		for (int i = 0; i < nblocks; i++)
		{
			long k1 = getLong(aData, aOffset + 16 * i);
			long k2 = getLong(aData, aOffset + 16 * i + 8);

			k1 *= c1;
			k1 = Long.rotateLeft(k1, 31);
			k1 *= c2;
			h1 ^= k1;

			h1 = Long.rotateLeft(h1, 27);
			h1 += h2;
			h1 = h1 * 5 + 0x52dce729;

			k2 *= c2;
			k2 = Long.rotateLeft(k2, 33);
			k2 *= c1;
			h2 ^= k2;

			h2 = Long.rotateLeft(h2, 31);
			h2 += h1;
			h2 = h2 * 5 + 0x38495ab5;

			h3 ^= k1; // extension
			h4 ^= k2; // extension
		}

		long k1 = 0;
		long k2 = 0;
		int tail = aOffset + nblocks * 16;

		switch (aLength & 15)
		{
			case 15:
				k2 |= (0xffL & aData[tail + 14]) << 48;
			case 14:
				k2 |= (0xffL & aData[tail + 13]) << 40;
			case 13:
				k2 |= (0xffL & aData[tail + 12]) << 32;
			case 12:
				k2 |= (0xffL & aData[tail + 11]) << 24;
			case 11:
				k2 |= (0xffL & aData[tail + 10]) << 16;
			case 10:
				k2 |= (0xffL & aData[tail + 9]) << 8;
			case 9:
				k2 |= (0xffL & aData[tail + 8]);
				k2 *= c2;
				k2 = Long.rotateLeft(k2, 33);
				k2 *= c1;
				h2 ^= k2;
			case 8:
				k1 |= (0xffL & aData[tail + 7]) << 56;
			case 7:
				k1 |= (0xffL & aData[tail + 6]) << 48;
			case 6:
				k1 |= (0xffL & aData[tail + 5]) << 40;
			case 5:
				k1 |= (0xffL & aData[tail + 4]) << 32;
			case 4:
				k1 |= (0xffL & aData[tail + 3]) << 24;
			case 3:
				k1 |= (0xffL & aData[tail + 2]) << 16;
			case 2:
				k1 |= (0xffL & aData[tail + 1]) << 8;
			case 1:
				k1 |= (0xffL & aData[tail + 0]);
				k1 *= c1;
				k1 = Long.rotateLeft(k1, 31);
				k1 *= c2;
				h1 ^= k1;
				break;
			case 0:
				break;
			default:
				throw new IllegalStateException();
		}

		h3 ^= k1; // extension
		h4 ^= k2; // extension

		h1 ^= aLength;
		h2 ^= aLength;
		h3 ^= aLength; // extension
		h4 ^= aLength; // extension

		h1 += h2;
		h2 += h1;
		h3 += h4; // extension
		h4 += h3; // extension

		h1 = fmix64(h1);
		h2 = fmix64(h2);
		h3 = fmix64(h3); // extension
		h4 = fmix64(h4); // extension

		h1 += h2;
		h2 += h1;
		h3 += h4; // extension
		h4 += h3; // extension

		long[] output = new long[4];
		output[0] = h1;
		output[1] = h2;
		output[2] = h3; // extension
		output[3] = h4; // extension

		return output;
	}
}
