package org.terifan.v1.raccoon.security;

import org.terifan.v1.raccoon.util.ByteArray;


/**
 * The MurmurHash3 algorithm was created by Austin Appleby. This java port was authored by Yonik Seeley
 * and is placed into the public domain. The author hereby disclaims copyright to this source code.
 * <p>
 * This produces exactly the same hash values as the final C++ version of MurmurHash3 and is thus suitable
 * for producing the same hash values across platforms.
 * <p>
 * The 32 bit x86 version of this hash should be the fastest variant for relatively short keys like ids.
 * <p>
 * Note - The x86 and x64 versions do _not_ produce the same results, as the algorithms are optimized for
 * their respective platforms.
 * <p>
 * See http://code.google.com/p/smhasher/source/browse/trunk/MurmurHash3.cpp
 * See http://github.com/yonik/java_util
 */
public class MurmurHash3
{
	/**
	 * Compute the hash value of buffer provided.
	 *
	 * @param aData
	 *   buffer to process
	 * @param aSeed
	 *   seed value
	 * @return
	 *   the hash value
	 */
	public static int hash_x86_32(byte[] aData, int aSeed)
	{
		return hash_x86_32(aData, 0, aData.length, aSeed);
	}


	/**
	 * Compute the hash value of buffer provided.
	 *
	 * @param aData
	 *   buffer to process
	 * @param aOffset
	 *   offset in buffer
	 * @param aLength
	 *   number of bytes to process
	 * @param aSeed
	 *   seed value
	 * @return
	 *   the hash value
	 */
	public static int hash_x86_32(byte[] aData, int aOffset, int aLength, int aSeed)
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
			k1 = (k1 << 15) | (k1 >>> 17); // ROTL32(k1,15);
			k1 *= c2;

			h1 ^= k1;
			h1 = (h1 << 13) | (h1 >>> 19); // ROTL32(h1,13);
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
				k1 = (k1 << 15) | (k1 >>> 17); // ROTL32(k1,15);
				k1 *= c2;
				h1 ^= k1;
		}

		// finalization
		h1 ^= aLength;

		// fmix(h1);
		h1 ^= h1 >>> 16;
		h1 *= 0x85ebca6b;
		h1 ^= h1 >>> 13;
		h1 *= 0xc2b2ae35;
		h1 ^= h1 >>> 16;

		return h1;
	}


	/**
	 * Returns the MurmurHash3 hash of the UTF-8 bytes of the String without actually encoding
	 * the string to a temporary buffer. This is more than 2x faster than hashing the result
	 * of String.getBytes().
	 */
	public static int hash_x86_32(CharSequence aData, int aSeed)
	{
		return hash_x86_32(aData, 0, aData.length(), aSeed);
	}


	/**
	 * Returns the MurmurHash3 hash of the UTF-8 bytes of the String without actually encoding
	 * the string to a temporary buffer. This is more than 2x faster than hashing the result
	 * of String.getBytes().
	 */
	public static int hash_x86_32(CharSequence aData, int aOffset, int aLength, int aSeed)
	{
		int h1 = aSeed;
		int c1 = 0xcc9e2d51;
		int c2 = 0x1b873593;

		int pos = aOffset;
		int end = aOffset + aLength;
		int k1 = 0;
		int k2;
		int shift = 0;
		int bits;
		int nBytes = 0; // length in UTF8 bytes

		while (pos < end)
		{
			int code = aData.charAt(pos++);
			if (code < 0x80)
			{
				k2 = code;
				bits = 8;
			}
			else if (code < 0x800)
			{
				k2 = (0xC0 | (code >> 6)) | ((0x80 | (code & 0x3F)) << 8);
				bits = 16;
			}
			else if (code < 0xD800 || code > 0xDFFF || pos >= end)
			{
				// we check for pos>=end to encode an unpaired surrogate as 3 bytes.
				k2 = (0xE0 | (code >> 12)) | ((0x80 | ((code >> 6) & 0x3F)) << 8) | ((0x80 | (code & 0x3F)) << 16);
				bits = 24;
			}
			else
			{
				// surrogate pair
				// int utf32 = pos < end ? (int) data.charAt(pos++) : 0;
				int utf32 = (int)aData.charAt(pos++);
				utf32 = ((code - 0xD7C0) << 10) + (utf32 & 0x3FF);
				k2 = (0xff & (0xF0 | (utf32 >> 18))) | ((0x80 | ((utf32 >> 12) & 0x3F))) << 8 | ((0x80 | ((utf32 >> 6) & 0x3F))) << 16 | (0x80 | (utf32 & 0x3F)) << 24;
				bits = 32;
			}

			k1 |= k2 << shift;

			// int used_bits = 32 - shift; // how many bits of k2 were used in k1.
			// int unused_bits = bits - used_bits; // (bits-(32-shift)) == bits+shift-32 == bits-newshift

			shift += bits;
			if (shift >= 32)
			{
				// mix after we have a complete word

				k1 *= c1;
				k1 = (k1 << 15) | (k1 >>> 17); // ROTL32(k1,15);
				k1 *= c2;

				h1 ^= k1;
				h1 = (h1 << 13) | (h1 >>> 19); // ROTL32(h1,13);
				h1 = h1 * 5 + 0xe6546b64;

				shift -= 32;
				// unfortunately, java won't let you shift 32 bits off, so we need to check for 0
				if (shift != 0)
				{
					k1 = k2 >>> (bits - shift); // bits used == bits - newshift
				}
				else
				{
					k1 = 0;
				}
				nBytes += 4;
			}

		} // inner

		// handle tail
		if (shift > 0)
		{
			nBytes += shift >> 3;
			k1 *= c1;
			k1 = (k1 << 15) | (k1 >>> 17); // ROTL32(k1,15);
			k1 *= c2;
			h1 ^= k1;
		}

		// finalization
		h1 ^= nBytes;

		// fmix(h1);
		h1 ^= h1 >>> 16;
		h1 *= 0x85ebca6b;
		h1 ^= h1 >>> 13;
		h1 *= 0xc2b2ae35;
		h1 ^= h1 >>> 16;

		return h1;
	}


	/**
	 * Compute the hash value of buffer provided.
	 *
	 * @param aData
	 *   buffer to process
	 * @param aSeed
	 *   seed value
	 * @param aOutput
	 *   an array containing at least two longs
	 * @return
	 *   the provided output array
	 */
	public static long[] hash_x64_128(byte[] aData, long aSeed, long[] aOutput)
	{
		return hash_x64_128(aData, 0, aData.length, aSeed, aOutput);
	}


	/**
	 * Compute the hash value of buffer provided.
	 *
	 * @param aData
	 *   buffer to process
	 * @param aOffset
	 *   offset in buffer
	 * @param aLength
	 *   number of bytes to process
	 * @param aSeed
	 *   seed value
	 * @param aOutput
	 *   an array containing at least two longs
	 * @return
	 *   the provided output array
	 */
	public static long[] hash_x64_128(byte[] aData, int aOffset, int aLength, long aSeed, long[] aOutput)
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
			case 15: k2 |= (0xffL & aData[tail + 14]) << 48;
			case 14: k2 |= (0xffL & aData[tail + 13]) << 40;
			case 13: k2 |= (0xffL & aData[tail + 12]) << 32;
			case 12: k2 |= (0xffL & aData[tail + 11]) << 24;
			case 11: k2 |= (0xffL & aData[tail + 10]) << 16;
			case 10: k2 |= (0xffL & aData[tail + 9]) << 8;
			case 9:	 k2 |= (0xffL & aData[tail + 8]);
				k2 *= c2;
				k2 = Long.rotateLeft(k2, 33);
				k2 *= c1;
				h2 ^= k2;
			case 8: k1 |= (0xffL & aData[tail + 7]) << 56;
			case 7: k1 |= (0xffL & aData[tail + 6]) << 48;
			case 6: k1 |= (0xffL & aData[tail + 5]) << 40;
			case 5: k1 |= (0xffL & aData[tail + 4]) << 32;
			case 4: k1 |= (0xffL & aData[tail + 3]) << 24;
			case 3: k1 |= (0xffL & aData[tail + 2]) << 16;
			case 2: k1 |= (0xffL & aData[tail + 1]) << 8;
			case 1: k1 |= (0xffL & aData[tail + 0]);
				k1 *= c1;
				k1 = Long.rotateLeft(k1, 31);
				k1 *= c2;
				h1 ^= k1;
		}

		h1 ^= aLength;
		h2 ^= aLength;

		h1 += h2;
		h2 += h1;

		h1 = fmix64(h1);
		h2 = fmix64(h2);

		h1 += h2;
		h2 += h1;

		aOutput[0] = h1;
		aOutput[1] = h2;

		return aOutput;
	}


	public static long hash_x64_64(byte[] aData, long aSeed)
	{
		return hash_x64_64(aData, 0, aData.length, aSeed);
	}


	public static long hash_x64_64(byte[] aData, int aOffset, int aLength, long aSeed)
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
			case 15: k2 |= (0xffL & aData[tail + 14]) << 48;
			case 14: k2 |= (0xffL & aData[tail + 13]) << 40;
			case 13: k2 |= (0xffL & aData[tail + 12]) << 32;
			case 12: k2 |= (0xffL & aData[tail + 11]) << 24;
			case 11: k2 |= (0xffL & aData[tail + 10]) << 16;
			case 10: k2 |= (0xffL & aData[tail + 9]) << 8;
			case 9:	 k2 |= (0xffL & aData[tail + 8]);
				k2 *= c2;
				k2 = Long.rotateLeft(k2, 33);
				k2 *= c1;
				h2 ^= k2;
			case 8: k1 |= (0xffL & aData[tail + 7]) << 56;
			case 7: k1 |= (0xffL & aData[tail + 6]) << 48;
			case 6: k1 |= (0xffL & aData[tail + 5]) << 40;
			case 5: k1 |= (0xffL & aData[tail + 4]) << 32;
			case 4: k1 |= (0xffL & aData[tail + 3]) << 24;
			case 3: k1 |= (0xffL & aData[tail + 2]) << 16;
			case 2: k1 |= (0xffL & aData[tail + 1]) << 8;
			case 1: k1 |= (0xffL & aData[tail + 0]);
				k1 *= c1;
				k1 = Long.rotateLeft(k1, 31);
				k1 *= c2;
				h1 ^= k1;
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


	private static long getLong(byte [] aBuffer, int aPosition)
	{
		return (((long)(aBuffer[aPosition + 7]      ) << 56)
			  + ((long)(aBuffer[aPosition + 6] & 255) << 48)
			  + ((long)(aBuffer[aPosition + 5] & 255) << 40)
			  + ((long)(aBuffer[aPosition + 4] & 255) << 32)
			  + ((long)(aBuffer[aPosition + 3] & 255) << 24)
			  + ((      aBuffer[aPosition + 2] & 255) << 16)
			  + ((      aBuffer[aPosition + 1] & 255) <<  8)
			  + ((      aBuffer[aPosition    ] & 255       )));
	}
}
