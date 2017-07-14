package org.terifan.security.cryptography;

import org.terifan.security.messagedigest.HMAC;


// https://github.com/wg/scrypt/tree/master/src/main/java/com/lambdaworks/crypto
public class SCrypt
{
	public static byte[] generate(HMAC aHmac, byte[] aSalt, int aCost, int aRounds, int aParallelization, int aIterationCount, int aOutputLength)
	{
		if (aCost < 2 || (aCost & (aCost - 1)) != 0)
		{
			throw new IllegalArgumentException("aCost must be a power of 2 greater than 1");
		}
		if (aCost > Integer.MAX_VALUE / 128 / aRounds)
		{
			throw new IllegalArgumentException("Parameter aCost is too large");
		}
		if (aRounds > Integer.MAX_VALUE / 128 / aParallelization)
		{
			throw new IllegalArgumentException("Parameter aRounds is too large");
		}
		if (aIterationCount < 1)
		{
			throw new IllegalArgumentException("aIterationCount must be one or more");
		}

		byte[] xy = new byte[256 * aRounds];
		byte[] v = new byte[128 * aRounds * aCost];

//		System.out.println((xy.length + v.length)/1024/1024);

		byte[] buffer = PBKDF2.generateKeyBytes(aHmac, aSalt, aIterationCount, aParallelization * 128 * aRounds);

		for (int i = 0; i < aParallelization; i++)
		{
			smix(buffer, i * 128 * aRounds, aRounds, aCost, v, xy);
		}

		return PBKDF2.generateKeyBytes(aHmac, buffer, aIterationCount, aOutputLength);
	}


	private static void smix(byte[] b, int bi, int r, int n, byte[] v, byte[] xy)
	{
		int Xi = 0;
		int Yi = 128 * r;
		int i;

		System.arraycopy(b, bi, xy, Xi, 128 * r);

		for (i = 0; i < n; i++)
		{
			System.arraycopy(xy, Xi, v, i * (128 * r), 128 * r);
			blockmix_salsa8(xy, Xi, Yi, r);
		}

		for (i = 0; i < n; i++)
		{
			int j = integerify(xy, Xi, r) & (n - 1);
			blockxor(v, j * (128 * r), xy, Xi, 128 * r);
			blockmix_salsa8(xy, Xi, Yi, r);
		}

		System.arraycopy(xy, Xi, b, bi, 128 * r);
	}


	private static void blockmix_salsa8(byte[] by, int bi, int yi, int r)
	{
		byte[] X = new byte[64];
		int i;

		System.arraycopy(by, bi + (2 * r - 1) * 64, X, 0, 64);

		for (i = 0; i < 2 * r; i++)
		{
			blockxor(by, i * 64, X, 0, 64);
			salsa20_8(X);
			System.arraycopy(X, 0, by, yi + (i * 64), 64);
		}

		for (i = 0; i < r; i++)
		{
			System.arraycopy(by, yi + (i * 2) * 64, by, bi + (i * 64), 64);
		}

		for (i = 0; i < r; i++)
		{
			System.arraycopy(by, yi + (i * 2 + 1) * 64, by, bi + (i + r) * 64, 64);
		}
	}


	private static int R(int a, int b)
	{
		return (a << b) | (a >>> (32 - b));
	}


	private static void salsa20_8(byte[] aBuffer)
	{
		int[] B32 = new int[16];
		int[] x = new int[16];
		int i;

		for (i = 0; i < 16; i++)
		{
			B32[i] = (aBuffer[i * 4 + 0] & 0xff) << 0;
			B32[i] |= (aBuffer[i * 4 + 1] & 0xff) << 8;
			B32[i] |= (aBuffer[i * 4 + 2] & 0xff) << 16;
			B32[i] |= (aBuffer[i * 4 + 3] & 0xff) << 24;
		}

		System.arraycopy(B32, 0, x, 0, 16);

		for (i = 8; i > 0; i -= 2)
		{
			x[4] ^= R(x[0] + x[12], 7);
			x[8] ^= R(x[4] + x[0], 9);
			x[12] ^= R(x[8] + x[4], 13);
			x[0] ^= R(x[12] + x[8], 18);
			x[9] ^= R(x[5] + x[1], 7);
			x[13] ^= R(x[9] + x[5], 9);
			x[1] ^= R(x[13] + x[9], 13);
			x[5] ^= R(x[1] + x[13], 18);
			x[14] ^= R(x[10] + x[6], 7);
			x[2] ^= R(x[14] + x[10], 9);
			x[6] ^= R(x[2] + x[14], 13);
			x[10] ^= R(x[6] + x[2], 18);
			x[3] ^= R(x[15] + x[11], 7);
			x[7] ^= R(x[3] + x[15], 9);
			x[11] ^= R(x[7] + x[3], 13);
			x[15] ^= R(x[11] + x[7], 18);
			x[1] ^= R(x[0] + x[3], 7);
			x[2] ^= R(x[1] + x[0], 9);
			x[3] ^= R(x[2] + x[1], 13);
			x[0] ^= R(x[3] + x[2], 18);
			x[6] ^= R(x[5] + x[4], 7);
			x[7] ^= R(x[6] + x[5], 9);
			x[4] ^= R(x[7] + x[6], 13);
			x[5] ^= R(x[4] + x[7], 18);
			x[11] ^= R(x[10] + x[9], 7);
			x[8] ^= R(x[11] + x[10], 9);
			x[9] ^= R(x[8] + x[11], 13);
			x[10] ^= R(x[9] + x[8], 18);
			x[12] ^= R(x[15] + x[14], 7);
			x[13] ^= R(x[12] + x[15], 9);
			x[14] ^= R(x[13] + x[12], 13);
			x[15] ^= R(x[14] + x[13], 18);
		}

		for (i = 0; i < 16; ++i)
		{
			B32[i] = x[i] + B32[i];
		}

		for (i = 0; i < 16; i++)
		{
			aBuffer[i * 4 + 0] = (byte)(B32[i] >> 0 & 0xff);
			aBuffer[i * 4 + 1] = (byte)(B32[i] >> 8 & 0xff);
			aBuffer[i * 4 + 2] = (byte)(B32[i] >> 16 & 0xff);
			aBuffer[i * 4 + 3] = (byte)(B32[i] >> 24 & 0xff);
		}
	}


	private static void blockxor(byte[] s, int si, byte[] d, int di, int len)
	{
		for (int i = 0; i < len; i++)
		{
			d[di + i] ^= s[si + i];
		}
	}


	private static int integerify(byte[] aBuffer, int bi, int r)
	{
		int n;

		bi += (2 * r - 1) * 64;

		n = (aBuffer[bi + 0] & 0xff) << 0;
		n |= (aBuffer[bi + 1] & 0xff) << 8;
		n |= (aBuffer[bi + 2] & 0xff) << 16;
		n |= (aBuffer[bi + 3] & 0xff) << 24;

		return n;
	}
}
