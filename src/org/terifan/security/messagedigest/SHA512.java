package org.terifan.security.messagedigest;

import java.nio.ByteBuffer;
import java.security.DigestException;
import java.security.MessageDigest;


/**
 * FIPS-180-2 compliant SHA-512 implementation<p>
 *
 * Implementation from bouncycastle.org
 *
 * Copyright (c) 2000-2006 The Legion Of The Bouncy Castle (http://www.bouncycastle.org)
 */
public final class SHA512 extends MessageDigest implements Cloneable
{
	private final static int DIGEST_LENGTH = 64;

	private byte[] xBuf;
	private int xBufOff;

	private long byteCount1;
	private long byteCount2;

	private long h1, h2, h3, h4, h5, h6, h7, h8;

	private long[] w = new long[80];
	private int wOff;


	public SHA512()
	{
		super("sha-512");

		xBuf = new byte[8];
		xBufOff = 0;

		reset();
	}


	/**
	 * Constructor for cloning
	 */
	public SHA512(SHA512 aBase)
	{
		super("sha-512");

		xBuf = aBase.xBuf.clone();
		xBufOff = aBase.xBufOff;
		byteCount1 = aBase.byteCount1;
		byteCount2 = aBase.byteCount2;
		h1 = aBase.h1;
		h2 = aBase.h2;
		h3 = aBase.h3;
		h4 = aBase.h4;
		h5 = aBase.h5;
		h6 = aBase.h6;
		h7 = aBase.h7;
		h8 = aBase.h8;
		w = aBase.w.clone();
		wOff = aBase.wOff;
	}


	@Override
	protected byte[] engineDigest()
	{
		byte[] buf = new byte[engineGetDigestLength()];

		try
		{
			engineDigest(buf, 0, buf.length);
		}
		catch (DigestException e)
		{
			throw new IllegalStateException(e);
		}

		return buf;
	}


	@Override
	protected void engineUpdate(ByteBuffer input)
	{
		while (input.hasRemaining())
		{
			engineUpdate(input.get());
		}
	}


	@Override
	protected void engineUpdate(byte in)
	{
		xBuf[xBufOff++] = in;

		if (xBufOff == xBuf.length)
		{
			processWord(xBuf, 0);
			xBufOff = 0;
		}

		byteCount1++;
	}


	@Override
	protected void engineUpdate(
		byte[] in,
		int inOff,
		int len)
	{
		//
		// fill the current word
		//
		while ((xBufOff != 0) && (len > 0))
		{
			engineUpdate(in[inOff]);

			inOff++;
			len--;
		}

		//
		// process whole words.
		//
		while (len > xBuf.length)
		{
			processWord(in, inOff);

			inOff += xBuf.length;
			len -= xBuf.length;
			byteCount1 += xBuf.length;
		}

		//
		// load in the remainder.
		//
		while (len > 0)
		{
			engineUpdate(in[inOff]);

			inOff++;
			len--;
		}
	}


	protected void finish()
	{
		adjustByteCounts();

		long lowBitLength = byteCount1 << 3;
		long hiBitLength = byteCount2;

		//
		// add the pad bytes.
		//
		engineUpdate((byte)128);

		while (xBufOff != 0)
		{
			engineUpdate((byte)0);
		}

		processLength(lowBitLength, hiBitLength);

		processBlock();
	}


	protected void baseReset()
	{
		byteCount1 = 0;
		byteCount2 = 0;

		xBufOff = 0;
		for (int i = 0; i < xBuf.length; i++)
		{
			xBuf[i] = 0;
		}

		wOff = 0;
		for (int i = 0; i != w.length; i++)
		{
			w[i] = 0;
		}
	}


	protected void processWord(
		byte[] in,
		int inOff)
	{
		w[wOff++] = ((long)(in[inOff] & 0xff) << 56)
			| ((long)(in[inOff + 1] & 0xff) << 48)
			| ((long)(in[inOff + 2] & 0xff) << 40)
			| ((long)(in[inOff + 3] & 0xff) << 32)
			| ((long)(in[inOff + 4] & 0xff) << 24)
			| ((long)(in[inOff + 5] & 0xff) << 16)
			| ((long)(in[inOff + 6] & 0xff) << 8)
			| ((in[inOff + 7] & 0xff));

		if (wOff == 16)
		{
			processBlock();
		}
	}


	protected void unpackWord(
		long word,
		byte[] out,
		int outOff)
	{
		out[outOff] = (byte)(word >>> 56);
		out[outOff + 1] = (byte)(word >>> 48);
		out[outOff + 2] = (byte)(word >>> 40);
		out[outOff + 3] = (byte)(word >>> 32);
		out[outOff + 4] = (byte)(word >>> 24);
		out[outOff + 5] = (byte)(word >>> 16);
		out[outOff + 6] = (byte)(word >>> 8);
		out[outOff + 7] = (byte)word;
	}


	/**
	 * adjust the byte counts so that byteCount2 represents the upper long (less 3 bits) word of the byte count.
	 */
	private void adjustByteCounts()
	{
		if (byteCount1 > 0x1fffffffffffffffL)
		{
			byteCount2 += (byteCount1 >>> 61);
			byteCount1 &= 0x1fffffffffffffffL;
		}
	}


	protected void processLength(
		long lowW,
		long hiW)
	{
		if (wOff > 14)
		{
			processBlock();
		}

		w[14] = hiW;
		w[15] = lowW;
	}


	protected void processBlock()
	{
		adjustByteCounts();

		//
		// expand 16 word block into 80 word blocks.
		//
		for (int t = 16; t <= 79; t++)
		{
			w[t] = sigma1(w[t - 2]) + w[t - 7] + sigma0(w[t - 15]) + w[t - 16];
		}

		//
		// set up working variables.
		//
		long a = h1;
		long b = h2;
		long c = h3;
		long d = h4;
		long e = h5;
		long f = h6;
		long g = h7;
		long h = h8;

		int t = 0;
		for (int i = 0; i < 10; i++)
		{
			// t = 8 * i
			h += sum1(e) + ch(e, f, g) + K[t] + w[t++];
			d += h;
			h += sum0(a) + maj(a, b, c);

			// t = 8 * i + 1
			g += sum1(d) + ch(d, e, f) + K[t] + w[t++];
			c += g;
			g += sum0(h) + maj(h, a, b);

			// t = 8 * i + 2
			f += sum1(c) + ch(c, d, e) + K[t] + w[t++];
			b += f;
			f += sum0(g) + maj(g, h, a);

			// t = 8 * i + 3
			e += sum1(b) + ch(b, c, d) + K[t] + w[t++];
			a += e;
			e += sum0(f) + maj(f, g, h);

			// t = 8 * i + 4
			d += sum1(a) + ch(a, b, c) + K[t] + w[t++];
			h += d;
			d += sum0(e) + maj(e, f, g);

			// t = 8 * i + 5
			c += sum1(h) + ch(h, a, b) + K[t] + w[t++];
			g += c;
			c += sum0(d) + maj(d, e, f);

			// t = 8 * i + 6
			b += sum1(g) + ch(g, h, a) + K[t] + w[t++];
			f += b;
			b += sum0(c) + maj(c, d, e);

			// t = 8 * i + 7
			a += sum1(f) + ch(f, g, h) + K[t] + w[t++];
			e += a;
			a += sum0(b) + maj(b, c, d);
		}

		h1 += a;
		h2 += b;
		h3 += c;
		h4 += d;
		h5 += e;
		h6 += f;
		h7 += g;
		h8 += h;

		//
		// reset the offset and clean out the word buffer.
		//
		wOff = 0;
		for (int i = 0; i < 16; i++)
		{
			w[i] = 0;
		}
	}


	/* SHA-384 and SHA-512 functions (as for SHA-256 but for longs) */
	private long ch(
		long x,
		long y,
		long z)
	{
		return ((x & y) ^ ((~x) & z));
	}


	private long maj(
		long x,
		long y,
		long z)
	{
		return ((x & y) ^ (x & z) ^ (y & z));
	}


	private long sum0(
		long x)
	{
		return ((x << 36) | (x >>> 28)) ^ ((x << 30) | (x >>> 34)) ^ ((x << 25) | (x >>> 39));
	}


	private long sum1(
		long x)
	{
		return ((x << 50) | (x >>> 14)) ^ ((x << 46) | (x >>> 18)) ^ ((x << 23) | (x >>> 41));
	}


	private long sigma0(
		long x)
	{
		return ((x << 63) | (x >>> 1)) ^ ((x << 56) | (x >>> 8)) ^ (x >>> 7);
	}


	private long sigma1(
		long x)
	{
		return ((x << 45) | (x >>> 19)) ^ ((x << 3) | (x >>> 61)) ^ (x >>> 6);
	}

	/* SHA-384 and SHA-512 Constants
     * (represent the first 64 bits of the fractional parts of the
     * cube roots of the first sixty-four prime numbers)
	 */
	static final long K[] =
	{
		0x428a2f98d728ae22L, 0x7137449123ef65cdL, 0xb5c0fbcfec4d3b2fL, 0xe9b5dba58189dbbcL,
		0x3956c25bf348b538L, 0x59f111f1b605d019L, 0x923f82a4af194f9bL, 0xab1c5ed5da6d8118L,
		0xd807aa98a3030242L, 0x12835b0145706fbeL, 0x243185be4ee4b28cL, 0x550c7dc3d5ffb4e2L,
		0x72be5d74f27b896fL, 0x80deb1fe3b1696b1L, 0x9bdc06a725c71235L, 0xc19bf174cf692694L,
		0xe49b69c19ef14ad2L, 0xefbe4786384f25e3L, 0x0fc19dc68b8cd5b5L, 0x240ca1cc77ac9c65L,
		0x2de92c6f592b0275L, 0x4a7484aa6ea6e483L, 0x5cb0a9dcbd41fbd4L, 0x76f988da831153b5L,
		0x983e5152ee66dfabL, 0xa831c66d2db43210L, 0xb00327c898fb213fL, 0xbf597fc7beef0ee4L,
		0xc6e00bf33da88fc2L, 0xd5a79147930aa725L, 0x06ca6351e003826fL, 0x142929670a0e6e70L,
		0x27b70a8546d22ffcL, 0x2e1b21385c26c926L, 0x4d2c6dfc5ac42aedL, 0x53380d139d95b3dfL,
		0x650a73548baf63deL, 0x766a0abb3c77b2a8L, 0x81c2c92e47edaee6L, 0x92722c851482353bL,
		0xa2bfe8a14cf10364L, 0xa81a664bbc423001L, 0xc24b8b70d0f89791L, 0xc76c51a30654be30L,
		0xd192e819d6ef5218L, 0xd69906245565a910L, 0xf40e35855771202aL, 0x106aa07032bbd1b8L,
		0x19a4c116b8d2d0c8L, 0x1e376c085141ab53L, 0x2748774cdf8eeb99L, 0x34b0bcb5e19b48a8L,
		0x391c0cb3c5c95a63L, 0x4ed8aa4ae3418acbL, 0x5b9cca4f7763e373L, 0x682e6ff3d6b2b8a3L,
		0x748f82ee5defb2fcL, 0x78a5636f43172f60L, 0x84c87814a1f0ab72L, 0x8cc702081a6439ecL,
		0x90befffa23631e28L, 0xa4506cebde82bde9L, 0xbef9a3f7b2c67915L, 0xc67178f2e372532bL,
		0xca273eceea26619cL, 0xd186b8c721c0c207L, 0xeada7dd6cde0eb1eL, 0xf57d4f7fee6ed178L,
		0x06f067aa72176fbaL, 0x0a637dc5a2c898a6L, 0x113f9804bef90daeL, 0x1b710b35131c471bL,
		0x28db77f523047d84L, 0x32caab7b40c72493L, 0x3c9ebe0a15c9bebcL, 0x431d67c49c100d4cL,
		0x4cc5d4becb3e42b6L, 0x597f299cfc657e2aL, 0x5fcb6fab3ad6faecL, 0x6c44198c4a475817L
	};


	@Override
	protected int engineDigest(byte[] out, int outOff, int len) throws DigestException
	{
		if (out.length - outOff < DIGEST_LENGTH)
		{
			throw new DigestException("Buffer too short.");
		}

		finish();

		unpackWord(h1, out, outOff);
		unpackWord(h2, out, outOff + 8);
		unpackWord(h3, out, outOff + 16);
		unpackWord(h4, out, outOff + 24);
		unpackWord(h5, out, outOff + 32);
		unpackWord(h6, out, outOff + 40);
		unpackWord(h7, out, outOff + 48);
		unpackWord(h8, out, outOff + 56);

		reset();

		return DIGEST_LENGTH;
	}


	@Override
	protected int engineGetDigestLength()
	{
		return DIGEST_LENGTH;
	}


	@Override
	protected void engineReset()
	{
		baseReset();

		/* SHA-512 initial hash value
         * The first 64 bits of the fractional parts of the square roots
         * of the first eight prime numbers
		 */
		h1 = 0x6a09e667f3bcc908L;
		h2 = 0xbb67ae8584caa73bL;
		h3 = 0x3c6ef372fe94f82bL;
		h4 = 0xa54ff53a5f1d36f1L;
		h5 = 0x510e527fade682d1L;
		h6 = 0x9b05688c2b3e6c1fL;
		h7 = 0x1f83d9abfb41bd6bL;
		h8 = 0x5be0cd19137e2179L;
	}


	@Override
	public String toString()
	{
		return "SHA512";
	}


	@Override
	public SHA512 clone()
	{
		return new SHA512(this);
	}
}
