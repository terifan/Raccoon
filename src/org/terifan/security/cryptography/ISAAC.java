package org.terifan.security.cryptography;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;


/**
 * ISAAC is a fast cryptographic random number generator. This implementation is not thread safe.
 *
 * See http://burtleburtle.net/bob/rand/isaacafa.html
 */
public final class ISAAC extends Random
{
	private transient int[] set;
	private transient int[] mem;
	private transient int ma, mb, mc, count;

	private final static AtomicLong seedUniquifier = new AtomicLong(8682522807148012L);


	/**
	 * Constructs a new ISAAC object with a random seed.
	 */
	public ISAAC()
	{
		this(seedUniquifier() ^ System.nanoTime());
	}


	private static long seedUniquifier()
	{
		// L'Ecuyer, "Tables of Linear Congruential Generators of Different Sizes and Good Lattice Structure", 1999
		for (;;)
		{
			long current = seedUniquifier.get();
			long next = current * 1181783497276652981L;
			if (seedUniquifier.compareAndSet(current, next))
			{
				return next;
			}
		}
	}


	/**
	 * Constructs a new ISAAC object with a predefined seed.
	 */
	public ISAAC(long aSeed)
	{
		int[] seed = new int[256];
		long s = aSeed;

		for (int j = 0; j < 32; j++)
		{
			s ^= 5712613008489222801L;

			for (int i = 0; i < 256; i++)
			{
				s = (s * 0x5DEECE66DL + 0xBL) & 281474976710655L;

				seed[i] ^= s >>> j;
			}
		}

		initializeState(seed);
	}


	private void initializeState(int[] aSeed)
	{
		mem = new int[256];
		set = new int[256];
		ma = mb = mc = count = 0;

		int a, b, c, d, e, f, g, h, i;
		a = b = c = d = e = f = g = h = 0x9e3779b9;

		for (i = 0; i < 4; ++i)
		{
			a ^= b << 11;
			d += a;
			b += c;
			b ^= c >>> 2;
			e += b;
			c += d;
			c ^= d << 8;
			f += c;
			d += e;
			d ^= e >>> 16;
			g += d;
			e += f;
			e ^= f << 10;
			h += e;
			f += g;
			f ^= g >>> 4;
			a += f;
			g += h;
			g ^= h << 8;
			b += g;
			h += a;
			h ^= a >>> 9;
			c += h;
			a += b;
		}

		for (i = 0; i < 256; i += 8)
		{
			a += aSeed[i];
			b += aSeed[i + 1];
			c += aSeed[i + 2];
			d += aSeed[i + 3];
			e += aSeed[i + 4];
			f += aSeed[i + 5];
			g += aSeed[i + 6];
			h += aSeed[i + 7];
			a ^= b << 11;
			d += a;
			b += c;
			b ^= c >>> 2;
			e += b;
			c += d;
			c ^= d << 8;
			f += c;
			d += e;
			d ^= e >>> 16;
			g += d;
			e += f;
			e ^= f << 10;
			h += e;
			f += g;
			f ^= g >>> 4;
			a += f;
			g += h;
			g ^= h << 8;
			b += g;
			h += a;
			h ^= a >>> 9;
			c += h;
			a += b;
			mem[i] = a;
			mem[i + 1] = b;
			mem[i + 2] = c;
			mem[i + 3] = d;
			mem[i + 4] = e;
			mem[i + 5] = f;
			mem[i + 6] = g;
			mem[i + 7] = h;
		}

		for (i = 0; i < 256; i += 8)
		{
			a += mem[i];
			b += mem[i + 1];
			c += mem[i + 2];
			d += mem[i + 3];
			e += mem[i + 4];
			f += mem[i + 5];
			g += mem[i + 6];
			h += mem[i + 7];
			a ^= b << 11;
			d += a;
			b += c;
			b ^= c >>> 2;
			e += b;
			c += d;
			c ^= d << 8;
			f += c;
			d += e;
			d ^= e >>> 16;
			g += d;
			e += f;
			e ^= f << 10;
			h += e;
			f += g;
			f ^= g >>> 4;
			a += f;
			g += h;
			g ^= h << 8;
			b += g;
			h += a;
			h ^= a >>> 9;
			c += h;
			a += b;
			mem[i] = a;
			mem[i + 1] = b;
			mem[i + 2] = c;
			mem[i + 3] = d;
			mem[i + 4] = e;
			mem[i + 5] = f;
			mem[i + 6] = g;
			mem[i + 7] = h;
		}
	}


	private void nextSet()
	{
		mb += ++mc;

		for (int i = 0, y; i < 256; ++i)
		{
			int x = mem[i];

			switch (i & 3)
			{
				case 0:
					ma ^= ma << 13;
					break;
				case 1:
					ma ^= ma >>> 6;
					break;
				case 2:
					ma ^= ma << 2;
					break;
				case 3:
					ma ^= ma >>> 16;
					break;
				default:
					throw new IllegalStateException();
			}

			ma = mem[(i + 128) & 255] + ma;
			mem[i] = y = mem[(x >> 2) & 255] + ma + mb;
			set[i] = mb = mem[(y >> 10) & 255] + x;
		}

		count = 256;
	}


	/**
	 * Returns a random integer.
	 */
	@Override
	public int nextInt()
	{
		if (count == 0)
		{
			nextSet();
		}
		return set[--count];
	}


	@Override
	public long nextLong()
	{
		return (((long)nextInt()) << 32) + nextInt();
	}


	/**
	 * Fills the buffer supplied with random bytes.
	 */
	public byte[] nextBytes(byte[] aBuffer, int aOffset, int aLength)
	{
		while (aLength >= 4)
		{
			if (count == 0)
			{
				nextSet();
			}
			int v = set[--count];
			aBuffer[aOffset++] = (byte)(v);
			aBuffer[aOffset++] = (byte)(v >> 8);
			aBuffer[aOffset++] = (byte)(v >> 16);
			aBuffer[aOffset++] = (byte)(v >>> 24);
			aLength -= 4;
		}
		while (--aLength >= 0)
		{
			if (count == 0)
			{
				nextSet();
			}
			aBuffer[aOffset++] = (byte)set[--count];
		}
		return aBuffer;
	}
}
