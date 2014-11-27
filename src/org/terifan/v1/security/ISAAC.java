package org.terifan.v1.security;

import java.util.concurrent.atomic.AtomicLong;


/**
 * ISAAC is a fast cryptographic random number generator. This implementation is not thread safe.
 *
 * See http://burtleburtle.net/bob/rand/isaacafa.html
 */
public final class ISAAC
{
	private final static AtomicLong seedUniquifier = new AtomicLong(8682522807148012L); // constants from java.util.Random

	private transient int[] set;
	private transient int[] mem;
	private transient int ma, mb, mc, count;
    private transient double nextNextGaussian;
    private transient boolean haveNextNextGaussian;


	/**
	 * Constructs a new ISAAC object with a random seed.
	 */
	public ISAAC()
	{
		setSeed(seedUniquifier() ^ System.nanoTime());
	}

	
	/**
	 * Constructs a new ISAAC object with a predefined seed.
	 */
	public ISAAC(long aSeed)
	{
		setSeed(aSeed);
	}

	
	public void setSeed(long aSeed)
	{
		int[] seed = new int[256];

		long s = aSeed ^ 5712613008489222801L; // just a random number

		for (int i = 0; i < 256; i++)
		{
			s = (s * 0x5DEECE66DL + 0xBL) & 281474976710655L; // constants from java.util.Random

			seed[i] = (int)(s >>> 16);
		}

		initializeState(seed);
	}
	

	// code from java.util.Random
    private static long seedUniquifier()
	{
		for (;;)
		{
			long current = seedUniquifier.get();
			long next = current * 181783497276652981L;
			if (seedUniquifier.compareAndSet(current, next))
			{
				return next;
			}
		}
	}
    

	private void initializeState(int[] seed)
	{
		mem = new int[256];
		set = new int[256];
		ma = mb = mc = count = 0;
		nextNextGaussian = 0;
		haveNextNextGaussian = false;
	
		int a, b, c, d, e, f, g, h, i;
		a = b = c = d = e = f = g = h = 0x9e3779b9;

		for (i = 0; i < 4; ++i)
		{
			a ^= b << 11;			d += a;			b += c;
			b ^= c >>> 2;			e += b;			c += d;
			c ^= d << 8;			f += c;			d += e;
			d ^= e >>> 16;			g += d;			e += f;
			e ^= f << 10;			h += e;			f += g;
			f ^= g >>> 4;			a += f;			g += h;
			g ^= h << 8;			b += g;			h += a;
			h ^= a >>> 9;			c += h;			a += b;
		}

		for (i = 0; i < 256; i += 8)
		{
			a += seed[i];
			b += seed[i + 1];
			c += seed[i + 2];
			d += seed[i + 3];
			e += seed[i + 4];
			f += seed[i + 5];
			g += seed[i + 6];
			h += seed[i + 7];
			a ^= b << 11;			d += a;			b += c;
			b ^= c >>> 2;			e += b;			c += d;
			c ^= d << 8;			f += c;			d += e;
			d ^= e >>> 16;			g += d;			e += f;
			e ^= f << 10;			h += e;			f += g;
			f ^= g >>> 4;			a += f;			g += h;
			g ^= h << 8;			b += g;			h += a;
			h ^= a >>> 9;			c += h;			a += b;
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
			a ^= b << 11;			d += a;			b += c;
			b ^= c >>> 2;			e += b;			c += d;
			c ^= d << 8;			f += c;			d += e;
			d ^= e >>> 16;			g += d;			e += f;
			e ^= f << 10;			h += e;			f += g;
			f ^= g >>> 4;			a += f;			g += h;
			g ^= h << 8;			b += g;			h += a;
			h ^= a >>> 9;			c += h;			a += b;
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
			}

			ma          = mem[(i + 128) & 255] + ma;
			mem[i] =  y = mem[(x >> 2) & 255] + ma + mb;
			set[i] = mb = mem[(y >> 10) & 255] + x;
		}

		count = 256;
	}


	/**
	 * Returns a random boolean.
	 */
	public boolean nextBoolean()
	{
		return nextInt() < 0;
	}


	/**
	 * Returns a random byte.
	 */
	public byte nextByte()
	{
		return (byte)nextInt();
	}


	/**
	 * Returns a random integer.
	 */
	public int nextInt()
	{
		if (count == 0)
		{
			nextSet();
		}
		return set[--count];
	}


	/**
	 * Returns a random integer.
	 *
	 * @param aMaxValue 
	 *   the bound on the random number to be returned. Must be positive.
	 */
	public int nextInt(int aMaxValue)
	{
		return Math.abs(nextInt()) % aMaxValue;
	}


	private int[] nextInts(int[] aBuffer)
	{
		return nextInts(aBuffer, 0, aBuffer.length);
	}


	private int[] nextInts(int[] aBuffer, int aOffset, int aLength)
	{
		for (int i = 0; i < aLength; i++)
		{
			aBuffer[aOffset++] = nextInt();
		}
		return aBuffer;
	}


	/**
	 * Returns a random long.
	 */
	public long nextLong()
	{
		return (((long)nextInt()) << 32) + nextInt();
	}

	
	// copy from java.util.Random
	public double nextDouble() 
	{
		return (((long)nextInt(1 << 26) << 27) + nextInt(1 << 27)) / (double)(1L << 53);
    }


	// copy from java.util.Random
	public float nextFloat() 
	{
        return nextInt(1 << 24) / ((float)(1 << 24));
    }


	// copy from java.util.Random
    public double nextGaussian() 
	{
		if (haveNextNextGaussian)
		{
			haveNextNextGaussian = false;
			return nextNextGaussian;
		}
		else
		{
			double v1, v2, s;
			do
			{
				v1 = 2 * nextDouble() - 1; // between -1 and 1
				v2 = 2 * nextDouble() - 1; // between -1 and 1
				s = v1 * v1 + v2 * v2;
			}
			while (s >= 1 || s == 0);
			double multiplier = StrictMath.sqrt(-2 * StrictMath.log(s) / s);
			nextNextGaussian = v2 * multiplier;
			haveNextNextGaussian = true;
			return v1 * multiplier;
		}
    }
	
	
	/**
	 * Fills the buffer supplied with random bytes.
	 */
	public byte[] nextBytes(byte[] aBuffer)
	{
		nextBytes(aBuffer, 0, aBuffer.length);
		return aBuffer;
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
			aBuffer[aOffset++] = (byte)(v>>8);
			aBuffer[aOffset++] = (byte)(v>>16);
			aBuffer[aOffset++] = (byte)(v>>>24);
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

	
	/**
	 * Get an integer value based on the probability of it.<p>
	 * 
	 * E.g. if the probabilities {25,25,50} are provided the integer value
	 *      0 and 1 will be returned 25% of the time each and value 2 returned  
	 *      50% of the time.
	 * 
	 * @param aProbabilties
	 *    an array of probabilities, any positive values can be provided as 
	 *    these are normalized by the implementation.
	 * @return 
	 *    an integer value ranging from 0 to the length of the provided probabilities array.
	 */
	public int nextProb(double ... aProbabilties)
	{
		double v = nextInt(Integer.MAX_VALUE) / (double)Integer.MAX_VALUE;

		double range = 0;
		for (double p : aProbabilties)
		{
			range += p;
		}

		for (int i = 0; i < aProbabilties.length; i++)
		{
			double p = aProbabilties[i] / range;
			if (v < p)
			{
				return i;
			}
			v -= p;
		}

		return aProbabilties.length - 1;
	}


	/**
	 * General purpose static instance of ISAAC. This implementation is not thread safe.
	 */
	public static class PRNG
	{
		private final static ISAAC instance = new ISAAC();


		/**
		 * @see org.terifan.v1.security.ISAAC#nextBoolean
		 */
		public static boolean nextBoolean()
		{
			return instance.nextBoolean();
		}


		/**
		 * @see org.terifan.v1.security.ISAAC#nextByte
		 */
		public static byte nextByte()
		{
			return instance.nextByte();
		}


		/**
		 * @see org.terifan.v1.security.ISAAC#nextInt
		 */
		public static int nextInt()
		{
			return instance.nextInt();
		}


		/**
		 * @param aMaxValue the bound on the random number to be returned. Must be positive.
		 * @see org.terifan.v1.security.ISAAC#nextInt
		 */
		public static int nextInt(int aMaxValue)
		{
			return instance.nextInt(aMaxValue);
		}


		/**
		 * @see org.terifan.v1.security.ISAAC#nextLong
		 */
		public static long nextLong()
		{
			return instance.nextLong();
		}


		/**
		 * @see org.terifan.v1.security.ISAAC#nextFloat
		 */
		public static float nextFloat()
		{
			return instance.nextFloat();
		}


		/**
		 * @see org.terifan.v1.security.ISAAC#nextDouble
		 */
		public static double nextDouble()
		{
			return instance.nextDouble();
		}


		/**
		 * @see org.terifan.v1.security.ISAAC#nextBytes
		 */
		public static byte[] nextBytes(byte[] aBuffer)
		{
			instance.nextBytes(aBuffer);
			return aBuffer;
		}


		/**
		 * @see org.terifan.v1.security.ISAAC#nextBytes
		 */
		public static byte[] nextBytes(byte[] aBuffer, int aOffset, int aLength)
		{
			instance.nextBytes(aBuffer, aOffset, aLength);
			return aBuffer;
		}


		/**
		 * @see org.terifan.v1.security.ISAAC#nextProb
		 */
		public static int nextProb(double ... aProbabilities)
		{
			return instance.nextProb(aProbabilities);
		}


		public static int [] nextInts(int[] aBuffer)
		{
			instance.nextInts(aBuffer);
			return aBuffer;
		}


		public static int [] nextInts(int[] aBuffer, int aOffset, int aLength)
		{
			instance.nextInts(aBuffer, aOffset, aLength);
			return aBuffer;
		}
	}
}
