package org.terifan.security.cryptography;


public class ISAAC
{
	private transient int[] set;
	private transient int[] mem;
	private transient int ma, mb, mc, count;


	public ISAAC(long aSeed)
	{
		int[] seed = new int[256];

		for (int i = 0; i < 256; i++)
		{
			aSeed = (aSeed * 0x5DEECE66DL + 0xBL) & 281474976710655L; // constants from java.util.Random
			seed[i] = (int)(aSeed >>> 16);
		}

		mem = new int[256];
		set = new int[256];
		ma = mb = mc = count = 0;

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
	 * @param aBound
	 *   must be power of 2
	 */
	public int nextInt(int aBound)
	{
		if (count == 0)
		{
			nextSet();
		}
		return set[--count] & (aBound - 1);
	}
}
