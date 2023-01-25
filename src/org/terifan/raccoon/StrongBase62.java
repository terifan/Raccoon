package org.terifan.raccoon;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Random;


public class StrongBase62
{
	private final static char[] CHARS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz+/".toCharArray();
	private final static int CHECK_SCALE = 37;
//	private final static BigInteger SHIFT = BigInteger.valueOf(4294967296L);
//	private final static BigInteger SCALE = BigInteger.valueOf(62);
//	private final static BigInteger CHECK_SHIFT = BigInteger.valueOf(CHECK_SCALE);
	private final static HashMap<Character, BigInteger> LOOKUP = new HashMap<>();


	static
	{
		for (char i = '0'; i <= '9'; i++)
		{
			LOOKUP.put(i, BigInteger.valueOf(i - '0'));
		}
		for (char i = 'A'; i <= 'Z'; i++)
		{
			LOOKUP.put(i, BigInteger.valueOf(10 + i - 'A'));
		}
		for (char i = 'a'; i <= 'z'; i++)
		{
			LOOKUP.put(i, BigInteger.valueOf(10 + 26 + i - 'a'));
		}
		LOOKUP.put('+', BigInteger.valueOf(62));
		LOOKUP.put('/', BigInteger.valueOf(63));
	}


	public static String encode(int aA, int aB, int aC)
	{
		StringBuilder sb = new StringBuilder();

		int[] val = {
			0xff&(aA>>>24),
			0xff&(aA>>>16),
			0xff&(aA>>>8),
			0xff&(aA>>>0),
			0xff&(aB>>>24),
			0xff&(aB>>>16),
			0xff&(aB>>>8),
			0xff&(aB>>>0),
			0xff&(aC>>>24),
			0xff&(aC>>>16),
			0xff&(aC>>>8),
			0xff&(aC>>>0)
		};

		long rem = 0;
		long acc = 0;

		for (int j = 0; j < 12; j++)
		{
			acc = val[j] + rem * 256;
			long dig = acc % 62;
			rem = acc / 62;

			sb.append(CHARS[(int)(dig)]);
		}

		return sb.toString();


//		BigInteger bi = BigInteger.valueOf(0xffffffffL & aA)
//			.multiply(SHIFT)
//			.add(BigInteger.valueOf(0xffffffffL & aB))
//			.multiply(SHIFT)
//			.add(BigInteger.valueOf(0xffffffffL & aC));
//
//		bi = bi.multiply(CHECK_SHIFT).add(BigInteger.valueOf(checksum(aA, aB, aC)));

//		char[] out = new char[17];
//		for (int i = 17; --i >= 0; )
//		{
//			out[i] = CHARS[bi.mod(SCALE).intValue()];
//			bi = bi.divide(SCALE);
//		}
//
//		return new String(out);
	}


	public static int[] decode(String aCode)
	{
		long v = 0;
		long S = 1;
		int[] out = new int[3];

		for (int i = 0, j = 0; j < 3; i++)
		{
			v += LOOKUP.get(aCode.charAt(i)).intValue() * S;
			S *= 256;

			if (S >= 0xffffffffL)
			{
				System.out.println((int)v);

				out[j++] = (int)v;
				v /= 0xffffffffL;
				S /= 0xffffffffL;
			}
		}

		return out;

//		BigInteger bi = BigInteger.ZERO;
//		for (int i = 0; i < 17; i++)
//		{
//			bi = bi.multiply(SCALE).add(LOOKUP.get(aCode.charAt(i)));
//		}
//
//		int expectedChecksum = bi.mod(CHECK_SHIFT).intValue();
//		bi = bi.divide(CHECK_SHIFT);
//
//		BigInteger tmp = bi.divide(SHIFT);
//		int c = bi.mod(SHIFT).intValue();
//		int b = tmp.mod(SHIFT).intValue();
//		int a = tmp.divide(SHIFT).mod(SHIFT).intValue();
//
//		if (checksum(a, b, c) != expectedChecksum)
//		{
//			throw new IllegalArgumentException();
//		}

//		return new int[]
//		{
//			(int)a, (int)b, (int)c
//		};
	}


	private static int checksum(int aA, int aB, int aC)
	{
		return Math.abs(aA ^ aB ^ aC + aA + aB + aC) % CHECK_SCALE;
	}


	public static void xmain(String ... args)
	{
		try
		{
			for (int j = 0; j < 4; j++)
			{
				int a = -1;
				int b = -1;
				int c = -1;

				String s = "";

				long t = System.currentTimeMillis();
				for (int i = 0; i < 1000000; i++)
				{
					s = encode(a, b, c);
				}
				System.out.print(System.currentTimeMillis() - t);

				t = System.currentTimeMillis();
				for (int i = 0; i < 1000000; i++)
				{
					int[] decoded = decode(s);
				}
				System.out.println("\t" + (System.currentTimeMillis() - t));
			}
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.out);
		}
	}


	public static void main(String ... args)
	{
		try
		{
			Random rnd = new Random();
			int a = -1;
			int b = 0;
			int c = 0;

//			a = rnd.nextInt();
//			b = rnd.nextInt();
//			c = rnd.nextInt();

			String s = encode(a, b, c);

			System.out.println(s);

			int[] decoded = decode(s);

			System.out.println((decoded[0] == a) + " " + a + " " + decoded[0]);
			System.out.println((decoded[1] == b) + " " + b + " " + decoded[1]);
			System.out.println((decoded[2] == c) + " " + c + " " + decoded[2]);
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.out);
		}
	}
}
