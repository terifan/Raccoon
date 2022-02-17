package test;

import java.util.Random;


public class Helper
{
	static String createString(Random rnd)
	{
		return createString(rnd, 5 + rnd.nextInt(40));
	}


	static String createString(Random rnd, int aLength)
	{
		String s = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 ";
		StringBuilder sb = new StringBuilder();
		for (int i = aLength; --i >= 0;)
		{
			sb.append(s.charAt(rnd.nextInt(s.length())));
		}
		return sb.toString();
	}


	static byte[] createBinary(Random rnd)
	{
		byte[] bytes = new byte[5 + rnd.nextInt(40)];
		rnd.nextBytes(bytes);
		return bytes;
	}
}
