package test;

import java.util.Random;


public class Helper
{
	static String createString(Random rnd)
	{
//		return createString(rnd, 50 + rnd.nextInt(80));
		return createString(rnd, 100);
	}


	static String createString(Random rnd, int aLength)
	{
		String alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < aLength; i++)
		{
			sb.append(alphabet.charAt(rnd.nextInt(alphabet.length())));
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
