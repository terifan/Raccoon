package tests;

import org.terifan.raccoon.util.Log;


public class Diffuser
{
	public static void main(String ... args)
	{
		try
		{
			byte[] in = new byte[1024];
			for (int i = 0; i < 1024; i++) in[i] = (byte)i;

			encode(in, 5, 0, 0);
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.out);
		}
	}


	private static byte[] encode(final byte[] in, final int add, final int trans, final int mul)
	{
		final int S = in.length;
		byte[] out = new byte[S];

		for (int i = 0; i < S; i++)
		{
			int k = i + add;
			int j = (k & 255) * 16 + ((k & 255) / 16) + (k / 256) * 256;
			out[k] = in[((j + 0*add) & (S - 1)) ^ trans];
		}

		Log.hexDump(out);

		return out;
	}
}
