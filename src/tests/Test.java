package tests;

import java.util.Random;
import org.terifan.raccoon.security.TranspositionDiffuser;
import org.terifan.raccoon.util.Log;


public class Test
{
	public static void main(String ... args)
	{
		try
		{
			byte[] buf = new byte[16];
			for (int i = 0; i < buf.length; i++)
			{
				buf[i] = (byte)i;
			}

			byte[] tmp;

			TranspositionDiffuser diffuser = new TranspositionDiffuser(new byte[32], 16);

			tmp = buf.clone(); diffuser.encode(tmp, 0, 16, 0); Log.hexDump(tmp);
			tmp = buf.clone(); diffuser.encode(tmp, 0, 16, 1); Log.hexDump(tmp);
			tmp = buf.clone(); diffuser.encode(tmp, 0, 16, 2); Log.hexDump(tmp);
			tmp = buf.clone(); diffuser.encode(tmp, 0, 16, 3); Log.hexDump(tmp);
			tmp = buf.clone(); diffuser.encode(tmp, 0, 16, 4); Log.hexDump(tmp);
			tmp = buf.clone(); diffuser.encode(tmp, 0, 16, 5); Log.hexDump(tmp);
			tmp = buf.clone(); diffuser.encode(tmp, 0, 16, 6); Log.hexDump(tmp);
			tmp = buf.clone(); diffuser.encode(tmp, 0, 16, 7); Log.hexDump(tmp);
			tmp = buf.clone(); diffuser.encode(tmp, 0, 16, 8); Log.hexDump(tmp);
			tmp = buf.clone(); diffuser.encode(tmp, 0, 16, 9); Log.hexDump(tmp);
			tmp = buf.clone(); diffuser.encode(tmp, 0, 16, 10); Log.hexDump(tmp);
			tmp = buf.clone(); diffuser.encode(tmp, 0, 16, 11); Log.hexDump(tmp);
			tmp = buf.clone(); diffuser.encode(tmp, 0, 16, 12); Log.hexDump(tmp);
			tmp = buf.clone(); diffuser.encode(tmp, 0, 16, 13); Log.hexDump(tmp);
			tmp = buf.clone(); diffuser.encode(tmp, 0, 16, 14); Log.hexDump(tmp);
			tmp = buf.clone(); diffuser.encode(tmp, 0, 16, 15); Log.hexDump(tmp);
			tmp = buf.clone(); diffuser.encode(tmp, 0, 16, 16); Log.hexDump(tmp);
			tmp = buf.clone(); diffuser.encode(tmp, 0, 16, 17); Log.hexDump(tmp);
			tmp = buf.clone(); diffuser.encode(tmp, 0, 16, 18); Log.hexDump(tmp);
			tmp = buf.clone(); diffuser.encode(tmp, 0, 16, 19); Log.hexDump(tmp);
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.out);
		}
	}
}
