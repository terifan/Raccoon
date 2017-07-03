package org.terifan.raccoon;


public class Constants
{
	final static int getDatabaseVersion()
	{
		return 1;
	}


	final static byte[] getApplicationidentity()
	{
		return new byte[]{(byte)'r',(byte)'a',(byte)'c',(byte)'c',(byte)'o',(byte)'o',(byte)'n',(byte)'-',(byte)'d',(byte)'a',(byte)'t',(byte)'a',(byte)'b',(byte)'a',(byte)'s',(byte)'e'};
	}


	final static int getDefaultBlockSize()
	{
		return 4096;
	}
}
