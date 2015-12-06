package org.terifan.raccoon.io;


public interface Compressor 
{
	int COMPRESSION_FAILED = -1;
	
	int compress(byte[] aInput, int aInputOffset, int aInputLength, byte[] aOutput, int aOutputOffset, int aOutputLimit);
	
	void decompress(byte[] aInput, int aInputOffset, int aInputLength, byte[] aOutput, int aOutputOffset, int aOutputLength);
}
