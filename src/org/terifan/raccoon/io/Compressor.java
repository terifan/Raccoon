package org.terifan.raccoon.io;

import java.io.ByteArrayOutputStream;
import java.io.IOException;


public interface Compressor
{
	boolean compress(byte[] aInput, int aInputOffset, int aInputLength, ByteArrayOutputStream aOutputStream);

	void decompress(byte[] aInput, int aInputOffset, int aInputLength, byte[] aOutput, int aOutputOffset, int aOutputLength) throws IOException;
}
