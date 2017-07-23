package org.terifan.raccoon.storage;

import java.io.IOException;
import org.terifan.raccoon.util.ByteBlockOutputStream;


interface Compressor
{
	boolean compress(byte[] aInput, int aInputOffset, int aInputLength, ByteBlockOutputStream aOutputStream);

	void decompress(byte[] aInput, int aInputOffset, int aInputLength, byte[] aOutput, int aOutputOffset, int aOutputLength) throws IOException;
}
