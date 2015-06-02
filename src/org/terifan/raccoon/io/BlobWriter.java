package org.terifan.raccoon.io;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import org.terifan.raccoon.security.ISAAC;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Log;


public class BlobWriter
{
	final static int MAX_ADJACENT_BLOCKS = 64;
	final static int POINTER_MAX_LENGTH = 64;

	
	private BlobWriter()
	{
	}
	
	
	public static byte[] transfer(IManagedBlockDevice aBlockDevice, long aTransactionId, InputStream aInputStream) throws IOException
	{
		int blockSize = aBlockDevice.getBlockSize();
		ByteArrayBuffer buffer = new ByteArrayBuffer(MAX_ADJACENT_BLOCKS * blockSize);
		ByteArrayBuffer pointerBuffer = new ByteArrayBuffer(blockSize);
		int totalLength = 0;

		for (int fragment = 0;;)
		{
			int b = aInputStream.read();

			if (b == -1)
			{
				buffer.write(new byte[buffer.capacity() - buffer.position()]);
			}
			else
			{
				totalLength++;
				buffer.write(b);
			}

			if (buffer.position() == buffer.capacity())
			{
				int blockCount = buffer.position() / blockSize;
				long blockIndex = aBlockDevice.allocBlock(blockCount);
				long blockKey = ISAAC.PRNG.nextLong();

				if (blockIndex < 0)
				{
					throw new IOException("Insufficient space in block device.");
				}

				aBlockDevice.writeBlock(blockIndex, buffer.array(), 0, blockCount * blockSize, blockKey);

				pointerBuffer.writeVar64(blockIndex);
				pointerBuffer.writeVar64(blockCount);
				pointerBuffer.writeInt64(blockKey);

				Log.d("Write fragment " + ++fragment + " at " + blockIndex + " +" + blockCount);

				buffer.position(0);
				
				if (b == -1)
				{
					break;
				}
			}
		}
		
		boolean indirectBlock = false;
		int pointerBufferLength = pointerBuffer.position();

		// create indirect block if pointers exceed max length
		if (pointerBufferLength > POINTER_MAX_LENGTH)
		{
			byte[] tmp = BlobWriter.transfer(aBlockDevice, aTransactionId, new ByteArrayInputStream(pointerBuffer.array(), 0, pointerBufferLength));

			indirectBlock = true;
			pointerBufferLength = tmp.length;
			pointerBuffer.array(tmp).position(tmp.length);
		}

		ByteArrayBuffer output = new ByteArrayBuffer(pointerBufferLength);
		output.writeBit(indirectBlock);
		output.writeVar64(totalLength);
		output.writeVar64(pointerBufferLength);
		output.writeVar64(aTransactionId);
		output.write(pointerBuffer.array(), 0, pointerBufferLength);

		Log.out.println(totalLength);
		Log.out.println(pointerBufferLength);

		return output.trim().array();
	}
}