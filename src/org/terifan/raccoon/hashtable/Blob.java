package org.terifan.raccoon.hashtable;

import org.terifan.raccoon.io.BlobOutputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import org.terifan.raccoon.DatabaseException;
import org.terifan.raccoon.io.IManagedBlockDevice;
import org.terifan.raccoon.io.Streams;
import org.terifan.raccoon.util.ByteArray;


class Blob
{
	final static int MAX_ADJACENT_BLOCKS = 64;
	final static int POINTER_MAX_LENGTH = 64;
	final static int HEADER_SIZE = 4+4+4+4;
	final static int HEADER_FIELD_COUNT = 0;
	final static int HEADER_FIELD_LENGTH = 4;
	final static int HEADER_FIELD_TRANSACTION = 4+4;
	final static int HEADER_FIELD_BLOCK_KEY = 4+4+4;
	final static int HEADER_INDIRECT_LENGTH = 0;
	final static int HEADER_INDIRECT_ESCAPE = -1;


	private Blob()
	{
	}


	static byte[] writeBlob(IManagedBlockDevice aBlockDevice, InputStream aInputStream, long aTransactionId)
	{
		try
		{
			BlobOutputStream bos = new BlobOutputStream(aBlockDevice, aTransactionId);
			bos.write(Streams.fetch(aInputStream));
			bos.close();

			return bos.finish();
		}
		catch (IOException e)
		{
			throw new DatabaseException(e);
		}
	}


	static void removeBlob(IManagedBlockDevice aBlockDevice, byte[] aBuffer)
	{
		try
		{
			int fragmentCount = ByteArray.getInt(aBuffer, HEADER_FIELD_COUNT);
			long transactionId = ByteArray.getUnsignedInt(aBuffer, HEADER_FIELD_TRANSACTION);
			long blockKey = (transactionId << 32) | ByteArray.getUnsignedInt(aBuffer, HEADER_FIELD_BLOCK_KEY);

			DataInputStream fragmentPointers = new DataInputStream(new ByteArrayInputStream(aBuffer));
			fragmentPointers.skip(HEADER_SIZE);

			// read indirect block
			if (fragmentCount == Blob.HEADER_INDIRECT_ESCAPE)
			{
				int blockIndex = (int)ByteArray.readVarLong(fragmentPointers);
				int blockCount = (int)ByteArray.readVarLong(fragmentPointers) + 1;

				byte[] indirectBuffer = new byte[aBlockDevice.getBlockSize() * blockCount];

				aBlockDevice.readBlock(blockIndex, indirectBuffer, 0, indirectBuffer.length, blockKey);

//				Log.d("Free indirect block at " + blockIndex + " +" + blockCount);

				aBlockDevice.freeBlock(blockIndex, blockCount);

				fragmentCount = ByteArray.getInt(indirectBuffer, HEADER_FIELD_COUNT);

				fragmentPointers = new DataInputStream(new ByteArrayInputStream(indirectBuffer));
				fragmentPointers.skip(HEADER_SIZE);
			}

			for (int i = 0; i < fragmentCount; i++)
			{
				int blockIndex = (int)ByteArray.readVarLong(fragmentPointers);
				int blockCount = (int)ByteArray.readVarLong(fragmentPointers) + 1;

//				Log.d("Free fragment " + (1+i) + " at " + blockIndex + " +" + blockCount);

				aBlockDevice.freeBlock(blockIndex, blockCount);
			}
		}
		catch (Exception e)
		{
			throw new DatabaseException(e);
		}
	}
}
