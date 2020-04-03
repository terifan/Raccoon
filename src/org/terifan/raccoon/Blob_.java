package org.terifan.raccoon;

import java.io.IOException;
import org.terifan.raccoon.storage.BlockAccessor;
import org.terifan.raccoon.storage.BlockPointer;
import org.terifan.raccoon.util.ByteArrayBuffer;


class Blob_
{
	public static void deleteBlob(BlockAccessor aBlockAccessor, byte[] aHeader) throws IOException
	{
		ByteArrayBuffer buffer = new ByteArrayBuffer(aHeader);
		buffer.readVar64(); // skip total length

		freeBlocks(aBlockAccessor, buffer);
	}


	private static void freeBlocks(BlockAccessor aBlockAccessor, ByteArrayBuffer aBuffer)
	{
		while (aBuffer.remaining() > 0)
		{
			BlockPointer bp = new BlockPointer().unmarshal(aBuffer);

			if (bp.getBlockType() == BlockType.BLOB_INDEX)
			{
				freeBlocks(aBlockAccessor, new ByteArrayBuffer(aBlockAccessor.readBlock(bp)).limit(bp.getLogicalSize()));
			}

			aBlockAccessor.freeBlock(bp);
		}
	}
}
