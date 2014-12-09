package org.terifan.v1.raccoon.hashtable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;
import org.terifan.v1.raccoon.io.IBlockDevice;
import org.terifan.v1.raccoon.DatabaseException;
import org.terifan.v1.raccoon.Node;
import org.terifan.v1.raccoon.Stats;
import org.terifan.v1.security.MurmurHash3;
import org.terifan.v1.util.Log;


class BlockAccessor
{
	private final static int CHECKSUM_HASH_SEED = 0x26e54d19;
	private final static int[] DEFLATER_LEVELS = {1,5,9};

	private IBlockDevice mBlockDevice;
	private HashTable mHashTable;
	private int mPageSize;
	private int mCompression;


	public BlockAccessor(HashTable aHashTable, IBlockDevice aBlockDevice)
	{
		mHashTable = aHashTable;
		mBlockDevice = aBlockDevice;
		mPageSize = mBlockDevice.getBlockSize();

		mCompression = 0;
	}


	public IBlockDevice getBlockDevice()
	{
		return mBlockDevice;
	}


	public void close() throws IOException
	{
		mBlockDevice.close();
	}


	public void freeBlock(BlockPointer aBlockPointer)
	{
		synchronized (BlockAccessor.class)
		{
			try
			{
				mHashTable.L.i("free block ", aBlockPointer);

				mBlockDevice.freeBlock(aBlockPointer.getPageIndex(), aBlockPointer.getPageCount());
				Stats.blockFree++;
			}
			catch (Exception e)
			{
				throw new DatabaseException(e);
			}
		}
	}


	public byte[] readBlock(BlockPointer aBlockPointer)
	{
		synchronized (BlockAccessor.class)
		{
		try
		{
			mHashTable.L.i("read block ", aBlockPointer);

			byte[] buffer = new byte[mPageSize * aBlockPointer.getPageCount()];

			long blockKey = ((long)aBlockPointer.getTransactionId() << 32) | (0xffffffffL & aBlockPointer.getChecksum());

			mBlockDevice.readBlock(aBlockPointer.getPageIndex(), buffer, 0, buffer.length, blockKey);
			Stats.blockRead++;

			if (MurmurHash3.hash_x86_32(buffer, CHECKSUM_HASH_SEED) != aBlockPointer.getChecksum())
			{
				throw new IOException("Checksum error in block " + aBlockPointer);
			}

			if (aBlockPointer.getCompression() > 0)
			{
				try (InflaterInputStream iis = new InflaterInputStream(new ByteArrayInputStream(buffer)))
				{
					if (aBlockPointer.getType() == Node.LEAF)
					{
						buffer = new byte[mHashTable.getLeafSize()];
					}
					else
					{
						buffer = new byte[mHashTable.getNodeSize()];
					}
					readAll(iis, buffer);
				}
			}

			return buffer;
		}
		catch (Exception e)
		{
			throw new DatabaseException("Error reading block", e);
		}
		}
	}


	public BlockPointer writeBlock(Node aNode, int aRange)
	{
		synchronized (BlockAccessor.class)
		{
		try
		{
			byte[] buffer = aNode.array();

			assert (buffer.length % mPageSize) == 0;

			int physicalSize;

			if (mCompression > 0)
			{
				ByteArrayOutputStream baos = new ByteArrayOutputStream(buffer.length);
				try (DeflaterOutputStream dis = new DeflaterOutputStream(baos, new Deflater(DEFLATER_LEVELS[mCompression - 1])))
				{
					dis.write(buffer);
				}
				physicalSize = baos.size();
				baos.write(new byte[padding(physicalSize)]);
				buffer = baos.toByteArray();
			}
			else
			{
				physicalSize = buffer.length;
				buffer = Arrays.copyOfRange(buffer, 0, physicalSize + padding(physicalSize));
			}

			int pageCount = buffer.length / mPageSize;
			long pageIndex = mBlockDevice.allocBlock(pageCount);
			Stats.blockAlloc++;

			int checksum = MurmurHash3.hash_x86_32(buffer, CHECKSUM_HASH_SEED);

			long tx = mHashTable.getTransactionId();
			long blockKey = (tx << 32) | (0xffffffffL & checksum);

			mBlockDevice.writeBlock(pageIndex, buffer, 0, buffer.length, blockKey);
			Stats.blockWrite++;

			BlockPointer blockPointer = new BlockPointer();
			blockPointer.setType(aNode.getType());
			blockPointer.setCompression(mCompression);
			blockPointer.setChecksum(checksum);
			blockPointer.setPageIndex((int)pageIndex);
			blockPointer.setPageCount(pageCount);
			blockPointer.setRange(aRange);
			blockPointer.setTransactionId((int)tx);

			mHashTable.L.i("write block ", blockPointer);

			return blockPointer;
		}
		catch (Exception e)
		{
			throw new DatabaseException("Error writing block", e);
		}
		}
	}


	private int padding(int aSize)
	{
		return (mPageSize - (aSize % mPageSize)) % mPageSize;
	}


	private byte [] readAll(InputStream aInput, byte[] aBuffer) throws IOException
	{
		for (int position = 0;;)
		{
			int len = aInput.read(aBuffer, position, aBuffer.length - position);

			if (len <= 0)
			{
				break;
			}

			position += len;
		}

		return aBuffer;
	}
}
