package org.terifan.raccoon.hashtable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;
import org.terifan.raccoon.io.IManagedBlockDevice;
import org.terifan.raccoon.DatabaseException;
import org.terifan.raccoon.Node;
import org.terifan.raccoon.Stats;
import org.terifan.raccoon.security.ISAAC;
import org.terifan.raccoon.security.MurmurHash3;
import org.terifan.raccoon.util.Log;


class BlockAccessor
{
	private final static int CHECKSUM_HASH_SEED = 0x26e54d19;
	private final static int[] DEFLATER_LEVELS = {0,1,5,9};

	private IManagedBlockDevice mBlockDevice;
	private int mPageSize;
	private int mCompression;
	private int mNodeSize;
	private int mLeafSize;


	public BlockAccessor(IManagedBlockDevice aBlockDevice, int aNodeSize, int aLeafSize) throws IOException
	{
		mBlockDevice = aBlockDevice;
		mPageSize = mBlockDevice.getBlockSize();

		mCompression = 0;
		this.mNodeSize = aNodeSize;
		this.mLeafSize = aLeafSize;
	}


	public IManagedBlockDevice getBlockDevice()
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
				Log.v("free block ", aBlockPointer);

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
			Log.v("read block ", aBlockPointer);

			byte[] buffer = new byte[mPageSize * aBlockPointer.getPageCount()];

			mBlockDevice.readBlock(aBlockPointer.getPageIndex(), buffer, 0, buffer.length, ((0xffffffffL & aBlockPointer.getBlockKey()) << 32) | (0xffffffffL & aBlockPointer.getChecksum()));
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
						buffer = new byte[mLeafSize];
					}
					else
					{
						buffer = new byte[mNodeSize];
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


	public BlockPointer writeBlock(Node aNode, int aRange, long aTransactionId)
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
					try (DeflaterOutputStream dis = new DeflaterOutputStream(baos, new Deflater(DEFLATER_LEVELS[mCompression])))
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
				int blockKey = ISAAC.PRNG.nextInt();

				mBlockDevice.writeBlock(pageIndex, buffer, 0, buffer.length, ((0xffffffffL & blockKey) << 32) | (0xffffffffL & checksum));
				Stats.blockWrite++;

				BlockPointer blockPointer = new BlockPointer();
				blockPointer.setType(aNode.getType());
				blockPointer.setCompression(mCompression);
				blockPointer.setChecksum(checksum);
				blockPointer.setBlockKey(blockKey);
				blockPointer.setPageIndex(pageIndex);
				blockPointer.setPageCount(pageCount);
				blockPointer.setRange(aRange);
				blockPointer.setTransactionId(aTransactionId);

				Log.v("write block ", blockPointer);

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
