package org.terifan.raccoon.io;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;
import org.terifan.raccoon.DatabaseException;
import org.terifan.raccoon.Stats;
import org.terifan.raccoon.security.ISAAC;
import org.terifan.raccoon.security.MurmurHash3;
import org.terifan.raccoon.util.Log;


public class BlockAccessor
{
	private final static int CHECKSUM_HASH_SEED = 0x26e54d19;
	private final static int[] DEFLATER_LEVELS = {0,1,5,9};

	private IManagedBlockDevice mBlockDevice;
	private int mPageSize;
	private int mCompression;


	public BlockAccessor(IManagedBlockDevice aBlockDevice) throws IOException
	{
		mBlockDevice = aBlockDevice;
		mPageSize = mBlockDevice.getBlockSize();

		mCompression = 1;
	}


	public IManagedBlockDevice getBlockDevice()
	{
		return mBlockDevice;
	}


	public void close() throws IOException
	{
		mBlockDevice.close();
	}


	public synchronized void freeBlock(BlockPointer aBlockPointer)
	{
		try
		{
			Log.v("free block ", aBlockPointer);

			mBlockDevice.freeBlock(aBlockPointer.getOffset(), roundUp(aBlockPointer.getPhysicalSize()) / mBlockDevice.getBlockSize());
			Stats.blockFree++;
		}
		catch (Exception e)
		{
			throw new DatabaseException(e);
		}
	}


	public synchronized byte[] readBlock(BlockPointer aBlockPointer)
	{
		try
		{
			Log.v("read block %s", aBlockPointer);
			Log.inc();

			byte[] buffer = new byte[roundUp(aBlockPointer.getPhysicalSize())];

			mBlockDevice.readBlock(aBlockPointer.getOffset(), buffer, 0, buffer.length, aBlockPointer.getBlockKey());
			Stats.blockRead++;

			if (MurmurHash3.hash_x86_32(buffer, 0, aBlockPointer.getPhysicalSize(), CHECKSUM_HASH_SEED) != aBlockPointer.getChecksum())
			{
				throw new IOException("Checksum error in block " + aBlockPointer);
			}

			if (aBlockPointer.getCompression() > 0)
			{
				buffer = decompress(aBlockPointer, buffer);
			}
			
			Log.dec();

			return buffer;
		}
		catch (Exception e)
		{
			throw new DatabaseException("Error reading block", e);
		}
	}


	public synchronized BlockPointer writeBlock(byte[] aBuffer, int aLength, int aType, int aRange, long aTransactionId)
	{
		try
		{
			int physicalSize = 0;
			int compression = 0;

			if (mCompression > 0)
			{
				ByteArrayOutputStream baos = new ByteArrayOutputStream(aLength);
				try (DeflaterOutputStream dis = new DeflaterOutputStream(baos, new Deflater(DEFLATER_LEVELS[mCompression])))
				{
					dis.write(aBuffer, 0, aLength);
				}
				physicalSize = baos.size();
				if (roundUp(physicalSize) < roundUp(aLength))
				{
					compression = mCompression;
					baos.write(new byte[roundUp(physicalSize) - physicalSize]);
					aBuffer = baos.toByteArray();
				}
			}

			if (compression == 0)
			{
				physicalSize = aLength;
				aBuffer = Arrays.copyOfRange(aBuffer, 0, roundUp(physicalSize));
			}

			assert aBuffer.length % mPageSize == 0;

			int blockCount = aBuffer.length / mPageSize;
			long blockIndex = mBlockDevice.allocBlock(blockCount);
			Stats.blockAlloc++;

			BlockPointer blockPointer = new BlockPointer();
			blockPointer.setType(aType);
			blockPointer.setCompression(compression);
			blockPointer.setChecksum(MurmurHash3.hash_x86_32(aBuffer, 0, physicalSize, CHECKSUM_HASH_SEED));
			blockPointer.setBlockKey(ISAAC.PRNG.nextLong());
			blockPointer.setOffset(blockIndex);
			blockPointer.setPhysicalSize(physicalSize);
			blockPointer.setLogicalSize(aLength);
			blockPointer.setRange(aRange);
			blockPointer.setTransactionId(aTransactionId);

			Log.v("write block %s", blockPointer);
			Log.inc();

			mBlockDevice.writeBlock(blockIndex, aBuffer, 0, aBuffer.length, blockPointer.getBlockKey());
			Stats.blockWrite++;
			
			Log.dec();

			return blockPointer;
		}
		catch (Exception e)
		{
			throw new DatabaseException("Error writing block", e);
		}
	}


	private byte [] decompress(BlockPointer aBlockPointer, byte[] aInput) throws IOException
	{
		byte[] output = new byte[aBlockPointer.getLogicalSize()];
		
		try (InflaterInputStream iis = new InflaterInputStream(new ByteArrayInputStream(aInput, 0, aBlockPointer.getLogicalSize())))
		{
			for (int position = 0;;)
			{
				int len = iis.read(output, position, output.length - position);

				if (len <= 0)
				{
					break;
				}

				position += len;
			}
		}

		return output;
	}


	private int roundUp(int aSize)
	{
		return aSize + ((mPageSize - (aSize % mPageSize)) % mPageSize);
	}
}
