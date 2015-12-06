package org.terifan.raccoon.io;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;
import org.terifan.raccoon.CompressionParam;
import org.terifan.raccoon.DatabaseException;
import org.terifan.raccoon.Node;
import org.terifan.raccoon.Stats;
import org.terifan.raccoon.security.ISAAC;
import org.terifan.raccoon.security.MurmurHash3;
import org.terifan.raccoon.util.Log;


public class BlockAccessor
{
	private IManagedBlockDevice mBlockDevice;
	private CompressionParam mCompressionParam;
	private int mPageSize;


	public BlockAccessor(IManagedBlockDevice aBlockDevice) throws IOException
	{
		mBlockDevice = aBlockDevice;
		mPageSize = mBlockDevice.getBlockSize();

		mCompressionParam = CompressionParam.NO_COMPRESSION;
	}


	public IManagedBlockDevice getBlockDevice()
	{
		return mBlockDevice;
	}


	public void setCompressionParam(CompressionParam aCompressionParam)
	{
		mCompressionParam = aCompressionParam;
	}


	public CompressionParam getCompressionParam()
	{
		return mCompressionParam;
	}


	public void freeBlock(BlockPointer aBlockPointer)
	{
		try
		{
			Log.v("free block %s", aBlockPointer);
			Log.inc();

			mBlockDevice.freeBlock(aBlockPointer.getOffset(), roundUp(aBlockPointer.getPhysicalSize()) / mBlockDevice.getBlockSize());
			Stats.blockFree++;

			Log.dec();
		}
		catch (Exception e)
		{
			throw new DatabaseException(e);
		}
	}


	public byte[] readBlock(BlockPointer aBlockPointer)
	{
		try
		{
			Log.v("read block %s", aBlockPointer);
			Log.inc();

			byte[] buffer = new byte[roundUp(aBlockPointer.getPhysicalSize())];

			mBlockDevice.readBlock(aBlockPointer.getOffset(), buffer, 0, buffer.length, aBlockPointer.getBlockKey());
			Stats.blockRead++;

			if (MurmurHash3.hash_x86_32(buffer, 0, aBlockPointer.getPhysicalSize(), (int)aBlockPointer.getOffset()) != aBlockPointer.getChecksum())
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


	public BlockPointer writeBlock(byte[] aBuffer, int aOffset, int aLength, long aTransactionId, int aType, int aRange)
	{
		try
		{
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			int result = compress(aBuffer, aOffset, aLength, aType, baos);

			int compression = result >>> 24;
			int physicalSize;

			if (compression == 0)
			{
				physicalSize = aLength;
				aBuffer = Arrays.copyOfRange(aBuffer, aOffset, aOffset + roundUp(aLength));
			}
			else
			{
				physicalSize = result & 0xffffff;
				aBuffer = baos.toByteArray();
			}

			assert aBuffer.length % mPageSize == 0;

			int blockCount = aBuffer.length / mPageSize;
			long blockIndex = mBlockDevice.allocBlock(blockCount);
			Stats.blockAlloc++;

			BlockPointer blockPointer = new BlockPointer();
			blockPointer.setCompression(compression);
			blockPointer.setChecksum(MurmurHash3.hash_x86_32(aBuffer, 0, physicalSize, (int)blockIndex));
			blockPointer.setBlockKey(ISAAC.PRNG.nextLong());
			blockPointer.setOffset(blockIndex);
			blockPointer.setPhysicalSize(physicalSize);
			blockPointer.setLogicalSize(aLength);
			blockPointer.setTransactionId(aTransactionId);
			blockPointer.setType(aType);
			blockPointer.setRange(aRange);

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


	private int compress(byte[] aBuffer, int aOffset, int aLength, int aType, ByteArrayOutputStream baos) throws IOException
	{
		int compressor = aType == Node.BLOB ? mCompressionParam.getBlob() : aType == Node.LEAF ? mCompressionParam.getLeaf() : mCompressionParam.getNode();

		if (compressor > 0)
		{
			try (DeflaterOutputStream dis = new DeflaterOutputStream(baos, new Deflater(compressor)))
			{
				dis.write(aBuffer, aOffset, aLength);
			}

			int physicalSize = baos.size();
			
			if (roundUp(physicalSize) < roundUp(aLength))
			{
				baos.write(new byte[roundUp(physicalSize) - physicalSize]); // padding

				return (compressor << 24) + physicalSize;
			}
		}

		return 0;
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
