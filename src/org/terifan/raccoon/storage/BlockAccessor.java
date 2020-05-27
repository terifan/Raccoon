package org.terifan.raccoon.storage;

import org.terifan.raccoon.BlockType;
import java.io.IOException;
import java.util.Arrays;
import java.util.zip.Deflater;
import org.terifan.raccoon.CompressionParam;
import org.terifan.raccoon.DatabaseException;
import org.terifan.raccoon.io.managed.IManagedBlockDevice;
import org.terifan.raccoon.util.ByteBlockOutputStream;
import org.terifan.raccoon.util.Log;
import org.terifan.security.cryptography.ISAAC;
import org.terifan.security.messagedigest.MurmurHash3;


public class BlockAccessor implements IBlockAccessor
{
	private final IManagedBlockDevice mBlockDevice;
	private CompressionParam mCompressionParam;


	public BlockAccessor(IManagedBlockDevice aBlockDevice, CompressionParam aCompressionParam)
	{
		mBlockDevice = aBlockDevice;
		mCompressionParam = aCompressionParam;
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


	@Override
	public void freeBlock(BlockPointer aBlockPointer)
	{
		try
		{
			Log.d("free block %s", aBlockPointer);
			Log.inc();

			mBlockDevice.freeBlock(aBlockPointer.getBlockIndex0(), roundUp(aBlockPointer.getPhysicalSize()) / mBlockDevice.getBlockSize());

			Log.dec();
		}
		catch (Exception | Error e)
		{
			throw new DatabaseException(aBlockPointer.toString(), e);
		}
	}


	@Override
	public byte[] readBlock(BlockPointer aBlockPointer)
	{
		try
		{
			Log.d("read block %s", aBlockPointer);
			Log.inc();

// TODO: remove
byte[] buffer;
if (aBlockPointer.getAllocatedSize() == 0)
{
	buffer = new byte[roundUp(aBlockPointer.getLogicalSize())];
}
else
{
	buffer = new byte[aBlockPointer.getAllocatedSize() * mBlockDevice.getBlockSize()];
}

			mBlockDevice.readBlock(aBlockPointer.getBlockIndex0(), buffer, 0, buffer.length, aBlockPointer.getIV());

			long[] hash = MurmurHash3.hash128(buffer, 0, aBlockPointer.getPhysicalSize(), aBlockPointer.getBlockIndex0());

			if (!aBlockPointer.verifyChecksum(hash))
			{
				throw new IOException("Checksum error in block " + aBlockPointer);
			}

			if (aBlockPointer.getCompressionAlgorithm() != CompressionParam.NONE)
			{
				byte[] tmp = new byte[aBlockPointer.getLogicalSize()];
				getCompressor(aBlockPointer.getCompressionAlgorithm()).decompress(buffer, 0, aBlockPointer.getPhysicalSize(), tmp, 0, tmp.length);
				buffer = tmp;
			}
			else if (aBlockPointer.getLogicalSize() < buffer.length)
			{
				buffer = Arrays.copyOfRange(buffer, 0, aBlockPointer.getLogicalSize());
			}

			Log.dec();

			return buffer;
		}
		catch (Exception | Error e)
		{
			throw new DatabaseException("Error reading block: " + aBlockPointer, e);
		}
	}


	public BlockPointer writeBlock(byte[] aBuffer, int aOffset, int aLength, long aTransactionId, BlockType aType, int aRange)
	{
		BlockPointer blockPointer = null;

		try
		{
			ByteBlockOutputStream compressedBlock = null;
			int compressor = mCompressionParam.getCompressorId(aType);
			boolean compressed = false;

			if (compressor != CompressionParam.NONE)
			{
				compressedBlock = new ByteBlockOutputStream(mBlockDevice.getBlockSize());
				compressed = getCompressor(compressor).compress(aBuffer, aOffset, aLength, compressedBlock);
			}

			int physicalSize;

			if (compressed && roundUp(compressedBlock.size()) < roundUp(aBuffer.length)) // use the compressed result only if we actual save one block or more
			{
				physicalSize = compressedBlock.size();
				aBuffer = compressedBlock.getBuffer();
			}
			else
			{
				physicalSize = aLength;
				aBuffer = Arrays.copyOfRange(aBuffer, aOffset, aOffset + roundUp(aLength));
				compressor = CompressionParam.NONE;
			}

			int allocatedSize = aBuffer.length / mBlockDevice.getBlockSize();
			long blockIndex = mBlockDevice.allocBlock(allocatedSize);

			blockPointer = new BlockPointer();
			blockPointer.setCompressionAlgorithm(compressor);
			blockPointer.setBlockIndex(blockIndex);
			blockPointer.setAllocatedSize(allocatedSize);
			blockPointer.setPhysicalSize(physicalSize);
			blockPointer.setLogicalSize(aLength);
			blockPointer.setTransactionId(aTransactionId);
			blockPointer.setBlockType(aType);
			blockPointer.setRange(aRange);
			blockPointer.setChecksum(MurmurHash3.hash128(aBuffer, 0, physicalSize, blockIndex));
			blockPointer.setIV(ISAAC.PRNG.nextLong(), ISAAC.PRNG.nextLong());

			Log.d("write block %s", blockPointer);
			Log.inc();

			mBlockDevice.writeBlock(blockIndex, aBuffer, 0, aBuffer.length, blockPointer.getIV());

			Log.dec();

			return blockPointer;
		}
		catch (Exception | Error e)
		{
			throw new DatabaseException("Error writing block: " + blockPointer, e);
		}
	}


	private Compressor getCompressor(int aCompressorId)
	{
		switch (aCompressorId)
		{
			case CompressionParam.ZLE:
				return new ZeroCompressor(mBlockDevice.getBlockSize());
			case CompressionParam.DEFLATE_FAST:
				return new DeflateCompressor(Deflater.BEST_SPEED);
			case CompressionParam.DEFLATE_DEFAULT:
				return new DeflateCompressor(Deflater.DEFAULT_COMPRESSION);
			case CompressionParam.DEFLATE_BEST:
				return new DeflateCompressor(Deflater.BEST_COMPRESSION);
			default:
				throw new IllegalStateException("Illegal compressor: " + aCompressorId);
		}
	}


	private int roundUp(int aSize)
	{
		int s = mBlockDevice.getBlockSize();
		return aSize + ((s - (aSize % s)) % s);
	}
}
