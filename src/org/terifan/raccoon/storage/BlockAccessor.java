package org.terifan.raccoon.storage;

import org.terifan.raccoon.BlockType;
import java.io.IOException;
import java.util.Arrays;
import java.util.zip.Deflater;
import org.terifan.raccoon.CompressionParam;
import org.terifan.raccoon.DatabaseException;
import static org.terifan.raccoon.RuntimeDiagnostics.Operation.*;
import static org.terifan.raccoon.RuntimeDiagnostics.collectStatistics;
import org.terifan.raccoon.io.managed.IManagedBlockDevice;
import org.terifan.raccoon.io.secure.BlockKeyGenerator;
import org.terifan.raccoon.util.ByteBlockOutputStream;
import org.terifan.raccoon.util.Log;
import org.terifan.security.messagedigest.MurmurHash3;


public class BlockAccessor implements IBlockAccessor, AutoCloseable
{
	private final IManagedBlockDevice mBlockDevice;
	private final CompressionParam mCompressionParam;
	private boolean mCloseUnderlyingDevice;


	public BlockAccessor(IManagedBlockDevice aBlockDevice, CompressionParam aCompressionParam, boolean aCloseUnderlyingDevice)
	{
		mBlockDevice = aBlockDevice;
		mCompressionParam = aCompressionParam;
		mCloseUnderlyingDevice = aCloseUnderlyingDevice;
	}


	public IManagedBlockDevice getBlockDevice()
	{
		return mBlockDevice;
	}


	@Override
	public void close()
	{
		if (mCloseUnderlyingDevice)
		{
			mBlockDevice.close();
		}
	}


	@Override
	public synchronized void freeBlock(BlockPointer aBlockPointer)
	{
		try
		{
			Log.d("free block %s", aBlockPointer);
			Log.inc();

			mBlockDevice.freeBlock(aBlockPointer.getBlockIndex0(), aBlockPointer.getAllocatedSize() / mBlockDevice.getBlockSize());

			assert collectStatistics(FREE_BLOCK, aBlockPointer.getAllocatedSize());

			Log.dec();
		}
		catch (Exception | Error e)
		{
			throw new DatabaseException(aBlockPointer.toString(), e);
		}
	}


	@Override
	public synchronized byte[] readBlock(BlockPointer aBlockPointer)
	{
		try
		{
			Log.d("read block %s", aBlockPointer);
			Log.inc();

			byte[] buffer = new byte[aBlockPointer.getAllocatedSize()];

			mBlockDevice.readBlock(aBlockPointer.getBlockIndex0(), buffer, 0, buffer.length, aBlockPointer.getBlockKey(new int[8]));

			long[] hash = MurmurHash3.hash256(buffer, 0, aBlockPointer.getPhysicalSize(), aBlockPointer.getTransactionId());

			if (!aBlockPointer.verifyChecksum(hash))
			{
				throw new IOException("Checksum error in block " + aBlockPointer);
			}

			if (aBlockPointer.getCompressionAlgorithm() != CompressionParam.Level.NONE.ordinal())
			{
				byte[] tmp = new byte[aBlockPointer.getLogicalSize()];
				getCompressor(aBlockPointer.getCompressionAlgorithm()).decompress(buffer, 0, aBlockPointer.getPhysicalSize(), tmp, 0, tmp.length);
				buffer = tmp;
			}
			else if (aBlockPointer.getLogicalSize() < buffer.length)
			{
				buffer = Arrays.copyOfRange(buffer, 0, aBlockPointer.getLogicalSize());
			}

			assert collectStatistics(READ_BLOCK, buffer.length);

			Log.dec();

			return buffer;
		}
		catch (Exception | Error e)
		{
			throw new DatabaseException("Error reading block: " + aBlockPointer, e);
		}
	}


	@Override
	public synchronized BlockPointer writeBlock(byte[] aBuffer, int aOffset, int aLength, BlockType aType)
	{
		long transactionId = getBlockDevice().getTransactionId();

		BlockPointer blockPointer = null;

		try
		{
			ByteBlockOutputStream compressedBlock = null;
			CompressionParam.Level compressor = mCompressionParam.getCompressorLevel(aType);
			boolean compressed = false;

			if (compressor != CompressionParam.Level.NONE)
			{
				compressedBlock = new ByteBlockOutputStream(mBlockDevice.getBlockSize());
				compressed = getCompressor(compressor.ordinal()).compress(aBuffer, aOffset, aLength, compressedBlock);
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
				compressor = CompressionParam.Level.NONE;
			}

			assert aBuffer.length % mBlockDevice.getBlockSize() == 0;

			long blockIndex = mBlockDevice.allocBlock(aBuffer.length / mBlockDevice.getBlockSize());
			int[] blockKey = BlockKeyGenerator.generate();

			blockPointer = new BlockPointer();
			blockPointer.setCompressionAlgorithm(compressor.ordinal());
			blockPointer.setAllocatedSize(aBuffer.length);
			blockPointer.setPhysicalSize(physicalSize);
			blockPointer.setLogicalSize(aLength);
			blockPointer.setTransactionId(transactionId);
			blockPointer.setBlockType(aType);
			blockPointer.setChecksumAlgorithm((byte)0); // not used
			blockPointer.setChecksum(MurmurHash3.hash256(aBuffer, 0, physicalSize, transactionId));
			blockPointer.setBlockKey(blockKey);
			blockPointer.setBlockIndex0(blockIndex);

			Log.d("write block %s", blockPointer);
			Log.inc();

			mBlockDevice.writeBlock(blockIndex, aBuffer, 0, aBuffer.length, blockKey);

			assert collectStatistics(WRITE_BLOCK, aBuffer.length);

			Log.dec();

			return blockPointer;
		}
		catch (Exception | Error e)
		{
			throw new DatabaseException("Error writing block: " + blockPointer, e);
		}
	}


	private Compressor getCompressor(int aLevel)
	{
		switch (CompressionParam.Level.values()[aLevel])
		{
			case ZLE:
				return new ZLE(mBlockDevice.getBlockSize());
			case DEFLATE_FAST:
				return new DeflateCompressor(Deflater.BEST_SPEED);
			case DEFLATE_DEFAULT:
				return new DeflateCompressor(Deflater.DEFAULT_COMPRESSION);
			case DEFLATE_BEST:
				return new DeflateCompressor(Deflater.BEST_COMPRESSION);
			default:
				throw new IllegalStateException("Illegal compressor: " + aLevel);
		}
	}


	private int roundUp(int aSize)
	{
		int s = mBlockDevice.getBlockSize();
		int d = aSize % s;
		if (d == 0)
		{
			return aSize;
		}
		return aSize + (s - d);
	}
}
