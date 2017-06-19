package org.terifan.raccoon.storage;

import org.terifan.raccoon.core.BlockType;
import org.terifan.raccoon.util.Cache;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.zip.Deflater;
import org.terifan.raccoon.CompressionParam;
import org.terifan.raccoon.DatabaseException;
import org.terifan.raccoon.PerformanceCounters;
import static org.terifan.raccoon.PerformanceCounters.*;
import org.terifan.raccoon.io.managed.IManagedBlockDevice;
import org.terifan.raccoon.util.Log;
import org.terifan.security.messagedigest.MurmurHash3;


public class BlockAccessor
{
	private final IManagedBlockDevice mBlockDevice;
	private final Cache<Long,byte[]> mCache;
	private final int mBlockSize;
	private CompressionParam mCompressionParam;


	public BlockAccessor(IManagedBlockDevice aBlockDevice, CompressionParam aCompressionParam, int aCacheSize) throws IOException
	{
		mBlockDevice = aBlockDevice;
		mCompressionParam = aCompressionParam;
		mBlockSize = mBlockDevice.getBlockSize();
		mCache = aCacheSize == 0 ? null : new Cache<>(aCacheSize);
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
			Log.d("free block %s", aBlockPointer);
			Log.inc();

			mBlockDevice.freeBlock(aBlockPointer.getOffset(), roundUp(aBlockPointer.getPhysicalSize()) / mBlockDevice.getBlockSize());

			if (mCache != null)
			{
				mCache.remove(aBlockPointer.getOffset());
			}

			Log.dec();

			assert PerformanceCounters.increment(BLOCK_FREE);
		}
		catch (Exception | Error e)
		{
			throw new DatabaseException(aBlockPointer.toString(), e);
		}
	}


	public byte[] readBlock(BlockPointer aBlockPointer)
	{
		try
		{
			Log.d("read block %s", aBlockPointer);
			Log.inc();

			if (mCache != null)
			{
				byte[] copy = mCache.get(aBlockPointer.getOffset());

				if (copy != null)
				{
					Log.dec();

					return copy.clone();
				}
			}

			byte[] buffer = new byte[roundUp(aBlockPointer.getPhysicalSize())];

			mBlockDevice.readBlock(aBlockPointer.getOffset(), buffer, 0, buffer.length, aBlockPointer.getTransactionId());

			if (digest(buffer, 0, aBlockPointer.getPhysicalSize(), (int)aBlockPointer.getOffset()) != aBlockPointer.getChecksum())
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

			assert PerformanceCounters.increment(BLOCK_READ);

			return buffer;
		}
		catch (Exception | Error e)
		{
			throw new DatabaseException("Error reading block", e);
		}
	}


	public BlockPointer writeBlock(byte[] aBuffer, int aOffset, int aLength, long aTransactionId, BlockType aType, int aRange)
	{
		try
		{
			ByteArrayOutputStream baos = new ByteArrayOutputStream();

			int physicalSize;
			boolean result = false;
			int compressorId = mCompressionParam.getCompressorId(aType);

			if (compressorId != CompressionParam.NONE)
			{
				result = getCompressor(compressorId).compress(aBuffer, aOffset, aLength, baos);
			}

			byte[] copy = null;
			if (mCache != null)
			{
				copy = Arrays.copyOfRange(aBuffer, aOffset, aOffset + aLength);
			}

			if (result && roundUp(baos.size()) < roundUp(aBuffer.length)) // use the compressed result only if we actual save one block or more
			{
				physicalSize = baos.size();
				baos.write(new byte[roundUp(physicalSize) - physicalSize]); // padding
				aBuffer = baos.toByteArray();
			}
			else
			{
				physicalSize = aLength;
				aBuffer = Arrays.copyOfRange(aBuffer, aOffset, aOffset + roundUp(aLength));
				compressorId = CompressionParam.NONE;
			}

			assert aBuffer.length % mBlockSize == 0;

			int blockCount = aBuffer.length / mBlockSize;
			long blockIndex = mBlockDevice.allocBlock(blockCount);

			assert PerformanceCounters.increment(BLOCK_ALLOC);

			BlockPointer blockPointer = new BlockPointer();
			blockPointer.setCompression(compressorId);
			blockPointer.setChecksum(digest(aBuffer, 0, physicalSize, blockIndex));
			blockPointer.setOffset(blockIndex);
			blockPointer.setPhysicalSize(physicalSize);
			blockPointer.setLogicalSize(aLength);
			blockPointer.setTransactionId(aTransactionId);
			blockPointer.setBlockType(aType);
			blockPointer.setRange(aRange);

			Log.d("write block %s", blockPointer);
			Log.inc();

			mBlockDevice.writeBlock(blockIndex, aBuffer, 0, aBuffer.length, blockPointer.getTransactionId());

			assert PerformanceCounters.increment(BLOCK_WRITE);

			Log.dec();

			if (mCache != null)
			{
				mCache.put(blockPointer.getOffset(), copy);
			}

			return blockPointer;
		}
		catch (Exception | Error e)
		{
			throw new DatabaseException("Error writing block", e);
		}
	}


	private int digest(byte[] aBuffer, int aOffset, int aLength, long aBlockIndex)
	{
		return (int)MurmurHash3.hash_x64_64(aBuffer, aOffset, aLength, aBlockIndex);
	}


	private Compressor getCompressor(int aCompressorId)
	{
		switch (aCompressorId)
		{
			case CompressionParam.ZLE:
				return new ZeroCompressor(mBlockSize);
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
		return aSize + ((mBlockSize - (aSize % mBlockSize)) % mBlockSize);
	}
}
