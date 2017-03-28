package org.terifan.raccoon.io;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.zip.Deflater;
import org.terifan.raccoon.CompressionParam;
import org.terifan.raccoon.DatabaseException;
import org.terifan.raccoon.Stats;
import org.terifan.security.random.ISAAC;
import org.terifan.raccoon.util.Log;
import org.terifan.security.messagedigest.MurmurHash3;


public class BlockAccessor
{
	private IManagedBlockDevice mBlockDevice;
	private CompressionParam mCompressionParam;
	private int mPageSize;
	private Cache<Long,byte[]> mCache;
	private boolean mCacheEnabled;


	public BlockAccessor(IManagedBlockDevice aBlockDevice) throws IOException
	{
		mBlockDevice = aBlockDevice;
		mPageSize = mBlockDevice.getBlockSize();
		mCache = new Cache<>(1024);

		mCompressionParam = CompressionParam.BEST_SPEED;

		mCacheEnabled = !false;
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
			Stats.blockFree.incrementAndGet();

			if (mCacheEnabled)
			{
				mCache.remove(aBlockPointer.getOffset());
			}

			Log.dec();
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
			Log.v("read block %s", aBlockPointer);
			Log.inc();

			if (mCacheEnabled)
			{
				byte[] copy = mCache.get(aBlockPointer.getOffset());

				if (copy != null)
				{
					Log.dec();

					return copy.clone();
				}
			}

			byte[] buffer = new byte[roundUp(aBlockPointer.getPhysicalSize())];

			mBlockDevice.readBlock(aBlockPointer.getOffset(), buffer, 0, buffer.length, aBlockPointer.getBlockKey());
			Stats.blockRead.incrementAndGet();

			if (digest(buffer, 0, aBlockPointer.getPhysicalSize(), (int)aBlockPointer.getOffset()) != aBlockPointer.getChecksum())
			{
				throw new IOException("Checksum error in block " + aBlockPointer);
			}

			if (aBlockPointer.getCompression() != CompressionParam.NONE)
			{
				byte[] tmp = new byte[aBlockPointer.getLogicalSize()];
				getCompressor(aBlockPointer.getCompression()).decompress(buffer, 0, aBlockPointer.getPhysicalSize(), tmp, 0, tmp.length);
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
			if (mCacheEnabled) // && aType == BlockPointer.Types.NODE_INDIRECT
			{
				copy = Arrays.copyOfRange(aBuffer, aOffset, aOffset + aLength);
			}

			if (result && roundUp(baos.size()) < roundUp(aBuffer.length)) // use the compressed result only if we actual save one page or more
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

			assert aBuffer.length % mPageSize == 0;

			int blockCount = aBuffer.length / mPageSize;
			long blockIndex = mBlockDevice.allocBlock(blockCount);
			Stats.blockAlloc.incrementAndGet();

			BlockPointer blockPointer = new BlockPointer();
			blockPointer.setCompression(compressorId);
			blockPointer.setChecksum(digest(aBuffer, 0, physicalSize, blockIndex));
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
			Stats.blockWrite.incrementAndGet();

			Log.dec();

			if (copy != null)
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
		return MurmurHash3.hash_x86_32(aBuffer, aOffset, aLength, (int)aBlockIndex) ^ (int)(aBlockIndex >>> 32);
	}


	private Compressor getCompressor(int aCompressorId)
	{
		Compressor compressor;
		switch (aCompressorId)
		{
			case CompressionParam.ZLE:
				compressor = new ZeroCompressor(mPageSize);
				break;
			case CompressionParam.DEFLATE_FAST:
				compressor = new DeflateCompressor(Deflater.BEST_SPEED);
				break;
			case CompressionParam.DEFLATE_DEFAULT:
				compressor = new DeflateCompressor(Deflater.DEFAULT_COMPRESSION);
				break;
			case CompressionParam.DEFLATE_BEST:
				compressor = new DeflateCompressor(Deflater.BEST_COMPRESSION);
				break;
			default:
				throw new IllegalStateException("Illegal compressor: " + aCompressorId);
		}
		return compressor;
	}


	private int roundUp(int aSize)
	{
		return aSize + ((mPageSize - (aSize % mPageSize)) % mPageSize);
	}
}
