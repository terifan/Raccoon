package org.terifan.raccoon.io.managed;

import java.util.HashSet;
import org.terifan.raccoon.BlockType;
import org.terifan.raccoon.CompressionParam;
import org.terifan.raccoon.io.DatabaseIOException;
import org.terifan.raccoon.io.physical.IPhysicalBlockDevice;
import org.terifan.raccoon.io.secure.BlockKeyGenerator;
import org.terifan.raccoon.storage.BlockPointer;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Log;
import org.terifan.security.messagedigest.MurmurHash3;


class SpaceMap
{
	private RangeMap mRangeMap;
	private RangeMap mPendingRangeMap;
	private HashSet<Integer> mUncommittedAllocations;


	public SpaceMap()
	{
		mUncommittedAllocations = new HashSet<>();

		mRangeMap = new RangeMap();
		mRangeMap.add(0, Integer.MAX_VALUE);

		mPendingRangeMap = mRangeMap.clone();
	}


	public SpaceMap(SuperBlock aSuperBlock, ManagedBlockDevice aBlockDevice, IPhysicalBlockDevice aBlockDeviceDirect)
	{
		mUncommittedAllocations = new HashSet<>();

		mRangeMap = read(aSuperBlock, aBlockDevice, aBlockDeviceDirect);

		mPendingRangeMap = mRangeMap.clone();
	}


	public RangeMap getRangeMap()
	{
		return mRangeMap;
	}


	public int alloc(int aBlockCount)
	{
		int blockIndex = mRangeMap.next(aBlockCount);

		if (blockIndex < 0)
		{
			return -1;
		}

		Log.d("alloc block %d +%d", blockIndex, aBlockCount);

		for (int i = 0; i < aBlockCount; i++)
		{
			mUncommittedAllocations.add(blockIndex + i);
		}

		mPendingRangeMap.remove(blockIndex, aBlockCount);

		return blockIndex;
	}


	public void free(long aBlockIndex, int aBlockCount)
	{
		int blockIndex = (int)aBlockIndex;

		for (int i = 0; i < aBlockCount; i++)
		{
			if (mUncommittedAllocations.remove(blockIndex + i))
			{
				mRangeMap.add(blockIndex + i, 1);
			}
		}

		mPendingRangeMap.add(blockIndex, aBlockCount);
	}


	public void assertUsed(long aBlockIndex, int aBlockCount)
	{
		if (!mRangeMap.isFree((int)aBlockIndex, aBlockCount))
		{
			throw new DatabaseIOException("Range not allocated: " + aBlockIndex + " +" + aBlockCount);
		}
	}


	public void rollback()
	{
		mPendingRangeMap = mRangeMap.clone();
	}


	public void reset()
	{
		mUncommittedAllocations.clear();
	}


	public void write(BlockPointer aSpaceMapBlockPointer, ManagedBlockDevice aBlockDevice, IPhysicalBlockDevice aBlockDeviceDirect)
	{
		Log.d("write space map");
		Log.inc();

		int blockSize = aBlockDevice.getBlockSize();

		if (aSpaceMapBlockPointer.getAllocatedBlocks() > 0)
		{
			aBlockDevice.freeBlockInternal(aSpaceMapBlockPointer.getBlockIndex0(), aSpaceMapBlockPointer.getAllocatedBlocks());
		}

		ByteArrayBuffer buffer = ByteArrayBuffer.alloc(blockSize);

		mPendingRangeMap.marshal(buffer);

		int allocBlocks = (buffer.position() + blockSize - 1) / blockSize;

		long blockIndex = aBlockDevice.allocBlockInternal(allocBlocks);
		long[] blockKey = BlockKeyGenerator.generate();

		aSpaceMapBlockPointer.setCompressionAlgorithm(CompressionParam.NONE);
		aSpaceMapBlockPointer.setBlockType(BlockType.SPACEMAP);
		aSpaceMapBlockPointer.setAllocatedBlocks(allocBlocks);
		aSpaceMapBlockPointer.setBlockIndex0(blockIndex);
		aSpaceMapBlockPointer.setLogicalSize(buffer.position());
		aSpaceMapBlockPointer.setPhysicalSize(blockSize * allocBlocks);
		aSpaceMapBlockPointer.setChecksumAlgorithm((byte)0); // not used
		aSpaceMapBlockPointer.setChecksum(MurmurHash3.hash256(buffer.array(), 0, buffer.position(), aSpaceMapBlockPointer.getTransactionId()));
		aSpaceMapBlockPointer.setBlockKey(blockKey);

		// Pad buffer to block size
		buffer.capacity(blockSize * allocBlocks);

		aBlockDeviceDirect.writeBlock(blockIndex, buffer.array(), 0, buffer.capacity(), blockKey);

		mRangeMap = mPendingRangeMap.clone();

		Log.dec();
	}


	private RangeMap read(SuperBlock aSuperBlock, ManagedBlockDevice aBlockDevice, IPhysicalBlockDevice aBlockDeviceDirect)
	{
		BlockPointer blockPointer = aSuperBlock.getSpaceMapPointer();

		Log.d("read space map %d +%d (bytes used %d)", blockPointer.getBlockIndex0(), blockPointer.getAllocatedBlocks(), blockPointer.getLogicalSize());
		Log.inc();

		RangeMap rangeMap = new RangeMap();

		if (blockPointer.getAllocatedBlocks() == 0)
		{
			// all blocks are free in this device
			rangeMap.add(0, Integer.MAX_VALUE);
		}
		else
		{
			if (blockPointer.getBlockIndex0() < 0)
			{
				throw new DatabaseIOException("Block at illegal offset: " + blockPointer.getBlockIndex0());
			}

			int blockSize = aBlockDevice.getBlockSize();

			ByteArrayBuffer buffer = ByteArrayBuffer.alloc(blockSize * blockPointer.getAllocatedBlocks());

			aBlockDeviceDirect.readBlock(blockPointer.getBlockIndex0(), buffer.array(), 0, blockSize * blockPointer.getAllocatedBlocks(), blockPointer.getBlockKey(new long[4]));

			long[] hash = MurmurHash3.hash256(buffer.array(), 0, blockPointer.getLogicalSize(), blockPointer.getTransactionId());

			if (!blockPointer.verifyChecksum(hash))
			{
				throw new DatabaseIOException("Checksum error at block index ");
			}

			buffer.limit(blockPointer.getLogicalSize());

			rangeMap.unmarshal(buffer);

			rangeMap.remove((int)blockPointer.getBlockIndex0(), blockPointer.getAllocatedBlocks());
		}

		Log.dec();

		return rangeMap;
	}
}
