package org.terifan.raccoon.io.managed;

import java.io.IOException;
import java.util.HashSet;
import org.terifan.raccoon.BlockType;
import org.terifan.raccoon.io.physical.IPhysicalBlockDevice;
import org.terifan.raccoon.storage.BlockPointer;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Log;
import org.terifan.security.cryptography.ISAAC;
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


	public SpaceMap(SuperBlock aSuperBlock, ManagedBlockDevice aBlockDevice, IPhysicalBlockDevice aBlockDeviceDirect) throws IOException
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


	public void assertUsed(long aBlockIndex, int aBlockCount) throws IOException
	{
		if (!mRangeMap.isFree((int)aBlockIndex, aBlockCount))
		{
			throw new IOException("Range not allocted: " + aBlockIndex + " +" + aBlockCount);
		}
	}


	public void rollback()
	{
		mPendingRangeMap = mRangeMap.clone();
	}


	public void clearUncommitted()
	{
		mUncommittedAllocations.clear();
	}


	public void write(BlockPointer aSpaceMapBlockPointer, ManagedBlockDevice aBlockDevice, IPhysicalBlockDevice aBlockDeviceDirect) throws IOException
	{
		Log.d("write space map");
		Log.inc();

		int blockSize = aBlockDevice.getBlockSize();

		if (aSpaceMapBlockPointer.getAllocatedSize() > 0)
		{
			aBlockDevice.freeBlockInternal(aSpaceMapBlockPointer.getBlockIndex0(), aSpaceMapBlockPointer.getAllocatedSize());
		}

		ByteArrayBuffer buffer = new ByteArrayBuffer(blockSize);

		mPendingRangeMap.marshal(buffer);

		aSpaceMapBlockPointer.setBlockType(BlockType.SPACEMAP);
		aSpaceMapBlockPointer.setAllocatedSize((buffer.position() + blockSize - 1) / blockSize);
		aSpaceMapBlockPointer.setBlockIndex(aBlockDevice.allocBlockInternal(aSpaceMapBlockPointer.getAllocatedSize()));
		aSpaceMapBlockPointer.setLogicalSize(buffer.position());
		aSpaceMapBlockPointer.setPhysicalSize(blockSize * aSpaceMapBlockPointer.getAllocatedSize());
		aSpaceMapBlockPointer.setChecksum(MurmurHash3.hash128(buffer.array(), 0, buffer.position(), 0L));
		aSpaceMapBlockPointer.setIV(ISAAC.PRNG.nextLong(), ISAAC.PRNG.nextLong());

		// Pad buffer to block size
		buffer.capacity(blockSize * aSpaceMapBlockPointer.getAllocatedSize());

		aBlockDeviceDirect.writeBlock(aSpaceMapBlockPointer.getBlockIndex0(), buffer.array(), 0, buffer.capacity(), aSpaceMapBlockPointer.getIV());

		mRangeMap = mPendingRangeMap.clone();
		
		Log.dec();
	}


	private RangeMap read(SuperBlock aSuperBlock, ManagedBlockDevice aBlockDevice, IPhysicalBlockDevice aBlockDeviceDirect) throws IOException
	{
		BlockPointer blockPointer = aSuperBlock.getSpaceMapPointer();

		Log.d("read space map %d +%d (bytes used %d)", blockPointer.getBlockIndex0(), blockPointer.getAllocatedSize(), blockPointer.getLogicalSize());
		Log.inc();

		RangeMap rangeMap = new RangeMap();

		if (blockPointer.getAllocatedSize() == 0)
		{
			// all blocks are free in this device
			rangeMap.add(0, Integer.MAX_VALUE);
		}
		else
		{
			if (blockPointer.getBlockIndex0() < 0)
			{
				throw new IOException("Block at illegal offset: " + blockPointer.getBlockIndex0());
			}

			int blockSize = aBlockDevice.getBlockSize();

			ByteArrayBuffer buffer = new ByteArrayBuffer(blockSize * blockPointer.getAllocatedSize());

			aBlockDeviceDirect.readBlock(blockPointer.getBlockIndex0(), buffer.array(), 0, blockSize * blockPointer.getAllocatedSize(), blockPointer.getIV());

			long[] hash = MurmurHash3.hash128(buffer.array(), 0, blockPointer.getLogicalSize(), 0L);

			if (!blockPointer.verifyChecksum(hash))
			{
				throw new IOException("Checksum error at block index ");
			}

			buffer.limit(blockPointer.getLogicalSize());

			rangeMap.unmarshal(buffer);

			rangeMap.remove((int)blockPointer.getBlockIndex0(), blockPointer.getAllocatedSize());
		}

		Log.dec();

		return rangeMap;
	}
}
