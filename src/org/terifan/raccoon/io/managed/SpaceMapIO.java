package org.terifan.raccoon.io.managed;

import java.io.IOException;
import org.terifan.raccoon.core.BlockType;
import org.terifan.raccoon.io.physical.IPhysicalBlockDevice;
import org.terifan.raccoon.storage.BlockPointer;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Log;
import org.terifan.security.cryptography.ISAAC;
import org.terifan.security.messagedigest.MurmurHash3;


public class SpaceMapIO
{
	RangeMap readSpaceMap(SuperBlock aSuperBlock, ManagedBlockDevice aBlockDevice, IPhysicalBlockDevice aBlockDeviceDirect) throws IOException
	{
		BlockPointer blockPointer = aSuperBlock.mSpaceMapPointer;

		Log.d("read space map %d +%d (bytes used %d)", blockPointer.getBlockIndex(), blockPointer.getAllocatedSize(), blockPointer.getLogicalSize());
		Log.inc();

		int blockSize = aBlockDevice.getBlockSize();

		RangeMap rangeMap = new RangeMap();

		if (blockPointer.getAllocatedSize() == 0) // all blocks are free in this device
		{
			rangeMap.add(0, Integer.MAX_VALUE);
		}
		else
		{
			if (blockPointer.getBlockIndex() < 0)
			{
				throw new IOException("Block at illegal offset: " + blockPointer.getBlockIndex());
			}

			ByteArrayBuffer buffer = new ByteArrayBuffer(blockSize * blockPointer.getAllocatedSize());

			aBlockDeviceDirect.readBlock(blockPointer.getBlockIndex(), buffer.array(), 0, blockSize * blockPointer.getAllocatedSize(), blockPointer.getIV0(), blockPointer.getIV1());

			long[] hash = MurmurHash3.hash_x64_128(buffer.array(), 0, blockPointer.getLogicalSize(), 0L);

			if (hash[0] != blockPointer.getChecksum0() || hash[1] != blockPointer.getChecksum1())
			{
				throw new IOException("Checksum error at block index ");
			}

			buffer.position(0);
			buffer.limit(blockPointer.getLogicalSize());

			rangeMap.unmarshal(buffer);

			rangeMap.remove((int)blockPointer.getBlockIndex(), blockPointer.getAllocatedSize());
		}

		Log.dec();

		return rangeMap;
	}


	void writeSpaceMap(SuperBlock aSuperBlock, RangeMap aRangeMap, ManagedBlockDevice aBlockDevice, IPhysicalBlockDevice aBlockDeviceDirect) throws IOException
	{
		Log.d("write space map");
		Log.inc();

		int blockSize = aBlockDevice.getBlockSize();

		BlockPointer blockPointer = aSuperBlock.mSpaceMapPointer;

		if (blockPointer.getAllocatedSize() > 0)
		{
			aBlockDevice.freeBlockInternal(blockPointer.getBlockIndex(), blockPointer.getAllocatedSize());
		}

		ByteArrayBuffer buffer = new ByteArrayBuffer(blockSize);

		aRangeMap.marshal(buffer);

		long[] hash = MurmurHash3.hash_x64_128(buffer.array(), 0, buffer.position(), 0L);

		// Allocate space for the new space map block
		blockPointer.setBlockType(BlockType.SPACEMAP);
		blockPointer.setAllocatedSize((buffer.position() + blockSize - 1) / blockSize);
		blockPointer.setBlockIndex(aBlockDevice.allocBlockInternal(blockPointer.getAllocatedSize()));
		blockPointer.setLogicalSize(buffer.position());
		blockPointer.setPhysicalSize(blockSize * blockPointer.getAllocatedSize());
		blockPointer.setChecksum0(hash[0]);
		blockPointer.setChecksum1(hash[1]);
		blockPointer.setIV0(ISAAC.PRNG.nextLong());
		blockPointer.setIV1(ISAAC.PRNG.nextLong());

		// Pad buffer to block size
		buffer.capacity(blockSize * blockPointer.getAllocatedSize());

		buffer.position(0);

		aBlockDeviceDirect.writeBlock(blockPointer.getBlockIndex(), buffer.array(), 0, buffer.capacity(), blockPointer.getIV0(), blockPointer.getIV1());

		Log.dec();
	}
}
