package org.terifan.raccoon.io.managed;

import java.io.IOException;
import org.terifan.raccoon.io.physical.IPhysicalBlockDevice;
import org.terifan.raccoon.io.secure.SecureBlockDevice;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.security.cryptography.ISAAC;
import org.terifan.security.messagedigest.MurmurHash3;


final class SuperBlockIO
{
	private final static int CHECKSUM_SIZE = 16;
	private final static int IV_SIZE = 16;


	SuperBlock readSuperBlock(IPhysicalBlockDevice aBlockDevice, long aBlockIndex) throws IOException
	{
		int blockSize = aBlockDevice.getBlockSize();

		ByteArrayBuffer buffer = new ByteArrayBuffer(blockSize);

		if (aBlockDevice instanceof SecureBlockDevice)
		{
			((SecureBlockDevice)aBlockDevice).readBlockWithIV(aBlockIndex, buffer.array(), 0, blockSize);
		}
		else
		{
			aBlockDevice.readBlock(aBlockIndex, buffer.array(), 0, buffer.capacity(), 0L, 0L);
		}

		long[] hash = MurmurHash3.hash_x64_128(buffer.array(), CHECKSUM_SIZE, blockSize - CHECKSUM_SIZE - IV_SIZE, aBlockIndex);

		buffer.position(0);

		if (buffer.readInt64() != hash[0] || buffer.readInt64() != hash[1])
		{
			throw new IOException("Checksum error at block index " + aBlockIndex);
		}

		SuperBlock superBlock = new SuperBlock();
		superBlock.unmarshal(buffer);

		return superBlock;
	}


	void writeSuperBlock(IPhysicalBlockDevice aBlockDevice, long aBlockIndex, SuperBlock aSuperBlock) throws IOException
	{
		if (aBlockIndex < 0)
		{
			throw new IOException("Block at illegal offset: " + aBlockIndex);
		}

		aSuperBlock.mUpdated = System.currentTimeMillis();

		int blockSize = aBlockDevice.getBlockSize();

		ByteArrayBuffer buffer = new ByteArrayBuffer(blockSize);
		buffer.position(CHECKSUM_SIZE); // reserve space for checksum

		aSuperBlock.marshal(buffer);

		if (buffer.remaining() < IV_SIZE)
		{
			throw new IOException("SuperBlock marshalled into a too large buffer");
		}

		if (aBlockDevice instanceof SecureBlockDevice)
		{
			ISAAC.PRNG.nextBytes(buffer.array(), buffer.position(), buffer.remaining() - IV_SIZE);
		}

		long[] hash = MurmurHash3.hash_x64_128(buffer.array(), CHECKSUM_SIZE, blockSize - CHECKSUM_SIZE - IV_SIZE, aBlockIndex);

		buffer.position(0);
		buffer.writeInt64(hash[0]);
		buffer.writeInt64(hash[1]);

		if (aBlockDevice instanceof SecureBlockDevice)
		{
			((SecureBlockDevice)aBlockDevice).writeBlockWithIV(aBlockIndex, buffer.array(), 0, blockSize);
		}
		else
		{
			aBlockDevice.writeBlock(aBlockIndex, buffer.array(), 0, blockSize, 0L, 0L);
		}
	}
}
