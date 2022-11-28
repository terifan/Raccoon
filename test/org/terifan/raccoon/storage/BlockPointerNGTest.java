package org.terifan.raccoon.storage;

import org.terifan.raccoon.BlockType;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.ByteArrayUtil;
import org.terifan.raccoon.util.Log;
import static org.testng.Assert.*;
import org.testng.annotations.Test;


public class BlockPointerNGTest
{
	@Test
	public void testSomeMethod()
	{
		BlockPointer bp = new BlockPointer()
			.setBlockType(BlockType.HOLE)
			.setBlockLevel(0x02)
			.setChecksumAlgorithm((byte)0x03)
			.setCompressionAlgorithm((byte)0x04)
			.setAllocatedSize(0x05060708)
			.setLogicalSize(0x09101112)
			.setPhysicalSize(0x13141516)
			.setBlockIndex0(0x1718192021222324L)
			.setBlockIndex1(0x2526272829303132L)
			.setBlockIndex2(0x3334353637383940L)
			.setUserData(0x4142434445464748L)
			.setTransactionId(0x4950515253545556L)
			.setBlockKey(new long[]{0x5758596061626364L,0x6566676869707172L,0x7374757677787980L,0x8182838485868788L})
			.setChecksum(new long[]{0x8990919293949596L,0x9798990102030405L,0x0607080910111213L,0x1415161718192021L})
			;

		Log.hexDump(bp.marshal(ByteArrayBuffer.alloc(BlockPointer.SIZE)).array(), 8);
	}
}
