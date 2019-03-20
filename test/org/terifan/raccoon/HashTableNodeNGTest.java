package org.terifan.raccoon;

import org.terifan.raccoon.storage.BlockPointer;
import org.terifan.raccoon.storage.IBlockAccessor;
import static org.testng.Assert.*;
import org.testng.annotations.Test;


public class HashTableNodeNGTest
{
	@Test
	public void testPrintRanges()
	{
		HashTableNode node = createSimpleNode();

		assertEquals(node.printRanges(), "[97897,0,0,0],[3467,0],[5679],[2349]");
	}


	@Test
	public void testReadNode()
	{
		HashTableNode node = createSimpleNode();

		HashTableAbstractNode child = node.readBlock(node.getPointer(0));

		System.out.println(child);

//		assertEquals(node.printRanges(), "[97897,0,0,0],[3467,0],[5679],[2349]");
	}


	private HashTableNode createSimpleNode()
	{
		IBlockAccessor ba = new IBlockAccessor()
		{
			@Override
			public void freeBlock(BlockPointer aBlockPointer)
			{
				throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
			}


			@Override
			public byte[] readBlock(BlockPointer aBlockPointer)
			{
				return new ArrayMap(aBlockPointer.getAllocatedSize() * Constants.DEFAULT_BLOCK_SIZE).array();
			}


			@Override
			public BlockPointer writeBlock(byte[] aBuffer, int aOffset, int aLength, long aTransactionId, BlockType aType, int aRangeOffset, int aRangeSize, int aLevel)
			{
				throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
			}
		};

		HashTableNode node = new HashTableNode(null, ba, null, new byte[8 * BlockPointer.SIZE]);

		BlockPointer bp0 = new BlockPointer();
		bp0.setBlockType(BlockType.LEAF);
		bp0.setAllocatedSize(4);
		bp0.setLevel(4);
		bp0.setLogicalSize(15000);
		bp0.setPhysicalSize(15000);
		bp0.setBlockIndex0(97897);
		bp0.setRangeOffset(0);
		bp0.setRangeSize(4);
		node.setPointer(bp0);

		BlockPointer bp1 = new BlockPointer();
		bp1.setBlockType(BlockType.LEAF);
		bp1.setAllocatedSize(4);
		bp1.setLevel(4);
		bp1.setLogicalSize(15000);
		bp1.setPhysicalSize(15000);
		bp1.setBlockIndex0(3467);
		bp1.setRangeOffset(4);
		bp1.setRangeSize(2);
		node.setPointer(bp1);

		BlockPointer bp2 = new BlockPointer();
		bp2.setBlockType(BlockType.LEAF);
		bp2.setAllocatedSize(4);
		bp2.setLevel(4);
		bp2.setLogicalSize(15000);
		bp2.setPhysicalSize(15000);
		bp2.setBlockIndex0(5679);
		bp2.setRangeOffset(6);
		bp2.setRangeSize(1);
		node.setPointer(bp2);

		BlockPointer bp3 = new BlockPointer();
		bp3.setBlockType(BlockType.LEAF);
		bp3.setAllocatedSize(4);
		bp3.setLevel(4);
		bp3.setLogicalSize(15000);
		bp3.setPhysicalSize(15000);
		bp3.setBlockIndex0(2349);
		bp3.setRangeOffset(7);
		bp3.setRangeSize(1);
		node.setPointer(bp3);

		return node;
	}
}
