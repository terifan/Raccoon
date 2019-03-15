package org.terifan.raccoon;

import org.terifan.raccoon.storage.BlockPointer;
import static org.testng.Assert.*;
import org.testng.annotations.Test;


public class HashTableNodeNGTest
{
	@Test
	public void testCreatingNode()
	{
		HashTableNode node = new HashTableNode(null, null, new byte[512]);

		BlockPointer bp0 = new BlockPointer();
		bp0.setRangeOffset(0);
		bp0.setRangeSize(512 / 2 / BlockPointer.SIZE);
		node.setPointer(bp0);

		BlockPointer bp1 = new BlockPointer();
		bp1.setRangeOffset(512 / 2 / BlockPointer.SIZE);
		bp1.setRangeSize(512 / 2 / BlockPointer.SIZE);
		node.setPointer(bp1);

		System.out.println(node.printRanges());

		assertNull(node.integrityCheck());
	}
}
