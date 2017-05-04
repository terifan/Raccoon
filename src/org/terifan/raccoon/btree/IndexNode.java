package org.terifan.raccoon.btree;

import org.terifan.raccoon.core.Node;
import org.terifan.raccoon.PerformanceCounters;
import static org.terifan.raccoon.PerformanceCounters.*;
import org.terifan.raccoon.storage.BlockPointer;
import org.terifan.raccoon.core.BlockType;
import org.terifan.raccoon.util.ByteArrayBuffer;


final class IndexNode implements Node
{
	@Override
	public byte[] array()
	{
		return null;
	}


	@Override
	public BlockType getType()
	{
		return BlockType.INDEX;
	}
}
