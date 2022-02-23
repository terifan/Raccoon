package org.terifan.raccoon;

import java.util.HashMap;
import org.terifan.raccoon.storage.BlockPointer;


public class BTreeNode
{
	BlockPointer mBlockPointer;
	ArrayMap mMap;
	HashMap<MarshalledKey, BTreeNode> mChildren;
	boolean mIndexNode;
	boolean mChanged;


	BTreeNode(ArrayMap aMap, boolean aIndexNode)
	{
		mMap = aMap;
		mIndexNode = aIndexNode;

		if (mIndexNode)
		{
			mChildren = new HashMap<>();
		}
	}
}
