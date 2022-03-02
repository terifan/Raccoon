package org.terifan.raccoon;

import java.util.HashMap;


public class BTreeIndex extends BTreeNode
{
	HashMap<MarshalledKey, BTreeNode> mChildren;


	BTreeIndex()
	{
		mChildren = new HashMap<>();
	}
}
