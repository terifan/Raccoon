package org.terifan.raccoon;

import java.util.Map.Entry;
import java.util.TreeMap;
import org.terifan.raccoon.ArrayMap.InsertResult;
import static org.terifan.raccoon.BTreeTableImplementation.POINTER_PLACEHOLDER;
import org.terifan.raccoon.storage.BlockPointer;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Result;
import static org.terifan.raccoon.BTreeTableImplementation.INDEX_SIZE;


public class BTreeIndex extends BTreeNode
{
	TreeMap<MarshalledKey, BTreeNode> mChildren;


	BTreeIndex(BTreeTableImplementation aImplementation, BTreeIndex aParent, int aLevel)
	{
		super(aImplementation, aParent, aLevel);

		mChildren = new TreeMap<>((o1, o2) -> o1.compareTo(o2));
	}


	@Override
	boolean get(MarshalledKey aKey, ArrayMapEntry aEntry)
	{
		ArrayMapEntry entry = new ArrayMapEntry(aKey.array());
		mMap.nearestIndexEntry(entry);

		MarshalledKey key = new MarshalledKey(entry.getKey());

		BTreeNode node = getNode(entry, key);

		return node.get(aKey, aEntry);
	}


	@Override
	InsertResult put(MarshalledKey aKey, ArrayMapEntry aEntry, Result<ArrayMapEntry> aResult)
	{
		mModified = true;

		ArrayMapEntry entry = new ArrayMapEntry(aKey.array());
		mMap.nearestIndexEntry(entry);

		MarshalledKey key = new MarshalledKey(entry.getKey());

		BTreeNode node = getNode(entry, key);

		if (node.put(aKey, aEntry, aResult) == InsertResult.PUT)
		{
			return InsertResult.PUT;
		}

		mMap.remove(entry.getKey(), null);

		SplitResult split = node.split();

		MarshalledKey rightKey = split.key();

		mChildren.put(key, split.left());
		mChildren.put(rightKey, split.right());

		boolean overflow = false;
		overflow |= mMap.insert(new ArrayMapEntry(key.array(), BTreeTableImplementation.POINTER_PLACEHOLDER, (byte)0x44), null) == InsertResult.RESIZED;
		overflow |= mMap.insert(new ArrayMapEntry(rightKey.array(), BTreeTableImplementation.POINTER_PLACEHOLDER, (byte)0x44), null) == InsertResult.RESIZED;

		return overflow ? InsertResult.RESIZED : InsertResult.PUT;
	}


	@Override
	boolean remove(MarshalledKey aKey, Result<ArrayMapEntry> aOldEntry)
	{
		mModified = true;

		int index = mMap.nearestIndex(aKey.array());

		BTreeNode curntChld = getNode(index);

		if (!curntChld.remove(aKey, aOldEntry))
		{
			return false;
		}

		BTreeNode leftChild = index == 0 ? null : getNode(index - 1);
		BTreeNode rghtChild = index == mMap.size() - 1 ? null : getNode(index + 1);

		boolean a = leftChild != null && (curntChld.mMap.size() == 1 || curntChld.mMap.getUsedSpace() + leftChild.mMap.getUsedSpace() < BTreeTableImplementation.LEAF_SIZE);
		boolean b = rghtChild != null && (rghtChild.mMap.size() == 1 || curntChld.mMap.getUsedSpace() + rghtChild.mMap.getUsedSpace() < BTreeTableImplementation.LEAF_SIZE);

		if (a && b)
		{
			if (leftChild.mMap.getUsedSpace() > rghtChild.mMap.getUsedSpace())
			{
				a = false;
			}
			else
			{
				b = false;
			}
		}

		if (BTreeTableImplementation.TESTINDEX == 97)
		{
			System.out.println("- " + a+" "+b+" "+curntChld+" "+leftChild+" "+rghtChild);
		}

		int z = 0;
		if (mLevel == 1)
		{
			if (a)
			{
				z=1;
				merge(index, (BTreeLeaf)curntChld, (BTreeLeaf)leftChild);
			}
			else if (b)
			{
				z=2;
				merge(index + 1, (BTreeLeaf)rghtChild, (BTreeLeaf)curntChld);
			}
		}
		else
		{
			if (a)
			{
				z=3;
				merge(index, (BTreeIndex)curntChld, (BTreeIndex)leftChild);
			}
			else if (b)
			{
				z=4;
				merge(index + 1, (BTreeIndex)rghtChild, (BTreeIndex)curntChld);
			}
		}

		if (BTreeTableImplementation.TESTINDEX == 97)
		{
			System.out.println(z+" "+a+" "+b+" "+curntChld+" "+leftChild+" "+rghtChild);
			BTreeTableImplementation.STOP = true;
		}

		return true;
	}


	@Override
	SplitResult split()
	{
		mImplementation.freeBlock(mBlockPointer);

		ArrayMap[] maps = mMap.split(INDEX_SIZE);

		BTreeIndex left = new BTreeIndex(mImplementation, this, mLevel);
		BTreeIndex right = new BTreeIndex(mImplementation, this, mLevel);
		left.mMap = maps[0];
		right.mMap = maps[1];
		left.mModified = true;
		right.mModified = true;

		byte[] midKeyBytes = maps[1].getKey(0);
		MarshalledKey midKey = new MarshalledKey(midKeyBytes);

		for (Entry<MarshalledKey, BTreeNode> entry : mChildren.entrySet())
		{
			if (entry.getKey().compareTo(midKey) < 0)
			{
				left.mChildren.put(entry.getKey(), entry.getValue());
			}
			else
			{
				right.mChildren.put(entry.getKey(), entry.getValue());
			}
		}

		ArrayMapEntry firstRight = right.mMap.getFirst();

		MarshalledKey keyLeft = new MarshalledKey(new byte[0]);
		MarshalledKey keyRight = new MarshalledKey(firstRight.getKey());

		BTreeNode firstChild = right.mChildren.get(keyRight);

		right.mMap.remove(firstRight.getKey(), null);
		right.mChildren.remove(keyRight);

		firstRight.setKey(keyLeft.array());

		right.mMap.put(firstRight, null);
		right.mChildren.put(keyLeft, firstChild);

		mChildren.clear();

		return new SplitResult(left, right, keyRight);
	}


	BTreeNode grow()
	{
		mImplementation.freeBlock(mBlockPointer);

		ArrayMap[] maps = mMap.split(INDEX_SIZE);

		BTreeIndex left = new BTreeIndex(mImplementation, this, mLevel);
		BTreeIndex right = new BTreeIndex(mImplementation, this, mLevel);
		left.mMap = maps[0];
		right.mMap = maps[1];
		left.mModified = true;
		right.mModified = true;

		byte[] midKeyBytes = right.mMap.getKey(0);

		MarshalledKey midKey = new MarshalledKey(midKeyBytes);

		for (Entry<MarshalledKey, BTreeNode> entry : mChildren.entrySet())
		{
			if (entry.getKey().compareTo(midKey) < 0)
			{
				left.mChildren.put(entry.getKey(), entry.getValue());
			}
			else
			{
				right.mChildren.put(entry.getKey(), entry.getValue());
			}
		}

		ArrayMapEntry first = right.mMap.getFirst();

		MarshalledKey keyLeft = new MarshalledKey(new byte[0]);
		MarshalledKey keyRight = new MarshalledKey(first.getKey());

		BTreeNode firstChild = right.mChildren.get(keyRight);

		right.mMap.remove(first.getKey(), null);
		right.mChildren.remove(keyRight);

		first.setKey(keyLeft.array());

		right.mMap.put(first, null);
		right.mChildren.put(keyLeft, firstChild);

		BTreeIndex index = new BTreeIndex(mImplementation, this, mLevel + 1);
		index.mMap = new ArrayMap(INDEX_SIZE);
		index.mMap.put(new ArrayMapEntry(keyLeft.array(), POINTER_PLACEHOLDER, (byte)0x99), null);
		index.mMap.put(new ArrayMapEntry(keyRight.array(), POINTER_PLACEHOLDER, (byte)0x22), null);
		index.mChildren.put(keyLeft, left);
		index.mChildren.put(keyRight, right);
		index.mModified = true;

		mChildren.clear();

		return index;
	}


	/**
	 * Shrinks the tree by removing this node and merging all child nodes into a single index node which is returned.
	 */
	BTreeIndex shrink()
	{
		BTreeIndex index = new BTreeIndex(mImplementation, mParent, mLevel - 1);
		index.mModified = true;
		index.mMap = new ArrayMap(INDEX_SIZE);

		for (int i = 0; i < mMap.size(); i++)
		{
			BTreeIndex node = getNode(i);

			boolean first = i > 0;
			for (ArrayMapEntry entry : node.mMap)
			{
				ArrayMapEntry newEntry;
				if (first)
				{
					newEntry = new ArrayMapEntry(mMap.getKey(i), entry.getValue(), entry.getType());
					first = false;
				}
				else
				{
					newEntry = entry;
				}

				index.mMap.insert(newEntry, null);

				BTreeNode child = node.mChildren.remove(new MarshalledKey(entry.getKey()));
				if (child != null)
				{
					index.mChildren.put(new MarshalledKey(newEntry.getKey()), child);
				}
			}

			mImplementation.freeBlock(node.mBlockPointer);
		}

		mImplementation.freeBlock(mBlockPointer);

		return index;
	}


	private void merge(int aIndex, BTreeIndex aFrom, BTreeIndex aTo)
	{
		ArrayMapEntry temp = new ArrayMapEntry();

		for (int i = 0, sz = aFrom.mMap.size(); i < sz; i++)
		{
			aFrom.mMap.get(i, temp);

			MarshalledKey key = new MarshalledKey(temp.getKey());

			if (key.size() == 0)
			{
				temp.setKey(findFirstKey(aFrom));
			}

			aTo.mMap.insert(temp, null);
			aTo.mChildren.put(new MarshalledKey(temp.getKey()), aFrom.mChildren.get(key));
		}

		aFrom.mMap.clear();
		aFrom.mChildren.clear();
		mImplementation.freeBlock(aFrom.mBlockPointer);

		mMap.remove(aIndex, null);
		aTo.mModified = true;
	}


	private void merge(int aIndex, BTreeLeaf aFrom, BTreeLeaf aTo)
	{
		ArrayMapEntry temp = new ArrayMapEntry();

		for (int i = 0, sz = aFrom.mMap.size(); i < sz; i++)
		{
			aFrom.mMap.get(i, temp);

			aTo.mMap.insert(temp, null);
		}

		aFrom.mMap.clear();
		mImplementation.freeBlock(aFrom.mBlockPointer);

		mMap.remove(aIndex, null);

		aTo.mModified = true;
	}


	private byte[] findFirstKey(BTreeNode aNode)
	{
		if (aNode instanceof BTreeIndex node)
		{
			return findFirstKey(node.getNode(0));
		}

		return aNode.mMap.getKey(0);
	}


	/**
	 * Merge entries in all child nodes into a single LeafNode which is returned.
	 */
	BTreeLeaf downgrade()
	{
		assert mLevel == 1;

		BTreeLeaf newLeaf = getNode(0);
		newLeaf.mModified = true;

		for (int i = 1; i < mMap.size(); i++)
		{
			BTreeLeaf node = getNode(i);

			node.mMap.forEach(e -> newLeaf.mMap.insert(e, null));

			mImplementation.freeBlock(node.mBlockPointer);
		}

		mImplementation.freeBlock(mBlockPointer);

		return newLeaf;
	}


	@Override
	boolean commit()
	{
		for (Entry<MarshalledKey, BTreeNode> entry : mChildren.entrySet())
		{
			if (entry.getValue().commit())
			{
				mModified = true;

				mMap.put(new ArrayMapEntry(entry.getKey().array(), entry.getValue().mBlockPointer.marshal(ByteArrayBuffer.alloc(BlockPointer.SIZE)).array(), (byte)0x99), null);
			}

			entry.getValue().mModified = false;
		}

		mChildren.clear();

//		if (!mModified)
//		{
//			return false;
//		}

		mImplementation.freeBlock(mBlockPointer);

		mBlockPointer = mImplementation.writeBlock(mMap.array(), mLevel, BlockType.INDEX);

		mModified = false;

		return true;
	}


	private <T extends BTreeNode> T getNode(int aIndex)
	{
		ArrayMapEntry entry = new ArrayMapEntry();

		mMap.get(aIndex, entry);

		MarshalledKey key = new MarshalledKey(entry.getKey());

		BTreeNode node = getNode(entry, key);

		return (T)node;
	}


	private BTreeNode getNode(ArrayMapEntry aEntry, MarshalledKey aKey)
	{
		BTreeNode node = mChildren.get(aKey);

		if (node == null)
		{
			BlockPointer bp = new BlockPointer().unmarshal(ByteArrayBuffer.wrap(aEntry.getValue()));

			node = bp.getBlockType() == BlockType.INDEX ? new BTreeIndex(mImplementation, this, mLevel - 1) : new BTreeLeaf(mImplementation, this);
			node.mBlockPointer = bp;
			node.mMap = new ArrayMap(mImplementation.readBlock(bp));

			mChildren.put(aKey, node);
		}

		return node;
	}


	@Override
	public String toString()
	{
		String s = "BTreeIndex{mLevel=" + mLevel + ", mMap=" + mMap + ", mChildren={";
		for (MarshalledKey t : mChildren.keySet())
		{
			s += "\"" + t + "\",";
		}
		return s.substring(0, s.length() - 1) + '}';
	}
}
