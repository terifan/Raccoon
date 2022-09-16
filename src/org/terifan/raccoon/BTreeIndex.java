package org.terifan.raccoon;

import java.util.Arrays;
import java.util.Map.Entry;
import java.util.TreeMap;
import org.terifan.raccoon.ArrayMap.InsertResult;
import static org.terifan.raccoon.BTreeTableImplementation.POINTER_PLACEHOLDER;
import org.terifan.raccoon.storage.BlockPointer;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Result;
import static org.terifan.raccoon.BTreeTableImplementation.INDEX_SIZE;
import test.CCC;


public class BTreeIndex extends BTreeNode
{
	TreeMap<MarshalledKey, BTreeNode> mBuffer;


	BTreeIndex(BTreeTableImplementation aImplementation, BTreeIndex aParent, int aLevel)
	{
		super(aImplementation, aParent, aLevel);

		mBuffer = new TreeMap<>((o1, o2) -> o1.compareTo(o2));
	}


	@Override
	boolean get(MarshalledKey aKey, ArrayMapEntry aEntry)
	{
		ArrayMapEntry entry = new ArrayMapEntry(aKey.array());
		mMap.loadNearestIndexEntry(entry);

		BTreeNode node = getNode(entry);

		return node.get(aKey, aEntry);
	}


	@Override
	InsertResult put(MarshalledKey aKey, ArrayMapEntry aEntry, Result<ArrayMapEntry> aResult)
	{
		mModified = true;

		ArrayMapEntry nearestEntry = new ArrayMapEntry(aKey.array());
		mMap.loadNearestIndexEntry(nearestEntry);

		MarshalledKey nearestKey = new MarshalledKey(nearestEntry.getKey());

		BTreeNode node = getNode(nearestEntry);

		if (node.put(aKey, aEntry, aResult) == InsertResult.PUT)
		{
			return InsertResult.PUT;
		}

		assert Arrays.equals(nearestKey.array(), nearestEntry.getKey());

		mMap.remove(nearestEntry.getKey(), null);

		SplitResult split = node.split();

		MarshalledKey rightKey = split.rightKey();

		mBuffer.put(nearestKey, split.left());
		mBuffer.put(rightKey, split.right());

		boolean overflow = false;
		overflow |= mMap.insert(new ArrayMapEntry(nearestKey.array(), BTreeTableImplementation.POINTER_PLACEHOLDER, (byte)0x44), null) == InsertResult.RESIZED;
		overflow |= mMap.insert(new ArrayMapEntry(rightKey.array(), BTreeTableImplementation.POINTER_PLACEHOLDER, (byte)0x44), null) == InsertResult.RESIZED;

		return overflow ? InsertResult.RESIZED : InsertResult.PUT;
	}

public static int op;

	@Override
	RemoveResult remove(MarshalledKey aKey, Result<ArrayMapEntry> aOldEntry)
	{
		if (BTreeTableImplementation.STOP) return RemoveResult.NONE;

		mModified = true;

		int index = mMap.nearestIndex(aKey.array());

		BTreeNode curntChld = getNode(index);

		RemoveResult result = curntChld.remove(aKey, aOldEntry);

		if (result == RemoveResult.NONE)
		{
			return result;
		}

		ArrayMapEntry oldEntry = new ArrayMapEntry();
		mMap.get(index, oldEntry);

		MarshalledKey oldKey = new MarshalledKey(oldEntry.getKey());

		if (curntChld.mMap.size() == 0)
		{
			mMap.remove(index, null);
			mBuffer.remove(oldKey);

			if (index == 0)
			{
				clearFirstKey(this);
			}

			return RemoveResult.OK;
		}
//		else if (index > 0)
//		{
//			byte[] firstKeyBytes = findFirstKey(curntChld);
//
//			MarshalledKey firstKey = new MarshalledKey(firstKeyBytes);
//
//			ArrayMapEntry firstEntry = new ArrayMapEntry(firstKeyBytes);
//			get(firstKey, firstEntry);
//
//			oldEntry.setKey(firstEntry.getKey());
//
//			mMap.remove(index, null);
//			BTreeNode oldNode = mBuffer.remove(oldKey);
//
//			mMap.put(oldEntry, null);
//			mBuffer.put(firstKey, oldNode);
//
//			System.out.println("#" + index + " " + firstKey);
//		}

		if (!mBuffer.keySet().toString().replace(", ", "\",\"").replace("[", "{\"").replace("]", "\"}").equals(mMap.toString()))
		{
			System.out.println(mBuffer.keySet().toString().replace(", ", "\",\"").replace("[", "{\"").replace("]", "\"}")+" != "+mMap);
			BTreeTableImplementation.STOP = true;
		}
		if (BTreeTableImplementation.STOP) return RemoveResult.NONE;

		BTreeNode leftChild = index == 0 ? null : getNode(index - 1);
		BTreeNode rghtChild = index == mMap.size() - 1 ? null : getNode(index + 1);

		int keyLimit = mLevel == 1 ? 0 : 1;
		int sizeLimit = mLevel == 1 ? BTreeTableImplementation.LEAF_SIZE : BTreeTableImplementation.INDEX_SIZE;

		boolean a = leftChild != null;
		if (a)
		{
			a &= leftChild.mMap.size() <= keyLimit || curntChld.mMap.size() <= keyLimit || curntChld.mMap.getUsedSpace() + leftChild.mMap.getUsedSpace() <= sizeLimit;
		}

		boolean b = rghtChild != null;
		if (b)
		{
			b &= rghtChild.mMap.size() <= keyLimit || curntChld.mMap.size() <= keyLimit || curntChld.mMap.getUsedSpace() + rghtChild.mMap.getUsedSpace() <= sizeLimit;
		}

		if (a && b)
		{
			if (leftChild.mMap.getFreeSpace() < rghtChild.mMap.getFreeSpace())
			{
				a = false;
			}
			else
			{
				b = false;
			}
		}

		int z = 0;
		if (mLevel == 1)
		{
			if (a)
			{
				z=1;
				mergeLeafs(index - 1, (BTreeLeaf)leftChild, (BTreeLeaf)curntChld);
				index--;
			}
			else if (b)
			{
				z=2;
				mergeLeafs(index + 1, (BTreeLeaf)rghtChild, (BTreeLeaf)curntChld);
			}

			if (index == 0)
			{
				clearFirstKey(this);
			}
		}
		else
		{
			if (a)
			{
				z=3;
				mergeIndices(index - 1, (BTreeIndex)leftChild, index, (BTreeIndex)curntChld);
				index--;
			}
			else if (b)
			{
				z=4;
				mergeIndices(index + 1, (BTreeIndex)rghtChild, index, (BTreeIndex)curntChld);
			}
		}

		if (index > 0)
		{
			byte[] firstKeyBytes = findFirstKey(curntChld);

			MarshalledKey firstKey = new MarshalledKey(firstKeyBytes);

			ArrayMapEntry firstEntry = new ArrayMapEntry(firstKeyBytes);
			get(firstKey, firstEntry);

			oldEntry.setKey(firstEntry.getKey());

			mMap.remove(index, null);
			BTreeNode oldNode = mBuffer.remove(oldKey);

			mMap.put(oldEntry, null);
			mBuffer.put(firstKey, oldNode);
		}

		if (mLevel > 1 && curntChld.mMap.getUsedSpace() > sizeLimit)
		{
			MarshalledKey leftKey = new MarshalledKey(findFirstKey(curntChld));

			SplitResult split = curntChld.split();

			MarshalledKey rightKey = split.rightKey();

			mMap.remove(index, null);

			mBuffer.put(leftKey, split.left());
			mBuffer.put(rightKey, split.right());

			mMap.insert(new ArrayMapEntry(leftKey.array(), BTreeTableImplementation.POINTER_PLACEHOLDER, (byte)0x44), null);
			mMap.insert(new ArrayMapEntry(rightKey.array(), BTreeTableImplementation.POINTER_PLACEHOLDER, (byte)0x44), null);

			clearFirstKey(this);
		}

//System.out.println(BTreeTableImplementation.TESTINDEX+" "+op+" <"+z+"> "+mNodeId+" "+mMap+" "+mBuffer.keySet().toString().replace(", ", "\",\"").replace("[", "{\"").replace("]", "\"}")+" "+mLevel+" "+a+" "+b+" "+result);
op++;

		if (BTreeTableImplementation.TESTINDEX == 154)
//		if (op > 201)
		{
//			System.out.println(mLevel + " " + z+" "+a+" "+b+" "+curntChld+" "+leftChild+" "+rghtChild);
//			BTreeTableImplementation.STOP = true;
		}

		ArrayMapEntry temp = new ArrayMapEntry();
		mMap.get(0, temp);
		if (temp.getKey().length != 0)
		{
			throw new IllegalStateException(mMap.toString());
		}

		return result;
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

		for (Entry<MarshalledKey, BTreeNode> entry : mBuffer.entrySet())
		{
			if (entry.getKey().compareTo(midKey) < 0)
			{
				left.mBuffer.put(entry.getKey(), entry.getValue());
			}
			else
			{
				right.mBuffer.put(entry.getKey(), entry.getValue());
			}
		}

		ArrayMapEntry firstRight = right.mMap.getFirst();

		MarshalledKey keyLeft = new MarshalledKey(new byte[0]);
		MarshalledKey keyRight = new MarshalledKey(firstRight.getKey());

		BTreeNode firstChild = right.getNode(firstRight);

		right.mMap.remove(firstRight.getKey(), null);
		right.mBuffer.remove(keyRight);

		firstRight.setKey(keyLeft.array());

		right.mMap.put(firstRight, null);
		right.mBuffer.put(keyLeft, firstChild);

		mBuffer.clear();

		return new SplitResult(left, right, keyLeft, keyRight);
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

		for (Entry<MarshalledKey, BTreeNode> entry : mBuffer.entrySet())
		{
			if (entry.getKey().compareTo(midKey) < 0)
			{
				left.mBuffer.put(entry.getKey(), entry.getValue());
			}
			else
			{
				right.mBuffer.put(entry.getKey(), entry.getValue());
			}
		}

		ArrayMapEntry firstRight = right.mMap.getFirst();

		MarshalledKey keyLeft = new MarshalledKey(new byte[0]);
		MarshalledKey keyRight = new MarshalledKey(firstRight.getKey());

		BTreeNode firstChild = right.getNode(firstRight);

		right.mMap.remove(firstRight.getKey(), null);
		right.mBuffer.remove(keyRight);

		firstRight.setKey(keyLeft.array());

		right.mMap.put(firstRight, null);
		right.mBuffer.put(keyLeft, firstChild);

		BTreeIndex index = new BTreeIndex(mImplementation, this, mLevel + 1);
		index.mMap = new ArrayMap(INDEX_SIZE);
		index.mMap.put(new ArrayMapEntry(keyLeft.array(), POINTER_PLACEHOLDER, (byte)0x99), null);
		index.mMap.put(new ArrayMapEntry(keyRight.array(), POINTER_PLACEHOLDER, (byte)0x22), null);
		index.mBuffer.put(keyLeft, left);
		index.mBuffer.put(keyRight, right);
		index.mModified = true;

		mBuffer.clear();

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

				BTreeNode child = node.mBuffer.remove(new MarshalledKey(entry.getKey()));
				if (child != null)
				{
					index.mBuffer.put(new MarshalledKey(newEntry.getKey()), child);
				}
			}

			mImplementation.freeBlock(node.mBlockPointer);
		}

		mImplementation.freeBlock(mBlockPointer);

		return index;
	}


	private void mergeIndices(int aFromIndex, BTreeIndex aFrom, int aToIndex, BTreeIndex aTo)
	{
		ArrayMapEntry temp = new ArrayMapEntry();


		fixFirstKey(aTo);
		fixFirstKey(aFrom);

//		System.out.println("--v--");
//		System.out.println(aFrom.mMap + " // " + aTo.mMap);


		for (int i = 0, sz = aFrom.mMap.size(); i < sz; i++)
		{
			aFrom.mMap.get(i, temp);

//			MarshalledKey key = new MarshalledKey(temp.getKey());

			BTreeNode node = aFrom.getNode(temp);

//			if (key.size() == 0)
//			{
//				temp.setKey(findFirstKey(aFrom));
//			}

			aTo.mMap.insert(temp, null);
			aTo.mBuffer.put(new MarshalledKey(temp.getKey()), node);
		}

		aFrom.mMap.clear();
		aFrom.mBuffer.clear();
		mImplementation.freeBlock(aFrom.mBlockPointer);


		clearFirstKey(aTo);

//		System.out.println(aTo.mMap);
//		System.out.println(mMap);
//		System.out.println("-----");


		mMap.get(aFromIndex, temp);
		mMap.remove(aFromIndex, null);
		mBuffer.remove(new MarshalledKey(temp.getKey()));

		if (aFromIndex == 0)
		{
			clearFirstKey(this);
		}

		aTo.mModified = true;
	}


	private void fixFirstKey(BTreeIndex aNode)
	{
		ArrayMapEntry firstEntry = new ArrayMapEntry();
		aNode.mMap.get(0, firstEntry);

		assert firstEntry.getKey().length == 0;

		BTreeNode firstNode = aNode.getNode(firstEntry);

		firstEntry.setKey(findFirstKey(aNode));

		aNode.mMap.remove(new byte[0], null);
		aNode.mBuffer.remove(new MarshalledKey(new byte[0]));

		aNode.mMap.insert(firstEntry, null);
		aNode.mBuffer.put(new MarshalledKey(firstEntry.getKey()), firstNode);
	}


	private void clearFirstKey(BTreeIndex aNode)
	{
		ArrayMapEntry firstEntry = aNode.mMap.removeFirst();
		BTreeNode firstNode = aNode.mBuffer.remove(new MarshalledKey(firstEntry.getKey()));

		firstEntry.setKey(new byte[0]);

		aNode.mMap.insert(firstEntry, null);
		aNode.mBuffer.put(new MarshalledKey(new byte[0]), firstNode);
	}


	private void mergeLeafs(int aIndex, BTreeLeaf aFrom, BTreeLeaf aTo)
	{
		ArrayMapEntry temp = new ArrayMapEntry();

		for (int i = 0, sz = aFrom.mMap.size(); i < sz; i++)
		{
			aFrom.mMap.get(i, temp);

			aTo.mMap.insert(temp, null);
		}

		aFrom.mMap.clear();
		mImplementation.freeBlock(aFrom.mBlockPointer);

		mMap.get(aIndex, temp);
		mMap.remove(aIndex, null);

		mBuffer.remove(new MarshalledKey(temp.getKey()));

		aTo.mModified = true;
	}


	private byte[] findFirstKey(BTreeNode aNode)
	{
		if (aNode instanceof BTreeIndex node)
		{
			return findFirstKey(node.getNode(0));
		}

//		assert aNode.mMap.size() > 0;
		if (aNode.mMap.size() == 0) return null;

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
		for (Entry<MarshalledKey, BTreeNode> entry : mBuffer.entrySet())
		{
			if (entry.getValue().commit())
			{
				mModified = true;

				mMap.put(new ArrayMapEntry(entry.getKey().array(), entry.getValue().mBlockPointer.marshal(ByteArrayBuffer.alloc(BlockPointer.SIZE)).array(), (byte)0x99), null);
			}

			entry.getValue().mModified = false;
		}

		mBuffer.clear();

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

		BTreeNode node = getNode(entry);

		return (T)node;
	}


	private BTreeNode getNode(ArrayMapEntry aEntry)
	{
		MarshalledKey key = new MarshalledKey(aEntry.getKey());

		BTreeNode node = mBuffer.get(key);

		if (node == null)
		{
			BlockPointer bp = new BlockPointer().unmarshal(ByteArrayBuffer.wrap(aEntry.getValue()));

			node = bp.getBlockType() == BlockType.INDEX ? new BTreeIndex(mImplementation, this, mLevel - 1) : new BTreeLeaf(mImplementation, this);
			node.mBlockPointer = bp;
			node.mMap = new ArrayMap(mImplementation.readBlock(bp));

			mBuffer.put(key, node);
		}

		return node;
	}


	@Override
	public String toString()
	{
		String s = "BTreeIndex{mLevel=" + mLevel + ", mMap=" + mMap + ", mBuffer={";
		for (MarshalledKey t : mBuffer.keySet())
		{
			s += "\"" + t + "\",";
		}
		return s.substring(0, s.length() - 1) + '}';
	}
}
