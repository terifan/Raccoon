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

		mBuffer.put(key, split.left());
		mBuffer.put(rightKey, split.right());

		boolean overflow = false;
		overflow |= mMap.insert(new ArrayMapEntry(key.array(), BTreeTableImplementation.POINTER_PLACEHOLDER, (byte)0x44), null) == InsertResult.RESIZED;
		overflow |= mMap.insert(new ArrayMapEntry(rightKey.array(), BTreeTableImplementation.POINTER_PLACEHOLDER, (byte)0x44), null) == InsertResult.RESIZED;

		return overflow ? InsertResult.RESIZED : InsertResult.PUT;
	}

static int op;

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

		System.out.println("###");

//		if (result == RemoveResult.UPDATE_LOW)
		if (index > 0)
		{
			ArrayMapEntry oldEntry = new ArrayMapEntry();
			mMap.get(index, oldEntry);
			MarshalledKey oldKey = new MarshalledKey(oldEntry.getKey());

			MarshalledKey firstKey = new MarshalledKey(findFirstKey(curntChld));
			ArrayMapEntry firstEntry = new ArrayMapEntry(firstKey.array());
			get(firstKey, firstEntry);

//			System.out.println(firstEntry);
//			System.out.println(this);
//			System.out.println(index);

			mMap.remove(index, null);
			mMap.put(firstEntry, null);
			mBuffer.put(firstKey, mBuffer.remove(oldKey));
		}
//		else
		{
//			result = null;
		}

		System.out.println(mBuffer.keySet().toString().replace(", ", "\",\"").replace("[", "{\"").replace("]", "\"}")+" != "+mMap);
//		assert mBuffer.keySet().toString().replace(", ", "\",\"").replace("[", "{\"").replace("]", "\"}").equals(mMap.toString()) : mBuffer.keySet().toString().replace(", ", "\",\"").replace("[", "{\"").replace("]", "\"}")+" != "+mMap;
		if (!mBuffer.keySet().toString().replace(", ", "\",\"").replace("[", "{\"").replace("]", "\"}").equals(mMap.toString()))
		{
			BTreeTableImplementation.STOP = true;
		}
		if (BTreeTableImplementation.STOP) return RemoveResult.NONE;

		BTreeNode leftChild = index == 0 ? null : getNode(index - 1);
		BTreeNode rghtChild = index == mMap.size() - 1 ? null : getNode(index + 1);

		boolean a = leftChild != null && (curntChld.mMap.size() == 1 || curntChld.mMap.getUsedSpace() + leftChild.mMap.getUsedSpace() < BTreeTableImplementation.LEAF_SIZE);
		boolean b = rghtChild != null && (rghtChild.mMap.size() == 1 || curntChld.mMap.getUsedSpace() + rghtChild.mMap.getUsedSpace() < BTreeTableImplementation.LEAF_SIZE);
		boolean c = leftChild == null && rghtChild != null && curntChld.mMap.size() == 1;

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

//		if (BTreeTableImplementation.TESTINDEX == 97)
//		{
//			System.out.println(mLevel + " " + "- " + a+" "+b+" "+curntChld+" "+(leftChild==null?"-":leftChild.mMap.size())+" "+(rghtChild==null?"-":rghtChild.mMap.size()));
//		}

		int z = 0;
		if (mLevel == 1)
		{
			if (a)
			{
				z=1;

				if(op==18)
				{
					System.out.println(mBuffer.keySet().toString().replace(", ", "\",\"").replace("[", "{\"").replace("]", "\"}")+" != "+mMap);
				}

				merge1(index, (BTreeLeaf)curntChld, (BTreeLeaf)leftChild);

				if(op==18)
				{
					System.out.println(mBuffer.keySet().toString().replace(", ", "\",\"").replace("[", "{\"").replace("]", "\"}")+" != "+mMap);
				}
			}
			else if (b)
			{
				z=2;
				merge1(index + 1, (BTreeLeaf)rghtChild, (BTreeLeaf)curntChld);
			}
		}
		else
		{
			if (a)
			{
				z=3;
				merge2(index, (BTreeIndex)curntChld, (BTreeIndex)leftChild);
			}
			else if (b)
			{
				z=4;
				merge2(index + 1, (BTreeIndex)rghtChild, (BTreeIndex)curntChld);
			}
			else if (c)
			{
				z=5;
				result=RemoveResult.UPDATE_LOW;
				merge3(index, (BTreeIndex)curntChld, (BTreeIndex)rghtChild);
			}
		}

System.out.println(BTreeTableImplementation.TESTINDEX+" "+op+" <"+z+"> "+mNodeId+" "+mMap+" "+mBuffer.keySet().toString().replace(", ", "\",\"").replace("[", "{\"").replace("]", "\"}")+" "+mLevel+" "+a+" "+b+" "+c);
op++;

		if (BTreeTableImplementation.TESTINDEX == 97)
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

		BTreeNode firstChild = right.mBuffer.get(keyRight);

		right.mMap.remove(firstRight.getKey(), null);
		right.mBuffer.remove(keyRight);

		firstRight.setKey(keyLeft.array());

		right.mMap.put(firstRight, null);
		right.mBuffer.put(keyLeft, firstChild);

		mBuffer.clear();

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

		ArrayMapEntry first = right.mMap.getFirst();

		MarshalledKey keyLeft = new MarshalledKey(new byte[0]);
		MarshalledKey keyRight = new MarshalledKey(first.getKey());

		BTreeNode firstChild = right.mBuffer.get(keyRight);

		right.mMap.remove(first.getKey(), null);
		right.mBuffer.remove(keyRight);

		first.setKey(keyLeft.array());

		right.mMap.put(first, null);
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


	private void merge3(int aIndex, BTreeIndex aFrom, BTreeIndex aTo)
	{
		ArrayMapEntry temp = new ArrayMapEntry();
		aTo.mMap.get(0, temp);

		MarshalledKey firstKeyInTo = new MarshalledKey(temp.getKey());

		if (firstKeyInTo.size() == 0)
		{
			temp.setKey(findFirstKey(aTo));
		}

		aTo.mMap.insert(temp, null);
		aTo.mBuffer.put(new MarshalledKey(temp.getKey()), aTo.mBuffer.get(firstKeyInTo));

		aFrom.mMap.get(0, temp);

		MarshalledKey key = new MarshalledKey(new byte[0]);

		aTo.mMap.insert(temp, null);
		aTo.mBuffer.put(new MarshalledKey(temp.getKey()), aFrom.mBuffer.get(key));

		aFrom.mMap.clear();
		aFrom.mBuffer.clear();
		mImplementation.freeBlock(aFrom.mBlockPointer);

		mMap.remove(aIndex, null);
		aTo.mModified = true;
	}


	private void merge2(int aIndex, BTreeIndex aFrom, BTreeIndex aTo)
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
			aTo.mBuffer.put(new MarshalledKey(temp.getKey()), aFrom.mBuffer.get(key));
		}

		aFrom.mMap.clear();
		aFrom.mBuffer.clear();
		mImplementation.freeBlock(aFrom.mBlockPointer);

		mMap.get(aIndex, temp);
		mMap.remove(aIndex, null);

		MarshalledKey k = new MarshalledKey(temp.getKey());
		mBuffer.remove(k);

		aTo.mModified = true;
	}


	private void merge1(int aIndex, BTreeLeaf aFrom, BTreeLeaf aTo)
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

		MarshalledKey k = new MarshalledKey(temp.getKey());
		mBuffer.remove(k);

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

		MarshalledKey key = new MarshalledKey(entry.getKey());

		BTreeNode node = getNode(entry, key);

		return (T)node;
	}


	private BTreeNode getNode(ArrayMapEntry aEntry, MarshalledKey aKey)
	{
		BTreeNode node = mBuffer.get(aKey);

		if (node == null)
		{
			BlockPointer bp = new BlockPointer().unmarshal(ByteArrayBuffer.wrap(aEntry.getValue()));

			node = bp.getBlockType() == BlockType.INDEX ? new BTreeIndex(mImplementation, this, mLevel - 1) : new BTreeLeaf(mImplementation, this);
			node.mBlockPointer = bp;
			node.mMap = new ArrayMap(mImplementation.readBlock(bp));

			mBuffer.put(aKey, node);
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
