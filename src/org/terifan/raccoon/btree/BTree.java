package org.terifan.raccoon.btree;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import org.terifan.raccoon.CompressionParam;
import org.terifan.raccoon.PerformanceCounters;
import static org.terifan.raccoon.PerformanceCounters.PUT_VALUE;
import static org.terifan.raccoon.PerformanceCounters.SPLIT_LEAF;
import org.terifan.raccoon.TableParam;
import org.terifan.raccoon.TransactionCounter;
import org.terifan.raccoon.core.ArrayMap;
import org.terifan.raccoon.core.Node;
import org.terifan.raccoon.core.RecordEntry;
import org.terifan.raccoon.core.ScanResult;
import org.terifan.raccoon.core.TableImplementation;
import org.terifan.raccoon.io.managed.IManagedBlockDevice;
import org.terifan.raccoon.storage.BlockAccessor;
import org.terifan.raccoon.storage.BlockPointer;
import org.terifan.raccoon.util.Log;


public class BTree extends TableImplementation
{
	private BlockAccessor mBlockAccessor;
	private BlockPointer mRootBlockPointer;
	private LeafNode mRootMap;
	private int mNodeSize;
	private int mLeafSize;
	private int mPointersPerNode;
	/*private*/ int mModCount;
	private boolean mWasEmptyInstance;
	private boolean mClosed;
	private boolean mChanged;
	private boolean mCommitChangesToBlockDevice;
	private TransactionCounter mTransactionId;
	private IndexNode mRootNode;


	public BTree(IManagedBlockDevice aBlockDevice, byte[] aTableHeader, TransactionCounter aTransactionId, boolean aCommitChangesToBlockDevice, CompressionParam aCompressionParam, TableParam aTableParam) throws IOException
	{
		mTransactionId = aTransactionId;
		mCommitChangesToBlockDevice = aCommitChangesToBlockDevice;
		mBlockAccessor = new BlockAccessor(aBlockDevice, aCompressionParam, aTableParam.getBlockReadCacheSize());

//		if (aTableHeader == null)
		{
			Log.i("create hash table");
			Log.inc();

			mNodeSize = aBlockDevice.getBlockSize();
			mLeafSize = aBlockDevice.getBlockSize();
//			mNodeSize = aTableParam.getPagesPerNode() * aBlockDevice.getBlockSize();
//			mLeafSize = aTableParam.getPagesPerLeaf() * aBlockDevice.getBlockSize();

			mPointersPerNode = mNodeSize / BlockPointer.SIZE;
			mRootMap = new LeafNode(mLeafSize);
			mRootBlockPointer = writeBlock(mRootMap, mPointersPerNode);
			mWasEmptyInstance = true;
			mChanged = true;
		}
//		else
//		{
//			Log.i("open hash table");
//			Log.inc();
//
//			unmarshalHeader(aTableHeader);
//
//			mPointersPerNode = mNodeSize / BlockPointer.SIZE;
//
//			loadRoot();
//		}

		Log.dec();
	}


	@Override
	public boolean put(RecordEntry aEntry)
	{
//		checkOpen();
//
//		if (aEntry.getKey().length + aEntry.getValue().length > getEntryMaximumLength())
//		{
//			throw new IllegalArgumentException("Combined length of key and value exceed maximum length: key: " + aEntry.getKey().length + ", value: " + aEntry.getValue().length + ", maximum: " + getEntryMaximumLength());
//		}

		int modCount = ++mModCount;
		Log.i("put");
		Log.inc();

		mChanged = true;

		if (mRootMap != null)
		{
			Log.d("put root value");

			if (!mRootMap.put(aEntry))
			{
				Log.d("upgrade root leaf to node");

				mRootNode = splitLeaf(mRootBlockPointer, mRootMap, 0);

				mRootBlockPointer = writeBlock(mRootNode, mPointersPerNode);
				mRootMap = null;
			}
		}

		if (mRootMap == null)
		{
			putValue(aEntry, aEntry.getKey(), 0, mRootNode);
		}

		Log.dec();
		assert mModCount == modCount : "concurrent modification";

		return aEntry.getValue() != null;
	}


	private IndexNode splitLeaf(BlockPointer aBlockPointer, LeafNode aLeaf, int aLevel)
	{
		Log.inc();
		Log.d("split leaf");
		Log.inc();

		assert PerformanceCounters.increment(SPLIT_LEAF);

		freeBlock(aBlockPointer);

		LeafNode lowLeaf = new LeafNode(mLeafSize);
		LeafNode highLeaf = new LeafNode(mLeafSize);

		// create nodes pointing to leafs
//		BlockPointer lowIndex = writeIfNotEmpty(lowLeaf, halfRange);
//		BlockPointer highIndex = writeIfNotEmpty(highLeaf, halfRange);
		IndexNode node = new IndexNode();
//		node.setPointer(0, lowIndex);
//		node.setPointer(halfRange, highIndex);

		Log.dec();
		Log.dec();

		return node;
	}


	static ArrayMap[] splitLeafImpl(ArrayMap aMap, RecordEntry aNewEntry)
	{
		int len = 2 * aMap.array().length;

		ArrayMap low = new ArrayMap(len);
		ArrayMap high = new ArrayMap(len);

		ArrayMap map = aMap.resize(len);
		map.put(aNewEntry);

		while (!map.isEmpty())
		{
			if (low.getFreeSpace() >= high.getFreeSpace())
			{
				RecordEntry entry = map.removeFirst();
				if (!low.put(entry))
				{
					throw new IllegalStateException();
				}
			}
			else
			{
				RecordEntry entry = map.removeLast();
				if (!high.put(entry))
				{
					throw new IllegalStateException();
				}
			}
		}

		return new ArrayMap[]{low, high};
	}


	private byte[] putValue(RecordEntry aEntry, byte[] aKey, int aLevel, IndexNode aNode)
	{
		return null;
//		assert PerformanceCounters.increment(PUT_VALUE);
//		Log.d("put value");
//		Log.inc();
//
//		int index = aNode.findPointer(computeIndex(aKey, aLevel));
//		BlockPointer blockPointer = aNode.getPointer(index);
//		byte[] oldValue;
//
//		switch (blockPointer.getType())
//		{
//			case NODE_INDEX:
//				org.terifan.raccoon.hashtable.IndexNode node = readNode(blockPointer);
//				oldValue = putValue(aEntry, aKey, aLevel + 1, node);
//				freeBlock(blockPointer);
//				aNode.setPointer(index, writeBlock(node, blockPointer.getRange()));
//				break;
//			case NODE_LEAF:
//				oldValue = putValueLeaf(blockPointer, index, aEntry, aLevel, aNode, aKey);
//				break;
//			case NODE_HOLE:
//				oldValue = upgradeHoleToLeaf(aEntry, aNode, blockPointer, index);
//				break;
//			case NODE_FREE:
//			default:
//				throw new IllegalStateException("Block structure appears damaged, attempting to travese a free block");
//		}
//
//		Log.dec();
//
//		return oldValue;
	}


	@Override
	public boolean get(RecordEntry aEntry)
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	@Override
	public boolean remove(RecordEntry aEntry)
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	@Override
	public int size()
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	@Override
	public void clear()
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	@Override
	public ArrayList<RecordEntry> list()
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	@Override
	public void close()
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	@Override
	public boolean commit() throws IOException
	{
//		checkOpen();

		try
		{
			if (mChanged)
			{
				int modCount = mModCount; // no increment
				Log.i("commit hash table");
				Log.inc();

				freeBlock(mRootBlockPointer);

				if (mRootMap != null)
				{
					mRootBlockPointer = writeBlock(mRootMap, mPointersPerNode);
				}
				else
				{
					mRootBlockPointer = writeBlock(mRootNode, mPointersPerNode);
				}

				if (mCommitChangesToBlockDevice)
				{
					mBlockAccessor.getBlockDevice().commit();
				}

				mChanged = false;

				Log.i("commit finished; new root %s", mRootBlockPointer);

				Log.dec();
				assert mModCount == modCount : "concurrent modification";

				return true;
			}
			else if (mWasEmptyInstance && mCommitChangesToBlockDevice)
			{
				mBlockAccessor.getBlockDevice().commit();
			}

			return false;
		}
		finally
		{
			mWasEmptyInstance = false;
		}
	}


	@Override
	public void rollback() throws IOException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	@Override
	public int getEntryMaximumLength()
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	@Override
	public boolean isChanged()
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	@Override
	public byte[] marshalHeader()
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	@Override
	public Iterator<RecordEntry> iterator()
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	@Override
	public void scan(ScanResult aScanResult)
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	@Override
	public String integrityCheck()
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	private void freeBlock(BlockPointer aBlockPointer)
	{
		mBlockAccessor.freeBlock(aBlockPointer);
	}


	private BlockPointer writeBlock(Node aNode, int aRange)
	{
		return mBlockAccessor.writeBlock(aNode.array(), 0, aNode.array().length, mTransactionId.get(), aNode.getType(), aRange);
	}
}
