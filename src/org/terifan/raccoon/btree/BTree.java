package org.terifan.raccoon.btree;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import org.terifan.bundle.Document;
import org.terifan.raccoon.BlockType;
import org.terifan.raccoon.CompressionParam;
import org.terifan.raccoon.ScanResult;
import org.terifan.raccoon.TransactionGroup;
import org.terifan.raccoon.io.managed.IManagedBlockDevice;
import org.terifan.raccoon.storage.BlockAccessor;
import org.terifan.raccoon.storage.BlockPointer;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Console;
import org.terifan.raccoon.util.Log;
import org.terifan.raccoon.util.Result;


public class BTree
{
	static byte[] BLOCKPOINTER_PLACEHOLDER = new BlockPointer().setBlockType(BlockType.ILLEGAL).marshal(ByteArrayBuffer.alloc(BlockPointer.SIZE)).array();
	static int INDEX_SIZE = 1000;
	static int LEAF_SIZE = 500;

	private boolean mWasEmptyInstance;
	private boolean mClosed;
	private int mModCount;
	private BTreeNode mRoot;

	protected final boolean mCommitChangesToBlockDevice;
	protected BlockAccessor mBlockAccessor;


	public BTree(IManagedBlockDevice aBlockDevice, TransactionGroup aTransactionGroup, boolean aCommitChangesToBlockDevice)
	{
		mBlockAccessor = new BlockAccessor(aBlockDevice, CompressionParam.BEST_SPEED);
		mCommitChangesToBlockDevice = aCommitChangesToBlockDevice;
	}


	public void openOrCreateTable(String aTableName, Document aTableHeader)
	{
		if (!aTableHeader.containsKey("pointer"))
		{
			Log.i("create table %s", aTableName);
			Log.inc();

			setupEmptyTable();

			mWasEmptyInstance = true;

			Log.dec();
		}
		else
		{
			Log.i("open table %s", aTableName);
			Log.inc();

			unmarshalHeader(aTableHeader);

			Log.dec();
		}
	}


	private Document marshalHeader()
	{
		Document doc = new Document();
		doc.put("compression", mBlockAccessor.getCompressionParam());

		mRoot.mBlockPointer.setBlockType(mRoot instanceof BTreeIndex ? BlockType.INDEX : BlockType.LEAF);
		doc.putBinary("pointer", mRoot.mBlockPointer.marshal());

		return doc;
	}


	private void unmarshalHeader(Document aTableHeader)
	{
		mBlockAccessor.setCompressionParam(new CompressionParam().unmarshal(aTableHeader.getBundle("compression")));

		BlockPointer bp = new BlockPointer().unmarshal(aTableHeader.getBinary("pointer"));

		mRoot = bp.getBlockType() == BlockType.INDEX ? new BTreeIndex(bp.getBlockLevel()) : new BTreeLeaf();
		mRoot.mBlockPointer = bp;
		mRoot.mMap = new ArrayMap(readBlock(bp));
	}


	public boolean get(ArrayMapEntry aEntry)
	{
		checkOpen();

		return mRoot.get(this, new MarshalledKey(aEntry.getKey()), aEntry);
	}

	ReentrantReadWriteLock lock = new ReentrantReadWriteLock();


	public ArrayMapEntry put(ArrayMapEntry aEntry)
	{
		checkOpen();

		aEntry = new ArrayMapEntry(new MarshalledKey(aEntry.getKey()).array(), aEntry.getValue(), aEntry.getType());

		if (aEntry.getKey().length + aEntry.getValue().length > getEntrySizeLimit())
		{
			throw new IllegalArgumentException("Combined length of key and value exceed maximum length: key: " + aEntry.getKey().length + ", value: " + aEntry.getValue().length + ", maximum: " + getEntrySizeLimit());
		}

		int modCount = ++mModCount;
		Log.i("put");
		Log.inc();

		Result<ArrayMapEntry> result = new Result<>();

//		lock.writeLock().lock();
		synchronized (this)
		{
			if (mRoot.mLevel == 0 ? mRoot.mMap.getFreeSpace() < aEntry.getMarshalledLength() : mRoot.mMap.getUsedSpace() > BTree.INDEX_SIZE)
			{
				if (mRoot instanceof BTreeLeaf)
				{
					mRoot = ((BTreeLeaf)mRoot).upgrade(this);
				}
				else
				{
					mRoot = ((BTreeIndex)mRoot).grow(this);
				}
			}
		}
//		lock.writeLock().unlock();

		mRoot.put(this, new MarshalledKey(aEntry.getKey()), aEntry, result);

		Log.dec();
		assert mModCount == modCount : "concurrent modification";

		return result.get();
	}


	public ArrayMapEntry remove(ArrayMapEntry aEntry)
	{
		checkOpen();

		int modCount = ++mModCount;
		Log.i("put");
		Log.inc();

		Result<ArrayMapEntry> oldEntry = new Result<>();

		mRoot.remove(this, new MarshalledKey(aEntry.getKey()), oldEntry);

		if (mRoot.mLevel > 1 && ((BTreeIndex)mRoot).mMap.size() == 1)
		{
			mRoot = ((BTreeIndex)mRoot).shrink(this);
		}
		if (mRoot.mLevel == 1 && ((BTreeIndex)mRoot).mMap.size() == 1)
		{
			mRoot = ((BTreeIndex)mRoot).downgrade(this);
		}

		Log.dec();
		assert mModCount == modCount : "concurrent modification";

		return oldEntry.get();
	}


	public Iterator<ArrayMapEntry> iterator()
	{
		checkOpen();

		return new BTreeEntryIterator(mRoot);
	}


	public ArrayList<ArrayMapEntry> list()
	{
		checkOpen();

		ArrayList<ArrayMapEntry> list = new ArrayList<>();
		iterator().forEachRemaining(list::add);

		return list;
	}


	public boolean isChanged()
	{
		return mRoot.mModified;
	}


	public Document commit(TransactionGroup mTransactionGroup, AtomicBoolean oChanged)
	{
		checkOpen();

		mCommitHistory.clear();

		try
		{
			if (mRoot.mModified)
			{
				int modCount = mModCount; // no increment
				Log.i("commit table");
				Log.inc();

				assert integrityCheck() == null : integrityCheck();

				mRoot.commit(this, mTransactionGroup);

				if (mCommitChangesToBlockDevice)
				{
					mBlockAccessor.getBlockDevice().commit();
				}

				mRoot.postCommit();

				Log.i("table commit finished; root block is %s", mRoot.mBlockPointer);

				Log.dec();
				assert mModCount == modCount : "concurrent modification";

				if (oChanged != null)
				{
					oChanged.set(true);
				}

				return marshalHeader();
			}

			if (mWasEmptyInstance && mCommitChangesToBlockDevice)
			{
				mBlockAccessor.getBlockDevice().commit();
			}

			if (oChanged != null)
			{
				oChanged.set(false);
			}

			return marshalHeader();
		}
		finally
		{
			mWasEmptyInstance = false;
		}
	}

	private HashSet<BlockPointer> mCommitHistory = new HashSet<>();


	void hasCommitted(BTreeNode aNode)
	{
		if (!mCommitHistory.add(aNode.mBlockPointer))
		{
			System.out.println(aNode);
		}
	}


	public long flush(TransactionGroup mTransactionGroup)
	{
		return 0l;
	}


	public void rollback()
	{
		checkOpen();

		Log.i("rollback");

		if (mCommitChangesToBlockDevice)
		{
			mBlockAccessor.getBlockDevice().rollback();
		}

		if (mWasEmptyInstance)
		{
			Log.d("rollback empty");

			setupEmptyTable();
		}
		else
		{
			Log.d("rollback");

//			mDirectory = new Directory(mRootBlockPointer);
//			mNodes = new LeafNode[1 << mDirectory.getPrefixLength()];
		}
	}


	public void removeAll(Consumer<ArrayMapEntry> aConsumer)
	{
		checkOpen();

		int modCount = ++mModCount;

//		new ExtendibleHashTableNodeIterator().forEachRemaining(node->
//		{
//			new ArrayMap(node.mMap.array()).forEach(aConsumer);
//
//			freeBlock(node.mBlockPointer);
//
//			node.mBlockPointer = null;
//		});
		setupEmptyTable();

		assert mModCount == modCount : "concurrent modification";
	}


	/**
	 * Clean-up resources
	 */
	public void close()
	{
		if (!mClosed)
		{
			mClosed = true;
			mRoot = null;
			mBlockAccessor = null;
		}
	}


	public int size()
	{
		checkOpen();

		Result<Integer> result = new Result<>(0);

//		new BTreeNodeIterator(mRoot).forEachRemaining(node -> result.set(result.get() + node.mMap.size()));
		visit(mRoot, node -> result.set(result.get() + node.mMap.size()));

		return result.get();
	}


	public String integrityCheck()
	{
		Log.i("integrity check");

		return integrityCheck(mRoot);
	}


	private String integrityCheck(BTreeNode aNode)
	{
		if (aNode instanceof BTreeIndex)
		{
			BTreeIndex indexNode = (BTreeIndex)aNode;

			for (ArrayMapEntry entry : indexNode.mMap)
			{
				MarshalledKey key = new MarshalledKey(entry.getKey());

				BTreeNode node = indexNode.mChildNodes.get(key);

				if (node != null)
				{
//					String s = new String(key.marshall()).replaceAll("[^\\w]*", "").replace("'", "").replace("_", "");

					String result = integrityCheck(node);

					if (result != null)
					{
						return result;
					}
				}
			}
		}
		else
		{
			BTreeLeaf leafNode = (BTreeLeaf)aNode;

			for (ArrayMapEntry entry : leafNode.mMap)
			{
			}
		}

		return null;
	}


	public int getEntrySizeLimit()
	{
		return LEAF_SIZE / 3;
	}


	private void checkOpen()
	{
		if (mClosed)
		{
			throw new IllegalStateException("Table is closed");
		}
	}


	public void scan(ScanResult aScanResult)
	{
		aScanResult.tables++;

//		scan(mRoot, aScanResult, 0);
	}


	private void scan(BTreeNode aNode, ScanResult aScanResult, int aLevel)
	{
		System.out.print((aNode.mBlockPointer + "              ").substring(0,200));
		Console.repeat(aLevel, "... ");
		Console.println(aNode);

		if (aNode instanceof BTreeIndex)
		{
			BTreeIndex indexNode = (BTreeIndex)aNode;

			for (int i = 0, sz = indexNode.mMap.size(); i < sz; i++)
			{
				BTreeNode child = indexNode.getNode(this, i);

				ArrayMapEntry entry = new ArrayMapEntry();
				indexNode.mMap.get(i, entry);
				indexNode.mChildNodes.put(new MarshalledKey(entry.getKey()), child);

				scan(child, aScanResult, aLevel + 1);
			}
		}
		else
		{
		}
	}


	private void scanX(BTreeNode aNode, ScanResult aScanResult)
	{
		if (aNode instanceof BTreeIndex)
		{
			BTreeIndex indexNode = (BTreeIndex)aNode;

			int fillRatio = indexNode.mMap.getUsedSpace() * 100 / INDEX_SIZE;
			aScanResult.log.append("{" + (aNode.mBlockPointer == null ? "" : aNode.mBlockPointer.getBlockIndex0()) + ":" + fillRatio + "%" + "}");

			boolean first = true;
			aScanResult.log.append("'");
			for (ArrayMapEntry entry : indexNode.mMap)
			{
				if (!first)
				{
					aScanResult.log.append(":");
				}
				first = false;
				String s = new String(entry.getKey()).replaceAll("[^\\w]*", "").replace("'", "").replace("_", "");
				aScanResult.log.append(s.isEmpty() ? "*" : s);
			}
			aScanResult.log.append("'");

			if (indexNode.mMap.size() == 1)
			{
				aScanResult.log.append("#000#ff0#000");
			}
			else if (fillRatio > 100)
			{
				aScanResult.log.append(indexNode.mModified ? "#a00#a00#fff" : "#666#666#fff");
			}
			else
			{
				aScanResult.log.append(indexNode.mModified ? "#f00#f00#fff" : "#888#fff#000");
			}

			first = true;
			aScanResult.log.append("[");

			for (int i = 0, sz = indexNode.mMap.size(); i < sz; i++)
			{
				if (!first)
				{
					aScanResult.log.append(",");
				}
				first = false;

				BTreeNode child = indexNode.getNode(this, i);

				ArrayMapEntry entry = new ArrayMapEntry();
				indexNode.mMap.get(i, entry);
				indexNode.mChildNodes.put(new MarshalledKey(entry.getKey()), child);

				scanX(child, aScanResult);
			}

			aScanResult.log.append("]");
		}
		else
		{
			int fillRatio = aNode.mMap.getUsedSpace() * 100 / LEAF_SIZE;

			aScanResult.log.append("{" + (aNode.mBlockPointer == null ? "" : aNode.mBlockPointer.getBlockIndex0()) + ":" + fillRatio + "%" + "}");
			aScanResult.log.append("[");

			boolean first = true;

			for (ArrayMapEntry entry : aNode.mMap)
			{
				if (!first)
				{
					aScanResult.log.append(",");
				}
				first = false;
				aScanResult.log.append("'" + new String(entry.getKey()).replaceAll("[^\\w]*", "").replace("_", "") + "'");
			}

			aScanResult.log.append("]");

			if (fillRatio > 100)
			{
				aScanResult.log.append(aNode.mModified ? "#a00#a00#fff" : "#666#666#fff");
			}
			else
			{
				aScanResult.log.append(aNode.mModified ? "#f00#f00#fff" : "#888#fff#000");
			}
		}
	}


	protected void freeBlock(BlockPointer aBlockPointer)
	{
		if (aBlockPointer != null)
		{
			mBlockAccessor.freeBlock(aBlockPointer);
		}
	}


	protected byte[] readBlock(BlockPointer aBlockPointer)
	{
		if (aBlockPointer.getBlockType() != BlockType.INDEX && aBlockPointer.getBlockType() != BlockType.LEAF)
		{
			throw new IllegalArgumentException("Attempt to read bad block: " + aBlockPointer);
		}

		return mBlockAccessor.readBlock(aBlockPointer);
	}


	protected BlockPointer writeBlock(TransactionGroup mTransactionGroup, byte[] aContent, int aLevel, BlockType aBlockType)
	{
		return mBlockAccessor.writeBlock(aContent, 0, aContent.length, mTransactionGroup.get(), aBlockType, 0).setBlockLevel(aLevel);
	}


	private void setupEmptyTable()
	{
		mRoot = new BTreeLeaf();
		mRoot.mMap = new ArrayMap(LEAF_SIZE);
	}


	private void visit(BTreeNode aNode, Consumer<BTreeLeaf> aConsumer)
	{
		if (aNode instanceof BTreeIndex)
		{
			BTreeIndex indexNode = (BTreeIndex)aNode;

			for (int i = 0, sz = indexNode.mMap.size(); i < sz; i++)
			{
				BTreeNode node = indexNode.getNode(this, i);

				ArrayMapEntry entry = new ArrayMapEntry();
				indexNode.mMap.get(i, entry);
				indexNode.mChildNodes.put(new MarshalledKey(entry.getKey()), node);

				visit(node, aConsumer);
			}
		}
		else
		{
			BTreeLeaf leafNode = (BTreeLeaf)aNode;
			aConsumer.accept(leafNode);
		}
	}
}
