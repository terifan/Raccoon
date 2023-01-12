package org.terifan.raccoon;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.terifan.bundle.Document;
import org.terifan.raccoon.BTreeNode.RemoveResult;
import org.terifan.raccoon.storage.BlockPointer;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Log;
import org.terifan.raccoon.util.ReadWriteLock;
import org.terifan.raccoon.util.ReadWriteLock.WriteLock;
import org.terifan.raccoon.util.Result;


public class BTree implements AutoCloseable
{
	static byte[] BLOCKPOINTER_PLACEHOLDER = new BlockPointer().setBlockType(BlockType.ILLEGAL).marshal(ByteArrayBuffer.alloc(BlockPointer.SIZE)).array();

	final ReadWriteLock mReadWriteLock = new ReadWriteLock();

	private BTreeStorage mStorage;
	private Document mConfiguration;
	private BTreeNode mRoot;
	private int mModCount;

	public BTree(BTreeStorage aStorage, Document aConfiguration)
	{
		mStorage = aStorage;
		mConfiguration = aConfiguration;

		mConfiguration.putNumber("leafSize", mConfiguration.getInt("leafSize", k -> mStorage.getBlockDevice().getBlockSize()));
		mConfiguration.putNumber("indexSize", mConfiguration.getInt("indexSize", k -> mStorage.getBlockDevice().getBlockSize()));
		mConfiguration.putNumber("entrySizeLimit", mConfiguration.getInt("entrySizeLimit", k -> mStorage.getBlockDevice().getBlockSize() / 4));

		if (mConfiguration.containsKey("treeRoot"))
		{
			Log.i("open table %s", aConfiguration.getString("name","?"));
			Log.inc();

			unmarshalHeader();

			Log.dec();
		}
		else
		{
			Log.i("create table %s", aConfiguration.getString("name","?"));
			Log.inc();

			setupEmptyTable();

			Log.dec();
		}
	}


	private void marshalHeader()
	{
		mRoot.mBlockPointer.setBlockType(mRoot instanceof BTreeIndex ? BlockType.TREE_INDEX : BlockType.TREE_LEAF);

		mConfiguration.putBundle("treeRoot", mRoot.mBlockPointer.marshalDoc());
	}


	private void unmarshalHeader()
	{
		BlockPointer bp = new BlockPointer().unmarshalDoc(mConfiguration.getBundle("treeRoot"));

		mRoot = bp.getBlockType() == BlockType.TREE_INDEX ? new BTreeIndex(bp.getBlockLevel()) : new BTreeLeaf();
		mRoot.mBlockPointer = bp;
		mRoot.mMap = new ArrayMap(readBlock(bp));
	}


	private void setupEmptyTable()
	{
		mRoot = new BTreeLeaf();
		mRoot.mMap = new ArrayMap(mConfiguration.getInt("leafSize"));
	}


	BTreeNode getRoot()
	{
		return mRoot;
	}


	public boolean get(ArrayMapEntry aEntry)
	{
		assertNotClosed();

		return mRoot.get(this, aEntry.getKey(), aEntry);
	}


	public ArrayMapEntry put(ArrayMapEntry aEntry)
	{
		assertNotClosed();

		if (aEntry.getKey().size() + aEntry.getValue().length > mConfiguration.getInt("entrySizeLimit"))
		{
			throw new IllegalArgumentException("Combined length of key and value exceed maximum length: key: " + aEntry.getKey().size() + ", value: " + aEntry.getValue().length + ", maximum: " + mConfiguration.getInt("entrySizeLimit"));
		}

		Log.i("put");
		Log.inc();

		Result<ArrayMapEntry> result = new Result<>();

		try (WriteLock lock = mReadWriteLock.writeLock())
		{
			if (mRoot.mLevel == 0 ? mRoot.mMap.getCapacity() > mConfiguration.getInt("leafSize") || mRoot.mMap.getFreeSpace() < aEntry.getMarshalledLength() : mRoot.mMap.getUsedSpace() > mConfiguration.getInt("indexSize"))
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

		mRoot.put(this, aEntry.getKey(), aEntry, result);

		Log.dec();

		return result.get();
	}


	public ArrayMapEntry remove(ArrayMapEntry aEntry)
	{
		assertNotClosed();

		Log.i("put");
		Log.inc();

		Result<ArrayMapEntry> prev = new Result<>();

		RemoveResult result = mRoot.remove(this, aEntry.getKey(), prev);

		if (result == RemoveResult.REMOVED)
		{
			try (WriteLock lock = mReadWriteLock.writeLock())
			{
				if (mRoot.mLevel > 1 && ((BTreeIndex)mRoot).mMap.size() == 1)
				{
					mRoot = ((BTreeIndex)mRoot).shrink(this);
				}
				if (mRoot.mLevel == 1 && ((BTreeIndex)mRoot).mMap.size() == 1)
				{
					mRoot = ((BTreeIndex)mRoot).downgrade(this);
				}
			}
		}

		Log.dec();

		return prev.get();
	}


	public boolean isChanged()
	{
		return mRoot.mModified;
	}


	public long flush()
	{
		return 0L;
	}


	public boolean commit()
	{
		assertNotClosed();

		if (!mRoot.mModified)
		{
			marshalHeader();
			return false;
		}

		int modCount = mModCount; // no increment
		Log.i("commit table");
		Log.inc();

		assert integrityCheck() == null : integrityCheck();

		mRoot.commit(this);
		mRoot.postCommit();

		Log.i("table commit finished; root block is %s", mRoot.mBlockPointer);

		Log.dec();
		assert mModCount == modCount : "concurrent modification";

		marshalHeader();

		return true;
	}


	public void rollback()
	{
		assertNotClosed();

		Log.i("rollback");

		if (mConfiguration.containsKey("treeRoot"))
		{
			Log.d("rollback");

			unmarshalHeader();
		}
		else
		{
			Log.d("rollback empty");

			setupEmptyTable();
		}
	}


	/**
	 * Clean-up resources
	 */
	@Override
	public void close()
	{
		if (mConfiguration != null)
		{
			mRoot = null;
			mStorage = null;
			mConfiguration = null;
		}
	}


	public long size()
	{
		assertNotClosed();

		AtomicLong result = new AtomicLong();

		new BTreeNodeVisitor().visitLeafs(this, aNode -> result.addAndGet(aNode.mMap.size()));

		return result.get();
	}


	protected byte[] readBlock(BlockPointer aBlockPointer)
	{
		if (aBlockPointer.getBlockType() != BlockType.TREE_INDEX && aBlockPointer.getBlockType() != BlockType.TREE_LEAF)
		{
			throw new IllegalArgumentException("Attempt to read bad block: " + aBlockPointer);
		}

		return mStorage.getBlockAccessor().readBlock(aBlockPointer);
	}


	protected BlockPointer writeBlock(byte[] aContent, int aLevel, BlockType aBlockType)
	{
		return mStorage.getBlockAccessor().writeBlock(aContent, 0, aContent.length, mStorage.getTransaction(), aBlockType).setBlockLevel(aLevel);
	}


	protected void freeBlock(BlockPointer aBlockPointer)
	{
		if (aBlockPointer != null)
		{
			mStorage.getBlockAccessor().freeBlock(aBlockPointer);
		}
	}


	private void assertNotClosed()
	{
		if (mRoot == null)
		{
			throw new IllegalStateException("BTree is closed");
		}
	}


	public Document getConfiguration()
	{
		return mConfiguration;
	}


	public String integrityCheck()
	{
		Log.i("integrity check");

		AtomicReference<String> result = new AtomicReference<>();

		new BTreeNodeVisitor().visitAll(this, aNode ->
		{
			String tmp = aNode.mMap.integrityCheck();
			if (tmp != null)
			{
				result.set(tmp);
				throw new BTreeNodeVisitor.CancelVisitor(null);
			}
		});

		return result.get();
	}


	public ScanResult scan(ScanResult aScanResult)
	{
		aScanResult.tables++;

		scan(mRoot, aScanResult, 0);

		return aScanResult;
	}


	private void scan(BTreeNode aNode, ScanResult aScanResult, int aLevel)
	{
		if (aNode instanceof BTreeIndex)
		{
			BTreeIndex indexNode = (BTreeIndex)aNode;

			int fillRatio = indexNode.mMap.getUsedSpace() * 100 / mConfiguration.getInt("indexSize");
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
				String s = entry.getKey().toString().replaceAll("[^\\w]*", "").replace("'", "").replace("_", "");
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
				indexNode.mChildNodes.put(entry.getKey(), child);

				scan(child, aScanResult, aLevel + 1);
			}

			aScanResult.log.append("]");
		}
		else
		{
			int fillRatio = aNode.mMap.getUsedSpace() * 100 / mConfiguration.getInt("leafSize");

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
				aScanResult.log.append("'" + entry.getKey().toString().replaceAll("[^\\w]*", "").replace("_", "") + "'");
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
}
