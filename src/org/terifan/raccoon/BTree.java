package org.terifan.raccoon;

import java.util.concurrent.atomic.AtomicLong;
import org.terifan.bundle.Document;
import org.terifan.raccoon.BlockType;
import org.terifan.raccoon.BTreeNode.RemoveResult;
import org.terifan.raccoon.storage.BlockPointer;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Log;
import org.terifan.raccoon.util.Result;


public class BTree implements AutoCloseable
{
	static byte[] BLOCKPOINTER_PLACEHOLDER = new BlockPointer().setBlockType(BlockType.ILLEGAL).marshal(ByteArrayBuffer.alloc(BlockPointer.SIZE)).array();

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

		return mRoot.get(this, new MarshalledKey(aEntry.getKey()), aEntry);
	}


	public ArrayMapEntry put(ArrayMapEntry aEntry)
	{
		assertNotClosed();

		if (aEntry.getKey().length + aEntry.getValue().length > mConfiguration.getInt("entrySizeLimit"))
		{
			throw new IllegalArgumentException("Combined length of key and value exceed maximum length: key: " + aEntry.getKey().length + ", value: " + aEntry.getValue().length + ", maximum: " + mConfiguration.getInt("entrySizeLimit"));
		}

		int modCount = ++mModCount;
		Log.i("put");
		Log.inc();

		Result<ArrayMapEntry> result = new Result<>();

//		lock.writeLock().lock();
		synchronized (this)
		{
			if (mRoot.mLevel == 0 ? mRoot.mMap.getFreeSpace() < aEntry.getMarshalledLength() : mRoot.mMap.getUsedSpace() > mConfiguration.getInt("indexSize"))
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
		assertNotClosed();

		int modCount = ++mModCount;
		Log.i("put");
		Log.inc();

		Result<ArrayMapEntry> prev = new Result<>();

		RemoveResult result = mRoot.remove(this, new MarshalledKey(aEntry.getKey()), prev);

		if (result == RemoveResult.REMOVED)
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

		Log.dec();
		assert mModCount == modCount : "concurrent modification";

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

		assert BTreeScanner.integrityCheck(this) == null : BTreeScanner.integrityCheck(this);

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
}
