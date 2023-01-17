package org.terifan.raccoon;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators.AbstractSpliterator;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.terifan.bundle.Document;
import org.terifan.raccoon.storage.BlockAccessor;
import org.terifan.raccoon.util.Log;
import org.terifan.raccoon.util.ReadWriteLock;
import org.terifan.raccoon.util.ReadWriteLock.ReadLock;
import org.terifan.raccoon.util.ReadWriteLock.WriteLock;


public final class RaccoonCollection extends BTreeStorage
{
	public final static byte TYPE_TREENODE = 0;
	public final static byte TYPE_DOCUMENT = 1;
	public final static byte TYPE_EXTERNAL = 2;

	private final ReadWriteLock mLock;

	private RaccoonDatabase mDatabase;
	private HashSet<CommitLock> mCommitLocks;
	private IdentityCounter mIdentityCounter;
	private BTree mImplementation;
	private long mModCount;


	RaccoonCollection(RaccoonDatabase aDatabase, Document aConfiguration)
	{
		mDatabase = aDatabase;
		mCommitLocks = new HashSet<>();
		mImplementation = new BTree(this, aConfiguration);
		mIdentityCounter = new IdentityCounter(aConfiguration);
		mLock = new ReadWriteLock();
	}


	public Document get(Document aDocument)
	{
		Log.i("get entity %s", aDocument);
		Log.inc();

		try (ReadLock lock = mLock.readLock())
		{
			ArrayMapEntry entry = new ArrayMapEntry(marshalKey(aDocument, false));

			if (mImplementation.get(entry))
			{
				return unmarshalDocument(entry, aDocument);
			}

			return null;
		}
		finally
		{
			Log.dec();
		}
	}


	public boolean tryGet(Document aDocument)
	{
		return get(aDocument) != null;
	}


	public void saveAll(Document... aDocuments)
	{
		for (Document document : aDocuments)
		{
			save(document);
		}
	}


	public boolean save(Document aDocument)
	{
		Log.i("save %s", aDocument);
		Log.inc();

		try (WriteLock lock = mLock.writeLock())
		{
			mModCount++;

			ArrayMapKey key = marshalKey(aDocument, true);
			byte[] value = aDocument.marshal();
			byte type = TYPE_DOCUMENT;

			if (key.size() + value.length + 1 > mImplementation.getConfiguration().getInt("entrySizeLimit"))
			{
				type = TYPE_EXTERNAL;

				try (LobByteChannel blob = new LobByteChannel(mDatabase, null, LobOpenOption.WRITE))
				{
					value = blob.writeAllBytes(value).finish();
				}
				catch (Exception | Error e)
				{
					throw new DatabaseException(e);
				}
			}

			ArrayMapEntry entry = new ArrayMapEntry(key, value, type);
			ArrayMapEntry prev = mImplementation.put(entry);
			deleteIfBlob(prev);

			return prev == null;
		}
		finally
		{
			Log.dec();
		}
	}


	public void deleteAll(Document... aDocuments)
	{
		for (Document document : aDocuments)
		{
			delete(document);
		}
	}


	public boolean delete(Document aDocument)
	{
		try (WriteLock lock = mLock.writeLock())
		{
			mModCount++;

			ArrayMapEntry entry = new ArrayMapEntry(marshalKey(aDocument, false));
			ArrayMapEntry prev = mImplementation.remove(entry);
			deleteIfBlob(prev);

			return prev != null;
		}
	}


	public long size()
	{
		try (ReadLock lock = mLock.readLock())
		{
			return mImplementation.size();
		}
	}


	public List<Document> list()
	{
		return stream().collect(Collectors.toList());
	}


	public Stream<Document> stream()
	{
		long modCount = mModCount;

		ReadLock lock = mLock.readLock();

		Stream<Document> tmp = StreamSupport.stream(new AbstractSpliterator<Document>(Long.MAX_VALUE, Spliterator.IMMUTABLE | Spliterator.NONNULL)
		{
			private DocumentIterator iterator = new DocumentIterator(RaccoonCollection.this);

			@Override
			public boolean tryAdvance(Consumer<? super Document> aConsumer)
			{
				if (mModCount != modCount)
				{
					lock.close();
					throw new IllegalStateException("concurrent modification");
				}

				if (!iterator.hasNext())
				{
					lock.close();
					return false;
				}
				aConsumer.accept(iterator.next());
				return true;
			}
		}, false);

		return tmp;
	}


	public void clear()
	{
		try (WriteLock lock = mLock.writeLock())
		{
			mModCount++;

			BTree prev = mImplementation;

			mImplementation = new BTree(this, mImplementation.getConfiguration().clone().remove("treeRoot"));

			new BTreeNodeVisitor().visitAll(prev, node ->
			{
				node.mMap.forEach(this::deleteIfBlob);
				prev.freeBlock(node.mBlockPointer);
			});
		}
	}


	@Override
	public void close()
	{
		mImplementation.close();
	}


	boolean isModified()
	{
		return mImplementation.isChanged();
	}


	long flush()
	{
		return mImplementation.flush();
	}


	boolean commit()
	{
		try (WriteLock lock = mLock.writeLock())
		{
			mModCount++;

			if (!mCommitLocks.isEmpty())
			{
				StringBuilder sb = new StringBuilder();
				for (CommitLock cl : mCommitLocks)
				{
					sb.append("\nCaused by calling method: " + cl.getOwner());
				}

				throw new CommitBlockedException("A table cannot be committed while a stream is open." + sb.toString());
			}

			mImplementation.getConfiguration().put("identityCounter", mIdentityCounter.get());

			return mImplementation.commit();
		}
	}


	void rollback()
	{
		mImplementation.rollback();
	}


	@Override
	public BlockAccessor getBlockAccessor()
	{
		return new BlockAccessor(mDatabase.getBlockDevice(), mDatabase.getCompressionParameter());
	}


	@Override
	protected long getTransaction()
	{
		return mDatabase.getTransaction();
	}


	private ArrayMapKey marshalKey(Document aDocument, boolean aCreateKey)
	{
		Object id = aDocument.get("_id");

		if (id instanceof UUID)
		{
		}
		else if (id instanceof Number)
		{
			mIdentityCounter.set(((Number)id).longValue());
		}
		else if (id == null)
		{
			if (!aCreateKey)
			{
				throw new IllegalStateException("_id field not provided in Document");
			}
			id = mIdentityCounter.next();
			aDocument.put("_id", id);
		}

		if (id instanceof Number)
		{
			return new ArrayMapKey(((Number)id).longValue());
		}

		return new ArrayMapKey(id.toString());
	}


	private void deleteIfBlob(ArrayMapEntry aEntry)
	{
		if (aEntry != null && aEntry.getType() == TYPE_EXTERNAL)
		{
			try (LobByteChannel blob = new LobByteChannel(mDatabase, aEntry.getValue(), LobOpenOption.REPLACE))
			{
			}
			catch (IOException e)
			{
				throw new DatabaseException(e);
			}
		}
	}


	BTree getImplementation()
	{
		return mImplementation;
	}


	Document getConfiguration()
	{
		return mImplementation.getConfiguration();
	}


	Document unmarshalDocument(ArrayMapEntry aEntry, Document aDestination)
	{
		byte[] buffer;

		if (aEntry.getType() == TYPE_EXTERNAL)
		{
			try (LobByteChannel blob = new LobByteChannel(mDatabase, aEntry.getValue(), LobOpenOption.READ))
			{
				buffer = blob.readAllBytes();
			}
			catch (IOException e)
			{
				throw new DatabaseException(e);
			}
		}
		else
		{
			buffer = aEntry.getValue();
		}

		return aDestination.putAll(Document.unmarshal(buffer));
	}


	public IdentityCounter getIdentityCounter()
	{
		return mIdentityCounter;
	}
}
