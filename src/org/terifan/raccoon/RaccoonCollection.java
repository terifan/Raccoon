package org.terifan.raccoon;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators.AbstractSpliterator;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import static org.terifan.raccoon.RaccoonDatabase.INDEX_COLLECTION;
import org.terifan.raccoon.document.Array;
import org.terifan.raccoon.document.Document;
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

	private Document mConfiguration;
	private RaccoonDatabase mDatabase;
	private HashSet<CommitLock> mCommitLocks;
//	private IdentityCounter mIdentityCounter;
	private BTree mImplementation;
	private long mModCount;


	RaccoonCollection(RaccoonDatabase aDatabase, Document aConfiguration)
	{
		mDatabase = aDatabase;
		mConfiguration = aConfiguration;

		mLock = new ReadWriteLock();
		mCommitLocks = new HashSet<>();
		mImplementation = new BTree(getBlockAccessor(), aConfiguration.getDocument("btree"));
//		mIdentityCounter = new IdentityCounter(aConfiguration);
	}


	public boolean getAll(Document... aDocuments)
	{
		Log.i("get all entities %d", aDocuments.length);
		Log.inc();

		try (ReadLock lock = mLock.readLock())
		{
			boolean all = true;

			for (Document document : aDocuments)
			{
				ArrayMapEntry entry = new ArrayMapEntry(getDocumentKey(document, false));

				if (mImplementation.get(entry))
				{
					unmarshalDocument(entry, document);
				}
				else
				{
					all = false;
				}
			}

			return all;
		}
		finally
		{
			Log.dec();
		}
	}


	public Document get(Document aDocument)
	{
		Log.i("get entity %s", aDocument);
		Log.inc();

		try (ReadLock lock = mLock.readLock())
		{
			ArrayMapEntry entry = new ArrayMapEntry(getDocumentKey(aDocument, false));

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


	public boolean save(Document aDocument)
	{
		Log.i("save %s", aDocument);
		Log.inc();

		try (WriteLock lock = mLock.writeLock())
		{
			mModCount++;
			return insertOrUpdate(aDocument, false);
		}
		finally
		{
			Log.dec();
		}
	}


	public void saveAll(Document... aDocuments)
	{
		Log.i("save all %d", aDocuments.length);
		Log.inc();

		try (WriteLock lock = mLock.writeLock())
		{
			mModCount++;
			for (Document document : aDocuments)
			{
				insertOrUpdate(document, false);
			}
		}
		finally
		{
			Log.dec();
		}
	}


	public boolean insert(Document aDocument)
	{
		Log.i("insert %s", aDocument);
		Log.inc();

		try (WriteLock lock = mLock.writeLock())
		{
			mModCount++;
			return insertOrUpdate(aDocument, true);
		}
		finally
		{
			Log.dec();
		}
	}


	public void insertAll(Document... aDocuments)
	{
		Log.i("insert all %d", aDocuments.length);
		Log.inc();

		try (WriteLock lock = mLock.writeLock())
		{
			mModCount++;
			for (Document document : aDocuments)
			{
				insertOrUpdate(document, true);
			}
		}
		finally
		{
			Log.dec();
		}
	}


	public void createIndex(Document aDocument)
	{
		Log.i("create index");
		Log.inc();

		try (WriteLock lock = mLock.writeLock())
		{
			RaccoonCollection collection = mDatabase.getCollection(INDEX_COLLECTION);

			collection.find(new Document().put("collection", mConfiguration.getObjectId("_id")));

			Document conf = new Document().put("_id", new Document().put("collection", mConfiguration.getObjectId("object")).put("_id", ObjectId.randomId())).put("configuration", aDocument);
			collection.save(conf);

			System.out.println(conf);
		}
		finally
		{
			Log.dec();
		}
	}


	private boolean insertOrUpdate(Document aDocument, boolean aInsert)
	{
		ArrayMapKey key = getDocumentKey(aDocument, true);

		if (aInsert)
		{
			ArrayMapEntry entry = new ArrayMapEntry(key);
			if (mImplementation.get(entry))
			{
				throw new DuplicateKeyException();
			}
		}

		byte[] value = aDocument.toByteArray();
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

			ArrayMapEntry entry = new ArrayMapEntry(getDocumentKey(aDocument, false));
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
			private DocumentIterator iterator = new DocumentIterator(RaccoonCollection.this, new Query());

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

			mImplementation = new BTree(getBlockAccessor(), mImplementation.getConfiguration().clone().remove("root"));

			new BTreeNodeVisitor().visitAll(prev, node ->
			{
				node.mMap.forEach(this::deleteIfBlob);
				prev.freeBlock(node.mBlockPointer);
			});
		}
	}


	void close()
	{
		mImplementation.close();
		mImplementation = null;
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

//			mImplementation.getConfiguration().put("identityCounter", mIdentityCounter.get());
			mImplementation.getConfiguration().put("identityCounter", 0);

			return mImplementation.commit();
		}
	}


	void rollback()
	{
		mImplementation.rollback();
	}


	@Override
	BlockAccessor getBlockAccessor()
	{
		return new BlockAccessor(mDatabase.getBlockDevice(), mDatabase.getCompressionParameter());
	}


	private ArrayMapKey getDocumentKey(Document aDocument, boolean aCreateMissingKey)
	{
		Object id = aDocument.get("_id");

		if (id == null)
		{
			if (!aCreateMissingKey)
			{
				throw new IllegalStateException("_id field not provided in Document");
			}

			id = ObjectId.randomId();
			aDocument.put("_id", id);
			return new ArrayMapKey(id);
		}

//		if (id instanceof Long || id instanceof Integer)
//		{
//			mIdentityCounter.set(((Number)id).longValue());
//			return new ArrayMapKey(mIdentityCounter.get());
//		}

		return new ArrayMapKey(id);
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


	public BTree _getImplementation()
	{
		return mImplementation;
	}


	Document getConfiguration()
	{
		return mConfiguration;
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

		return aDestination.putAll(new Document().fromByteArray(buffer));
	}


//	public IdentityCounter getIdentityCounter()
//	{
//		return mIdentityCounter;
//	}


	public List<Document> find(Document aQuery)
	{
		Log.i("find %s", aQuery);
		Log.inc();

//		System.out.println(aQuery);

		try (ReadLock lock = mLock.readLock())
		{
			ArrayList<Document> list = new ArrayList<>();

			DocumentIterator iterator = new DocumentIterator(this, new Query());

//			iterator.setRange(new ArrayMapKey(35), new ArrayMapKey(55));
//			iterator.setRange(new ArrayMapKey("35"), null);
//			iterator.setRange(null, new ArrayMapKey("55"));
//			iterator.setRange(null, null);

			iterator.forEachRemaining(e -> list.add(e));

			return list;
		}
		finally
		{
			Log.dec();
		}
	}
}
