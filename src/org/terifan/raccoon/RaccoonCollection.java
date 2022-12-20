package org.terifan.raccoon;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators.AbstractSpliterator;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.terifan.bundle.Document;
import org.terifan.raccoon.storage.BlockAccessor;
import org.terifan.raccoon.util.ByteArrayUtil;
import org.terifan.raccoon.util.Log;


public final class RaccoonCollection implements BTreeStorage
{
	public final static byte TYPE_TREENODE = 0;
	public final static byte TYPE_DOCUMENT = 1;
	public final static byte TYPE_EXTERNAL = 2;

	private RaccoonDatabase mDatabase;
	private HashSet<CommitLock> mCommitLocks;
	private IdentityCounter mIdentityCounter;
	private BTree mImplementation;


	RaccoonCollection(RaccoonDatabase aDatabase, Document aConfiguration)
	{
		mDatabase = aDatabase;
		mCommitLocks = new HashSet<>();
		mImplementation = new BTree(this, aConfiguration);
		mIdentityCounter = new IdentityCounter(aConfiguration);
	}


	public Document get(Document aDocument)
	{
		Log.i("get entity %s", aDocument);
		Log.inc();

		try
		{
			ArrayMapEntry entry = new ArrayMapEntry(marshalKey(aDocument, false));

			if (mImplementation.get(entry))
			{
				return unmarshalDocument(entry, aDocument);
			}
		}
		finally
		{
			Log.dec();
		}

		return null;
	}


	public boolean tryGet(Document aDocument)
	{
		return get(aDocument) != null;
	}


	public boolean save(Document aDocument)
	{
		Log.i("save %s", aDocument);
		Log.inc();

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
			catch (IOException e)
			{
				throw new DatabaseException(e);
			}
		}

		ArrayMapEntry entry = new ArrayMapEntry(key, value, type);
		ArrayMapEntry prev = mImplementation.put(entry);
		deleteIfBlob(prev);

		Log.dec();

		return prev == null;
	}


	public boolean remove(Document aDocument)
	{
		ArrayMapEntry entry = new ArrayMapEntry(marshalKey(aDocument, false));
		ArrayMapEntry prev = mImplementation.remove(entry);
		deleteIfBlob(prev);

		return prev != null;
	}


	public long size()
	{
		return mImplementation.size();
	}


	public List<Document> list()
	{
		return stream().collect(Collectors.toList());
	}


	public void clear()
	{
		BTree prev = mImplementation;

		mImplementation = new BTree(this, mImplementation.getConfiguration().clone().remove("treeRoot"));

		new BTreeNodeVisitor().visitAll(prev, node ->
		{
			node.mMap.forEach(this::deleteIfBlob);
			prev.freeBlock(node.mBlockPointer);
		});
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
		synchronized (this)
		{
			if (!mCommitLocks.isEmpty())
			{
				StringBuilder sb = new StringBuilder();
				for (CommitLock cl : mCommitLocks)
				{
					sb.append("\nCaused by calling method: " + cl.getOwner());
				}

				throw new CommitBlockedException("A table cannot be committed while a stream is open." + sb.toString());
			}
		}

		mImplementation.getConfiguration().put("identityCounter", mIdentityCounter.get());

		return mImplementation.commit();
	}


	void rollback()
	{
		mImplementation.rollback();
	}


	public Stream<Document> stream()
	{
		Stream<Document> tmp = StreamSupport.stream(new AbstractSpliterator<Document>(Long.MAX_VALUE, Spliterator.IMMUTABLE | Spliterator.NONNULL)
		{
			private DocumentIterator iterator = new DocumentIterator(RaccoonCollection.this);

			@Override
			public boolean tryAdvance(Consumer<? super Document> aConsumer)
			{
				if (!iterator.hasNext())
				{
					return false;
				}
				aConsumer.accept(iterator.next());
				return true;
			}
		}, false);

		return tmp;
	}


	@Override
	public BlockAccessor getBlockAccessor()
	{
		return new BlockAccessor(mDatabase.getBlockDevice(), mDatabase.getCompressionParameter());
	}


	@Override
	public long getTransaction()
	{
		return mDatabase.getTransaction();
	}


	private ArrayMapKey marshalKey(Document aDocument, boolean aCreateKey)
	{
		Object id = aDocument.get("_id");

		if (id == null)
		{
			if (!aCreateKey)
			{
				throw new IllegalStateException();
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


	public BTree getImplementation()
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