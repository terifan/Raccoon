package org.terifan.raccoon;

import org.terifan.raccoon.document.ObjectId;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.terifan.raccoon.BTreeNode.VisitorState;
import org.terifan.raccoon.blockdevice.BlockAccessor;
import org.terifan.raccoon.blockdevice.LobByteChannel;
import org.terifan.raccoon.blockdevice.LobHeader;
import org.terifan.raccoon.blockdevice.LobOpenOption;
import org.terifan.raccoon.blockdevice.util.Log;
import org.terifan.raccoon.document.Array;
import org.terifan.raccoon.document.Document;
import org.terifan.raccoon.util.ReadWriteLock;
import org.terifan.raccoon.util.ReadWriteLock.ReadLock;
import org.terifan.raccoon.util.ReadWriteLock.WriteLock;

// db.getCollection("people").createIndex(Document.of("name:malesByName,unique:false,sparse:true,clone:true,filter:[{gender:{$eq:male}}],fields:[{firstName:1},{lastName:1}]"));

public final class RaccoonCollection
{
	public final static byte TYPE_TREENODE = 0;
	public final static byte TYPE_DOCUMENT = 1;
	public final static byte TYPE_EXTERNAL = 2;

	private final ReadWriteLock mLock;

	private Document mConfiguration;
	private RaccoonDatabase mDatabase;
	private HashSet<CommitLock> mCommitLocks;
	private BTree mImplementation;
	private long mModCount;


	RaccoonCollection(RaccoonDatabase aDatabase, Document aConfiguration)
	{
		mDatabase = aDatabase;
		mConfiguration = aConfiguration;

		mLock = new ReadWriteLock();
		mCommitLocks = new HashSet<>();
		mImplementation = new BTree(getBlockAccessor(), aConfiguration.getDocument("btree"));
	}


	public String getName()
	{
		return mConfiguration.getString("name");
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


	public <T extends Document> T get(T aDocument)
	{
		Log.i("get entity %s", aDocument);
		Log.inc();

		try (ReadLock lock = mLock.readLock())
		{
			ArrayMapEntry entry = new ArrayMapEntry(getDocumentKey(aDocument, false));

			if (mImplementation.get(entry))
			{
				return (T)unmarshalDocument(entry, aDocument);
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


	public void createIndex(Document aConfiguration, Document aFields)
	{
		Log.i("create index");
		Log.inc();

		Array id = Array.of(getCollectionId(), ObjectId.randomId());

		Document indexConf = new Document()
			.put("_id", id)
			.put("configuration", aConfiguration)
			.put("fields", aFields);

		mDatabase.getCollection("system:indices").save(indexConf);

		mDatabase.mIndices.put(id.getObjectId(0), id.getObjectId(1), indexConf);

//		try (WriteLock lock = mLock.writeLock())
//		{
//			RaccoonCollection collection = mDatabase.getCollection(INDEX_COLLECTION);
//
//			collection.find(new Document().put("collection", getCollectionId()));
//
//			Document conf = new Document().put("_id", new Document().put("collection", mConfiguration.getObjectId("object")).put("_id", ObjectId.randomId())).put("configuration", aDocument);
//			collection.save(conf);
//
//			System.out.println(conf);
//		}
//		finally
//		{
//			Log.dec();
//		}
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

		ArrayMapEntry entry = new ArrayMapEntry(key, aDocument, TYPE_DOCUMENT);

		if (entry.length() > mImplementation.getConfiguration().getInt("entrySizeLimit"))
		{
			LobHeader header = new LobHeader();

			try (LobByteChannel lob = new LobByteChannel(mDatabase.getBlockAccessor(), header, LobOpenOption.WRITE))
			{
				lob.writeAllBytes(aDocument.toByteArray());
			}
			catch (Exception | Error e)
			{
				throw new DatabaseException(e);
			}

			entry.setValue(header.marshal());
			entry.setType(TYPE_EXTERNAL);
		}

		ArrayMapEntry prev = mImplementation.put(entry);

		if (prev != null)
		{
			Document prevDoc = deleteLob(prev, true);

			if (prevDoc == null)
			{
				prevDoc = prev.getValue();
			}

			deleteIndexEntries(prevDoc);
		}

		for (Document indexConf : mDatabase.mIndices.values(getCollectionId()))
		{
			boolean unique = indexConf.getDocument("configuration").get("unique", false);
			boolean clone = indexConf.getDocument("configuration").get("clone", false);

			ArrayList<Array> result = new ArrayList<>();
			generatePermutations(indexConf, aDocument, new Array(), 0, result);
			for (Array values : result)
			{
				Document indexEntry = new Document().put("_id", values);

				if (unique)
				{
					indexEntry.put("_ref", aDocument.get("_id"));

					Document existing = getIndexByConf(indexConf).get(new Document().put("_id", values));

					if (existing != null && !aDocument.get("_id").equals(existing.get("_ref")))
					{
						throw new UniqueConstraintException("Collection <" + getName() + ">, index <" + indexConf.getObjectId("_id") + ">, existing ID <" + existing.get("_ref") + ">, saving ID <" + aDocument.get("_id") + ">, values " + values.toJson());
					}
				}
				else
				{
					values.add(aDocument.get("_id"));
				}

				if (clone)
				{
					indexEntry.put("_clone", aDocument);
				}

				getIndexByConf(indexConf).save(indexEntry);
			}
		}

		return prev == null;
	}


	private void generatePermutations(Document aIndexConf, Document aDocument, Array aIndexValues, int aPosition, ArrayList<Array> aResult)
	{
		Document confs = aIndexConf.getDocument("fields");
		if (aPosition == confs.size())
		{
			aResult.add(aIndexValues);
		}
		else
		{
			for (Object value : aDocument.findMany(confs.keySet().get(aPosition)))
			{
				Array tmp = aIndexValues.clone();
				tmp.add(value);
				generatePermutations(aIndexConf, aDocument, tmp, aPosition + 1, aResult);
			}
		}
	}


	public void deleteAll(Document... aDocuments)
	{
		try (WriteLock lock = mLock.writeLock())
		{
			mModCount++;

			for (Document document : aDocuments)
			{
				ArrayMapEntry entry = new ArrayMapEntry(getDocumentKey(document, false));
				ArrayMapEntry prev = mImplementation.remove(entry);

				if (prev != null)
				{
					Document prevDoc = deleteLob(prev, true);

					if (prevDoc == null)
					{
						prevDoc = prev.getValue();
					}

					deleteIndexEntries(prevDoc);
				}
			}
		}
	}


	public void delete(Document aDocument)
	{
		deleteAll(aDocument);
	}


	private void deleteIndexEntries(Document aPrevDoc)
	{
		for (Document indexConf : mDatabase.mIndices.values(getCollectionId()))
		{
//			ArrayList<Array> result = new ArrayList<>();
//			generatePermutations(indexConf, aPrevDoc, new Array(), 0, result);
//			for (Array values : result)
//			{
//			}

			List<Document> list = getIndexByConf(indexConf).listAll();
			for (Document doc : list)
			{
//				if (doc.get("_ref").equals(aPrevDoc.get("_id")))
				{
					getIndexByConf(indexConf).delete(doc);
					break;
				}
			}
		}
	}


	public RaccoonCollection getIndexByConf(Document aIndexConf)
	{
		return mDatabase.getCollection("index:" + aIndexConf.getArray("_id"));
	}


	public long size()
	{
		try (ReadLock lock = mLock.readLock())
		{
			return mImplementation.size();
		}
	}


	public List<Document> listAll()
	{
		return listAll(() -> new Document());
	}


	public <T extends Document> List<T> listAll(Supplier<T> aDocumentSupplier)
	{
		ArrayList<T> list = new ArrayList<>();

		try (ReadLock lock = mLock.readLock())
		{
			mModCount++;

			mImplementation.visit(new BTreeVisitor()
			{
				@Override
				VisitorState leaf(BTree aImplementation, BTreeLeafNode aNode)
				{
					aNode.mMap.forEach(e -> list.add((T)unmarshalDocument(e, aDocumentSupplier.get())));
					return VisitorState.CONTINUE;
				}
			});
		}

		return list;
	}


	public void forEach(Consumer<Document> aAction)
	{
		try (ReadLock lock = mLock.readLock())
		{
			mModCount++;

			mImplementation.visit(new BTreeVisitor()
			{
				@Override
				VisitorState leaf(BTree aImplementation, BTreeLeafNode aNode)
				{
					aNode.mMap.forEach(e -> aAction.accept(unmarshalDocument(e, new Document())));
					return VisitorState.CONTINUE;
				}
			});
		}
	}


//	public Stream<Document> stream()
//	{
//		long modCount = mModCount;
//
//		ReadLock lock = mLock.readLock();
//
//		Stream<Document> tmp = StreamSupport.stream(new AbstractSpliterator<Document>(Long.MAX_VALUE, Spliterator.IMMUTABLE | Spliterator.NONNULL)
//		{
//			private DocumentIterator iterator = new DocumentIterator(RaccoonCollection.this, new Query(new Document()));
//
//			@Override
//			public boolean tryAdvance(Consumer<? super Document> aConsumer)
//			{
//				if (mModCount != modCount)
//				{
//					lock.close();
//					throw new IllegalStateException("concurrent modification");
//				}
//
//				if (!iterator.hasNext())
//				{
//					lock.close();
//					return false;
//				}
//				aConsumer.accept(iterator.next());
//				return true;
//			}
//		}, false);
//
//		return tmp;
//	}
	public void clear()
	{
		try (WriteLock lock = mLock.writeLock())
		{
			mModCount++;

			BTree prev = mImplementation;

			mImplementation = new BTree(getBlockAccessor(), mImplementation.getConfiguration().clone().remove("root"));

			mImplementation.visit(new BTreeVisitor()
			{
				@Override
				VisitorState leaf(BTree aImplementation, BTreeLeafNode aNode)
				{
					aNode.mMap.forEach(e -> deleteLob(e, false));
					prev.freeBlock(aNode.mBlockPointer);
					return VisitorState.CONTINUE;
				}


				@Override
				VisitorState afterInteriorNode(BTree aImplementation, BTreeInteriorNode aNode)
				{
					prev.freeBlock(aNode.mBlockPointer);
					return VisitorState.CONTINUE;
				}
			});

			for (Document indexConf : mDatabase.mIndices.values(getCollectionId()))
			{
				getIndexByConf(indexConf).clear();
			}
		}
	}


	protected ObjectId getCollectionId()
	{
		return mConfiguration.getObjectId("_id");
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

			return mImplementation.commit();
		}
	}


	void rollback()
	{
		mImplementation.rollback();
	}


	BlockAccessor getBlockAccessor()
	{
		return new BlockAccessor(mDatabase.getBlockDevice(), true);
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

		return new ArrayMapKey(id);
	}


	private Document deleteLob(ArrayMapEntry aEntry, boolean aRestoreOldValue)
	{
		Document prev = null;

		if (aEntry != null && aEntry.getType() == TYPE_EXTERNAL)
		{
			LobHeader header = new LobHeader(aEntry.getValue());

			try
			{
				if (aRestoreOldValue)
				{
					try (LobByteChannel lob = new LobByteChannel(mDatabase.getBlockAccessor(), header, LobOpenOption.READ))
					{
						prev = new Document().fromByteArray(lob.readAllBytes());
					}
				}

				try (LobByteChannel lob = new LobByteChannel(mDatabase.getBlockAccessor(), header, LobOpenOption.REPLACE))
				{
				}
			}
			catch (IOException e)
			{
				throw new DatabaseException(e);
			}
		}

		return prev;
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
		if (aEntry.getType() == TYPE_EXTERNAL)
		{
			LobHeader header = new LobHeader(aEntry.getValue());

			try (LobByteChannel lob = new LobByteChannel(mDatabase.getBlockAccessor(), header, LobOpenOption.READ))
			{
				return aDestination.putAll(new Document().fromByteArray(lob.readAllBytes()));
			}
			catch (IOException e)
			{
				throw new DatabaseException(e);
			}
		}

		return aDestination.putAll(aEntry.getValue());
	}


	public List<Document> find(Document aQuery)
	{
		Log.i("find %s", aQuery);
		Log.inc();

		System.out.println(aQuery);
//		System.out.println(mDatabase.mIndices);

		Document bestIndex = null;
		int matchingFields = 0;
		ArrayList<String> queryKeys = aQuery.keySet();

		for (Document conf : mDatabase.mIndices.get(getCollectionId()).values())
		{
			ArrayList<String> indexKeys = conf.getDocument("fields").keySet();

			int i = 0;
			for (; i < Math.min(indexKeys.size(), queryKeys.size()) && indexKeys.get(i).equals(queryKeys.get(i)); i++)
			{
			}

			if (i > matchingFields || i == matchingFields && bestIndex != null && indexKeys.size() < conf.getDocument("fields").size())
			{
				bestIndex = conf;
				matchingFields = i;
			}
		}

//		System.out.println(matchingFields+" "+queryKeys.size());
//		System.out.println(bestIndex);

		if (bestIndex != null)
		{
			System.out.println("INDEX: " + bestIndex);

			return mDatabase.getCollection("index:" + bestIndex.getArray("_id")).findImpl(aQuery);
		}

		return findImpl(aQuery);
	}


	protected List<Document> findImpl(Document aQuery)
	{
		try (ReadLock lock = mLock.readLock())
		{
			ArrayList<Document> list = new ArrayList<>();

			mImplementation.visit(new BTreeVisitor()
			{
				@Override
				VisitorState beforeInteriorNode(BTree aImplementation, BTreeInteriorNode aNode, ArrayMapKey aLowestKey)
				{
					return x(aImplementation, aNode, aLowestKey, aQuery)? VisitorState.CONTINUE : VisitorState.SKIP;
				}

				@Override
				VisitorState leaf(BTree aImplementation, BTreeLeafNode aNode)
				{
//					System.out.println(aNode);
					for (int i = 0; i < aNode.mMap.size(); i++)
					{
						ArrayMapEntry entry = aNode.mMap.get(i, new ArrayMapEntry());
						Document doc = unmarshalDocument(entry, new Document());
						list.add(doc);
					}
					return VisitorState.CONTINUE;
				}
			});

			System.out.println(list.size());

			return list;
		}
		finally
		{
			Log.dec();
		}
	}


	private boolean x(BTree aImplementation, BTreeInteriorNode aNode, ArrayMapKey aLowestKey, Document aQuery)
	{
		System.out.println("*" + aLowestKey);
		System.out.println("... ".repeat(3 - aNode.mLevel) + aNode.mLevel + ", " + aNode.size());

		return true;
	}
}
