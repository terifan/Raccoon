package org.terifan.raccoon;

import org.terifan.raccoon.document.ObjectId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.terifan.logging.Logger;
import org.terifan.raccoon.RuntimeDiagnostics.Operation;
import org.terifan.raccoon.blockdevice.BlockAccessor;
import org.terifan.raccoon.blockdevice.BlockPointer;
import org.terifan.raccoon.blockdevice.BlockType;
import org.terifan.raccoon.blockdevice.compressor.CompressorAlgorithm;
import org.terifan.raccoon.document.Array;
import org.terifan.raccoon.document.Document;
import org.terifan.raccoon.util.ReadWriteLock;
import org.terifan.raccoon.util.ReadWriteLock.ReadLock;
import org.terifan.raccoon.util.ReadWriteLock.WriteLock;

// db.getCollection("people").createIndex(Document.of("name:malesByName,unique:false,sparse:true,clone:true,filter:[{gender:{$eq:male}}],fields:[{firstName:1},{lastName:1}]"));

public final class RaccoonCollection
{
	private final static Logger log = Logger.getLogger();

	public final static byte TYPE_TREENODE = 0;
	public final static byte TYPE_DOCUMENT = 1;
	public final static byte TYPE_EXTERNAL = 2;

	public static final String CONFIGURATION = "collection";

	private final ReadWriteLock mLock;

	private Document mConfiguration;
	private RaccoonDatabase mDatabase;
	private HashSet<CommitLock> mCommitLocks;
	private BTree mTree;
	private long mModCount;


	RaccoonCollection(RaccoonDatabase aDatabase, Document aConfiguration)
	{
		mDatabase = aDatabase;
		mConfiguration = aConfiguration;

		mLock = new ReadWriteLock();
		mCommitLocks = new HashSet<>();
		mTree = new BTree(getBlockAccessor(), aConfiguration.getDocument(CONFIGURATION));
	}


	public boolean getAll(Document... aDocuments)
	{
		log.i("get all entities {}", aDocuments.length);
		log.inc();

		try (ReadLock lock = mLock.readLock())
		{
			boolean all = true;

			for (Document document : aDocuments)
			{
				ArrayMapEntry entry = new ArrayMapEntry(getDocumentKey(document, false));

				if (mTree.get(entry))
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
			log.dec();
		}
	}


	public <T extends Document> T get(T aDocument)
	{
		log.i("get entity {}", aDocument);
		log.inc();

		try (ReadLock lock = mLock.readLock())
		{
			ArrayMapEntry entry = new ArrayMapEntry(getDocumentKey(aDocument, false));

			if (mTree.get(entry))
			{
				return (T)unmarshalDocument(entry, aDocument);
			}

			return null;
		}
		finally
		{
			log.dec();
		}
	}


	public boolean tryGet(Document aDocument)
	{
		return get(aDocument) != null;
	}


	public boolean save(Document aDocument)
	{
		log.i("save {}", aDocument);
		log.inc();

		try (WriteLock lock = mLock.writeLock())
		{
			mModCount++;
			return insertOrUpdate(aDocument, false);
		}
		finally
		{
			log.dec();
		}
	}


	public void saveAll(Document... aDocuments)
	{
		log.i("save all {}", aDocuments.length);
		log.inc();

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
			log.dec();
		}
	}


	public boolean insert(Document aDocument)
	{
		log.i("insert {}", aDocument);
		log.inc();

		try (WriteLock lock = mLock.writeLock())
		{
			mModCount++;
			return insertOrUpdate(aDocument, true);
		}
		finally
		{
			log.dec();
		}
	}


	public void insertAll(Document... aDocuments)
	{
		log.i("insert all {}", aDocuments.length);
		log.inc();

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
			log.dec();
		}
	}


	public void createIndex(Document aConfiguration, Document aFields)
	{
		log.i("create index");
		log.inc();

		Array id = Array.of(getCollectionId(), ObjectId.randomId());

		Document indexConf = new Document()
			.put("_id", id)
			.put("configuration", aConfiguration)
			.put("fields", aFields);

		mDatabase.getCollection("$indices").save(indexConf);

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
			if (mTree.get(entry))
			{
				throw new DuplicateKeyException();
			}
		}

		ArrayMapEntry entry = new ArrayMapEntry(key, aDocument, TYPE_DOCUMENT);

		if (entry.length() > mTree.getEntrySizeLimit())
		{
			RuntimeDiagnostics.collectStatistics(Operation.WRITE_EXT, 1);

			byte[] tmp = aDocument.toByteArray();
			BlockPointer bp = mDatabase.getBlockAccessor().writeBlock(tmp, 0, tmp.length, BlockType.EXTERNAL, 0, CompressorAlgorithm.LZJB.ordinal());
			entry.setValue(bp.marshalDocument());

//			Document header = new Document();
//
//			try (LobByteChannel lob = new LobByteChannel(mDatabase.getBlockAccessor(), header, LobOpenOption.WRITE, null))
//			{
//				lob.writeAllBytes(aDocument.toByteArray());
//			}
//			catch (Exception | Error e)
//			{
//				throw new DatabaseException(e);
//			}
//
//			entry.setValue(header);
			entry.setType(TYPE_EXTERNAL);
		}

		ArrayMapEntry prev = mTree.put(entry);

		if (prev != null)
		{
			Document prevDoc = deleteExternal(prev, true);

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
						throw new UniqueConstraintException("Collection index <" + indexConf.getObjectId("_id") + ">, existing ID <" + existing.get("_ref") + ">, saving ID <" + aDocument.get("_id") + ">, values " + values.toJson());
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
				ArrayMapEntry prev = mTree.remove(new ArrayMapEntry(getDocumentKey(document, false)));

				if (prev == null)
				{
					throw new EntryNotFoundException(document.get("_id"));
				}

				Document prevDoc = deleteExternal(prev, true);

				if (prevDoc == null)
				{
					prevDoc = prev.getValue();
				}

				deleteIndexEntries(prevDoc);
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
			return mTree.size();
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

			mTree.visit(new BTreeVisitor()
			{
				@Override
				boolean leaf(BTreeLeafNode aNode)
				{
					aNode.mMap.forEach(e -> list.add((T)unmarshalDocument(e, aDocumentSupplier.get())));
					return true;
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

			mTree.visit(new BTreeVisitor()
			{
				@Override
				boolean leaf(BTreeLeafNode aNode)
				{
					aNode.mMap.forEach(e -> aAction.accept(unmarshalDocument(e, new Document())));
					return true;
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

			BTree prev = mTree;

			mTree = new BTree(getBlockAccessor(), mTree.getConfiguration().clone().remove("root"));

			mTree.visit(new BTreeVisitor()
			{
				@Override
				boolean leaf(BTreeLeafNode aNode)
				{
					aNode.mMap.forEach(e -> deleteExternal(e, false));
					prev.freeBlock(aNode.mBlockPointer);
					return true;
				}


				@Override
				boolean afterInteriorNode(BTreeInteriorNode aNode)
				{
					prev.freeBlock(aNode.mBlockPointer);
					return true;
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
		mTree.close();
		mTree = null;
	}


	boolean isModified()
	{
		return mTree.isChanged();
	}


	long flush()
	{
		return mTree.flush();
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

			return mTree.commit();
		}
	}


	void rollback()
	{
		mTree.rollback();
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


	private Document deleteExternal(ArrayMapEntry aEntry, boolean aRestoreOldValue)
	{
		Document prev = null;

		if (aEntry != null && aEntry.getType() == TYPE_EXTERNAL)
		{
			Document header = aEntry.getValue();
			BlockPointer bp = new BlockPointer().unmarshalDocument(header);

//			try
//			{
				if (aRestoreOldValue)
				{
					RuntimeDiagnostics.collectStatistics(Operation.READ_EXT, 1);

					prev = new Document().fromByteArray(mDatabase.getBlockAccessor().readBlock(bp));
//					try (LobByteChannel lob = new LobByteChannel(mDatabase.getBlockAccessor(), header, LobOpenOption.READ, null))
//					{
//						prev = new Document().fromByteArray(lob.readAllBytes());
//					}
				}

				RuntimeDiagnostics.collectStatistics(Operation.FREE_EXT, 1);

				mDatabase.getBlockAccessor().freeBlock(bp);

//				try (LobByteChannel lob = new LobByteChannel(mDatabase.getBlockAccessor(), header, LobOpenOption.REPLACE, null))
//				{
//				}
//			}
//			catch (IOException e)
//			{
//				throw new DatabaseException(e);
//			}
		}

		return prev;
	}


	public BTree _getImplementation()
	{
		return mTree;
	}


	Document getConfiguration()
	{
		return mConfiguration;
	}


	Document unmarshalDocument(ArrayMapEntry aEntry, Document aDestination)
	{
		if (aEntry.getType() == TYPE_EXTERNAL)
		{
			RuntimeDiagnostics.collectStatistics(Operation.READ_EXT, 1);

			Document header = aEntry.getValue();

			BlockPointer bp = new BlockPointer().unmarshalDocument(header);
			return aDestination.putAll(new Document().fromByteArray(mDatabase.getBlockAccessor().readBlock(bp)));

//			try (LobByteChannel lob = new LobByteChannel(mDatabase.getBlockAccessor(), header, LobOpenOption.READ, null))
//			{
//				return aDestination.putAll(new Document().fromByteArray(lob.readAllBytes()));
//			}
//			catch (IOException e)
//			{
//				throw new DatabaseException(e);
//			}
		}

		return aDestination.putAll(aEntry.getValue());
	}


	public List<Document> find(Document aQuery)
	{
		log.i("find {}", aQuery);
		log.inc();

//		System.out.println(aQuery);
//		System.out.println(mDatabase.mIndices);
		Document bestIndex = null;
		int matchingFields = 0;
		ArrayList<String> queryKeys = aQuery.keySet();

		HashMap<ObjectId, Document> indices = mDatabase.mIndices.get(getCollectionId());
		if (indices != null)
		{
			for (Document conf : indices.values())
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

			mTree.visit(new BTreeVisitor()
			{
				@Override
				boolean beforeInteriorNode(BTreeInteriorNode aNode, ArrayMapKey aLowestKey, ArrayMapKey aHighestKey)
				{
					return matchKey(aLowestKey, aHighestKey, aQuery);
				}


				@Override
				boolean beforeLeafNode(BTreeLeafNode aNode)
				{
					boolean b = matchKey(aNode.mMap.getFirst().getKey(), aNode.mMap.getLast().getKey(), aQuery);

//					System.out.println("#" + aNode.mMap.getFirst().getKey()+", "+aNode.mMap.getLast().getKey() +" " + b+ " "+aQuery.getArray("_id"));
					return b;
				}


				@Override
				boolean leaf(BTreeLeafNode aNode)
				{
//					System.out.println(aNode);
					for (int i = 0; i < aNode.mMap.size(); i++)
					{
						ArrayMapEntry entry = aNode.mMap.get(i, new ArrayMapEntry());
						Document doc = unmarshalDocument(entry, new Document());

						if (matchKey(doc, aQuery))
						{
							list.add(doc);
						}
					}
					return true;
				}
			});

			return list;
		}
		finally
		{
			log.dec();
		}
	}

	//    A     B	Node
	//    1     1	1
	//    1     2	1
	//    1     3	1
	//    1     4	1
	//    2     1	1
	//    2     2	1
	//    2     3	2
	//    2     4	2
	//    3     1	2
	//    3     2	3
	//    3     3	3
	//    3     4	3
	//    4     1	3
	//    4     2	3
	//    4     3	3
	//    4     4	4
	//
	// 1: null -- 2,2
	// 2: 2,3  -- 3,1
	// 3: 3,2  -- 4,3
	// 4: 4,4  -- null

	private boolean matchKey(ArrayMapKey aLowestKey, ArrayMapKey aHighestKey, Document aQuery)
	{
		Array lowestKey = aLowestKey == null ? null : (Array)aLowestKey.get();
		Array highestKey = aHighestKey == null ? null : (Array)aHighestKey.get();
		Array array = aQuery.getArray("_id");

		int a = lowestKey == null ? 0 : compare(lowestKey, array);
		int b = highestKey == null ? 0 : compare(highestKey, array);

		return a >= 0 && b <= 0;
	}


	private int compare(Array aCompare, Array aWith)
	{
		for (int i = 0; i < aWith.size(); i++)
		{
			Comparable v = aWith.get(i);
			Comparable b = aCompare.get(i);

			int r = v.compareTo(b);

			if (r != 0)
			{
				return r;
			}
		}

		return 0;
	}


	private boolean matchKey(Document aEntry, Document aQuery)
	{
		Array array = aQuery.getArray("_id");

		for (int i = 0; i < array.size(); i++)
		{
			Comparable v = array.get(i);
			Object b = aEntry.getArray("_id").get(i);

			if (v.compareTo(b) != 0)
			{
				return false;
			}
		}

		return true;
	}


	public ScanResult getStats()
	{
		ScanResult scanResult = new ScanResult();
		mTree.scan(scanResult);
		System.out.println(scanResult);

		return scanResult;
//		return Document.of("depth:0, nodes:0, leafs:0, documents:0, externals:0, logicalSize:0, physicalSize:0, nodeFill: 0, leafFill: 0, nodeDegree: 0, pendingWrites:0, cacheSize:0");
	}
}
