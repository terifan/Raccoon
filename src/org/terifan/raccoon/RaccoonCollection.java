package org.terifan.raccoon;

import org.terifan.raccoon.exceptions.DocumentNotFoundException;
import org.terifan.raccoon.exceptions.UniqueConstraintException;
import org.terifan.raccoon.exceptions.DuplicateKeyException;
import org.terifan.raccoon.exceptions.CommitBlockedException;
import org.terifan.raccoon.btree.ArrayMapKey;
import org.terifan.raccoon.btree.ArrayMapEntry;
import org.terifan.raccoon.btree.BTreeVisitor;
import org.terifan.raccoon.btree.BTree;
import org.terifan.raccoon.btree.BTreeLeafNode;
import org.terifan.raccoon.document.ObjectId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
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
	final static Logger log = Logger.getLogger();
	final ReadWriteLock mLock;
	long mModCount;

	public final static byte TYPE_TREENODE = 0;
	public final static byte TYPE_DOCUMENT = 1;
	public final static byte TYPE_EXTERNAL = 2;

	public static final String CONFIGURATION = "collection";

	private ExecutorService mExecutor = Executors.newFixedThreadPool(1);

	private Document mConfiguration;
	private Supplier<Document> mDocumentSupplier;
	private Supplier<Object> mKeySupplier;
	private RaccoonDatabase mDatabase;
	private HashSet<CommitLock> mCommitLocks;
	private BTree mTree;


	RaccoonCollection(RaccoonDatabase aDatabase, Document aConfiguration)
	{
		mDatabase = aDatabase;
		mConfiguration = aConfiguration;

		mLock = new ReadWriteLock();
		mCommitLocks = new HashSet<>();
		mDocumentSupplier = () -> new Document();
		mKeySupplier = () -> ObjectId.randomId();

		mTree = new BTree(getBlockAccessor(), aConfiguration.getDocument(CONFIGURATION));
	}


	/**
	 * Find many documents from the collection returning a list with those found.
	 *
	 * @return those documents matching the criteria provided
	 */
	public Future<ArrayList<Document>> tryFindMany(Document... aDocuments)
	{
		return findMany(false, Arrays.asList(aDocuments));
	}


	/**
	 * Find many documents from the collection returning a list with those found.
	 *
	 * @return those documents matching the criteria provided
	 */
	public Future<ArrayList<Document>> tryFindMany(Iterable<Document> aDocuments)
	{
		return findMany(false, aDocuments);
	}


	/**
	 * Atomically find many documents from the collection or throws an exception if any not found.
	 *
	 * @return documents matching the criteria provided
	 */
	public Future<ArrayList<Document>> findMany(Document... aDocuments) throws DocumentNotFoundException
	{
		return findMany(true, Arrays.asList(aDocuments));
	}


	/**
	 * Find a document from the collection or throws an exception if not found.
	 */
	public Future<Document> findOne(Document aDocument)
	{
		Document result = new Document();
		WriteTask task = new WriteTask(this, "find one")
		{
			@Override
			public void call()
			{
				ArrayMapEntry entry = new ArrayMapEntry(getDocumentKey(aDocument, false));

				if (!mTree.get(entry))
				{
					throw new DocumentNotFoundException();
				}

				unmarshalDocument(entry, result);
			}
		};
		return mExecutor.submit(task, result);
	}


	/**
	 * Find the document from the collection returning if it was found.
	 *
	 * @return if a document was found
	 */
	public boolean tryFindOne(Document aDocument)
	{
		log.i("tryFindOne {}", aDocument);
		log.inc();

		try (ReadLock lock = mLock.readLock())
		{
			ArrayMapEntry entry = new ArrayMapEntry(getDocumentKey(aDocument, false));

			if (mTree.get(entry))
			{
				unmarshalDocument(entry, aDocument);
				return true;
			}

			return false;
		}
		finally
		{
			log.dec();
		}
	}


	/**
	 * Insert or replace a document in this collection returning if it was inserted.
	 */
	public Future<Result> saveOne(Document aDocument)
	{
		Result result = new Result();
		WriteTask task = new WriteTask(this, "replace one")
		{
			@Override
			public void call()
			{
				result.increment(insertOrUpdate(aDocument, false) ? "insert" : "replace");
			}
		};
		return mExecutor.submit(task, result);
	}


	/**
	 * Insert or replace many documents in this collection returning number of inserts.
	 *
	 * @return number of inserted documents
	 */
	public Future<Result> saveMany(Document... aDocuments)
	{
		return saveMany(Arrays.asList(aDocuments));
	}


	/**
	 * Insert or replace many documents in this collection returning number of inserts.
	 *
	 * @return number of inserted documents
	 */
	public Future<Result> saveMany(Iterable<Document> aDocuments)
	{
		Result result = new Result();
		WriteTask task = new WriteTask(this, "save many")
		{
			@Override
			public void call()
			{
				for (Document document : aDocuments)
				{
					result.increment(insertOrUpdate(document, false) ? "insert" : "replace");
				}
			}
		};
		return mExecutor.submit(task, result);
	}


	/**
	 * Insert a document in this collection or throws an exception if the document already exists.
	 */
	public Future<Result> insertOne(Document aDocument)
	{
		Result result = new Result();
		WriteTask task = new WriteTask(this, "insert one")
		{
			@Override
			public void call()
			{
				result.increment(insertOrUpdate(aDocument, true) ? "insert" : "replace");
			}
		};
		return mExecutor.submit(task, result);
	}


	/**
	 * Atomically insert many documents in this collection or throws an exception if any document already exists.
	 */
	public Future<Result> insertMany(Document... aDocuments)
	{
		return insertMany(Arrays.asList(aDocuments));
	}


	/**
	 * Atomically insert many documents in this collection or throws an exception if any document already exists.
	 */
	public Future<Result> insertMany(Iterable<Document> aDocuments)
	{
		Result result = new Result();
		WriteTask task = new WriteTask(this, "insert many")
		{
			@Override
			public void call()
			{
				for (Document document : aDocuments)
				{
					result.increment(insertOrUpdate(document, true) ? "insert" : "replace");
				}
			}
		};
		return mExecutor.submit(task, result);
	}


	/**
	 * Replace one document in this collection or throws an exception if document is missing.
	 */
	public Future<Result> replaceOne(Document aDocument) throws DocumentNotFoundException
	{
		Result result = new Result();
		WriteTask task = new WriteTask(this, "replace one")
		{
			@Override
			public void call()
			{
				ArrayMapEntry entry = new ArrayMapEntry(getDocumentKey(aDocument, false));
				if (!mTree.get(entry))
				{
					throw new DocumentNotFoundException();
				}
				result.increment(insertOrUpdate(aDocument, false) ? "insert" : "replace");
			}
		};
		return mExecutor.submit(task, result);
	}


	/**
	 * Replace one document in this collection and return if it was replaced.
	 *
	 * @return if a document was replaced
	 */
	public boolean tryReplaceOne(Document aDocument) throws DocumentNotFoundException
	{
		log.i("insert {}", aDocument);
		log.inc();

		try (WriteLock lock = mLock.writeLock())
		{
			mModCount++;

			return !insertOrUpdate(aDocument, false);
		}
		finally
		{
			log.dec();
		}
	}


	/**
	 * Replace many document in this collection and return the number of replaced.
	 */
	public int tryReplaceMany(Document... aDocuments) throws DocumentNotFoundException
	{
		return tryReplaceMany(Arrays.asList(aDocuments));
	}


	/**
	 * Replace many document in this collection and return the number of replaced.
	 */
	public int tryReplaceMany(Iterable<Document> aDocuments) throws DocumentNotFoundException
	{
		log.i("replace many");
		log.inc();

		try (WriteLock lock = mLock.writeLock())
		{
			mModCount++;
			int count = 0;

			for (Document document : aDocuments)
			{
				if (!insertOrUpdate(document, false))
				{
					count++;
				}
			}

			return count;
		}
		finally
		{
			log.dec();
		}
	}


	/**
	 * Atomically replace documents in this collection or throws an exception if any document missing.
	 */
	public Future<Result> replaceMany(Document... aDocuments) throws DocumentNotFoundException
	{
		return replaceMany(Arrays.asList(aDocuments));
	}


	/**
	 * Atomically replace documents in this collection or throws an exception if any document missing.
	 */
	public Future<Result> replaceMany(Iterable<Document> aDocuments) throws DocumentNotFoundException
	{
		Result result = new Result();
		WriteTask task = new WriteTask(this, "replace many")
		{
			@Override
			public void call()
			{
				for (Document document : aDocuments)
				{
					ArrayMapEntry entry = new ArrayMapEntry(getDocumentKey(document, false));

					if (!mTree.get(entry))
					{
						throw new DocumentNotFoundException();
					}
				}

				for (Document document : aDocuments)
				{
					result.increment(insertOrUpdate(document, false) ? "insert" : "replace");
				}
			}
		};
		return mExecutor.submit(task, result);
	}


	/**
	 * Delete one document or throws an exception if not found.
	 */
	public Future<Result> deleteOne(Document aDocument) throws DocumentNotFoundException
	{
		return deleteImpl(true, Arrays.asList(aDocument));
	}


	/**
	 * Delete many documents return the number of deleted documents.
	 */
	public Future<Result> tryDeleteMany(Document... aDocument)
	{
		return tryDeleteMany(Arrays.asList(aDocument));
	}


	/**
	 * Delete many documents return the number of deleted documents.
	 */
	public Future<Result> tryDeleteMany(Iterable<Document> aDocuments)
	{
		Result result = new Result();
		WriteTask task = new WriteTask(this, "replace many")
		{
			@Override
			public void call()
			{
				for (Document key : aDocuments)
				{
					ArrayMapEntry prevEntry = mTree.remove(new ArrayMapEntry(getDocumentKey(key, false)));

					if (prevEntry != null)
					{
						result.increment("delete");

						Document prevDoc = deleteExternal(prevEntry, true);

						if (prevDoc != null)
						{
							deleteIndexEntries(prevEntry.getValue());
						}
					}
				}
			}
		};
		return mExecutor.submit(task, result);
	}


	/**
	 * Try delete one document and return if it was deleted.
	 *
	 * @return if a document was deleted
	 */
	public boolean tryDeleteOne(Document aDocument)
	{
		try
		{
			return deleteImpl(false, Arrays.asList(aDocument)).get() != null;
		}
		catch (InterruptedException | ExecutionException e)
		{
			return false;
		}
	}


	/**
	 * Atomically find a document and then delete it.
	 *
	 * @return the deleted document
	 */
	public Future<Document> findOneAndDelete(Document aDocument)
	{
		Result result = new Result();
		WriteTask task = new WriteTask(this, "findOneAndDelete")
		{
			@Override
			public void call()
			{
				ArrayMapEntry prev = mTree.remove(new ArrayMapEntry(getDocumentKey(aDocument, false)));

				if (prev != null)
				{
					Document prevDoc = deleteExternal(prev, true);
					if (prevDoc == null)
					{
						prevDoc = prev.getValue();
					}
					result.putAll(prevDoc);
					deleteIndexEntries(prevDoc);
				}
			}
		};
		return mExecutor.submit(task, result);
	}


	/**
	 * Atomically delete many documents or throws an exception if any not found.
	 */
	public Future<Result> deleteMany(Document... aDocuments)
	{
		return deleteImpl(false, Arrays.asList(aDocuments));
	}


	/**
	 * Atomically delete many documents or throws an exception if any not found.
	 */
	public Future<Result> deleteMany(Iterable<Document> aDocuments)
	{
		return deleteImpl(false, aDocuments);
	}


	/**
	 * Return all documents in this collection.
	 */
	public Future<ArrayList<Document>> find()
	{
		ArrayList<Document> result = new ArrayList<>();
		ReadTask task = new ReadTask(this, "find")
		{
			@Override
			public void call()
			{
				mTree.visit(new BTreeVisitor()
				{
					@Override
					public boolean leaf(BTreeLeafNode aNode)
					{
						aNode.forEachEntry(e -> result.add(unmarshalDocument(e, new Document())));
						return true;
					}
				});
			}
		};
		return mExecutor.submit(task, result);
	}


	/**
	 * Return the number of documents in this collection.
	 */
	public long size()
	{
		try (ReadLock lock = mLock.readLock())
		{
			return mTree.size();
		}
	}


	/**
	 * Visit all documents in this collection.
	 */
	public void forEach(Consumer<Document> aAction)
	{
		try (ReadLock lock = mLock.readLock())
		{
			mTree.visit(new BTreeVisitor()
			{
				@Override
				public boolean leaf(BTreeLeafNode aNode)
				{
					aNode.forEachEntry(e -> aAction.accept(unmarshalDocument(e, mDocumentSupplier.get())));
					return true;
				}
			});
		}
	}


	/**
	 * Delete all documents in this collection.
	 */
	public void drop()
	{
		try (WriteLock lock = mLock.writeLock())
		{
			mModCount++;

//			BTree prev = mTree;
//			mTree = new BTree(getBlockAccessor(), mTree.getConfiguration().clone().remove("root"));
			mTree.drop(e -> deleteExternal(e, false));

			mDatabase.removeCollectionImpl(this);
		}

		mDatabase = null;
		mTree = null;
		mTree = null;
	}


	void close()
	{
		try
		{
			mExecutor.shutdown();
			mExecutor.awaitTermination(1, TimeUnit.HOURS);
			mExecutor = null;
		}
		catch (Exception e)
		{
			e.printStackTrace(System.out);
		}

		mTree.close();
		mTree = null;
	}


	boolean isChanged()
	{
		return mTree.isChanged();
	}


	long flush()
	{
		return mTree.flush();
	}


	void rollback()
	{
		mTree.rollback();
	}


	public Future<Result> commit()
	{
		return commit(() ->
		{
		});
	}


	Future<Result> commit(Runnable aOnModifiedAction)
	{
		Result result = new Result();
		WriteTask task = new WriteTask(this, "commit")
		{
			@Override
			public void call()
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

				if (mTree.commit())
				{
					result.increment("commit");
					aOnModifiedAction.run();
				}
			}
		};
		return mExecutor.submit(task, result);
	}


	public ArrayList<Document> find(Document aFilter)
	{
		log.i("find {}", aFilter);
		log.inc();

//		System.out.println(aQuery);
//		System.out.println(mDatabase.mIndices);
		Document bestIndex = null;
		int matchingFields = 0;
		ArrayList<String> queryKeys = aFilter.keySet();

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

			return mDatabase.getCollection("index:" + bestIndex.getArray("_id")).findImpl(aFilter);
		}

		return findImpl(aFilter);
	}


	BTree _getImplementation()
	{
		return mTree;
	}


	Document getConfiguration()
	{
		return mConfiguration;
	}


	BlockAccessor getBlockAccessor()
	{
		return new BlockAccessor(mDatabase.getBlockDevice(), true);
	}


	ObjectId getCollectionId()
	{
		return mConfiguration.getObjectId("_id");
	}


	private ArrayMapKey getDocumentKey(Object aDocument, boolean aCreateMissingKey)
	{
		if (aDocument instanceof Document v)
		{
			Object id = v.get("_id");

			if (id == null)
			{
				if (!aCreateMissingKey)
				{
					throw new IllegalArgumentException("Document missing _id field.");
				}
				id = mKeySupplier.get();
				v.put("_id", id);
			}

			return new ArrayMapKey(id);
		}

		if (aDocument == null)
		{
			if (!aCreateMissingKey)
			{
				throw new IllegalArgumentException("Null is not a valid _id.");
			}
			aDocument = mKeySupplier.get();
		}

		return new ArrayMapKey(aDocument);
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

			BlockPointer bp = mDatabase.getBlockAccessor().writeBlock(aDocument.toByteArray(), BlockType.EXTERNAL, 0, CompressorAlgorithm.LZJB.ordinal());

			entry.setValue(bp.marshalDocument());
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

					try
					{
						Document existing = getIndexByConf(indexConf).findOne(new Document().put("_id", values)).get();

						if (existing != null && !aDocument.get("_id").equals(existing.get("_ref")))
						{
							throw new UniqueConstraintException("Collection index <" + indexConf.getObjectId("_id") + ">, existing ID <" + existing.get("_ref") + ">, saving ID <" + aDocument.get("_id") + ">, values " + values.toJson());
						}
					}
					catch (InterruptedException | ExecutionException | UniqueConstraintException e)
					{
						e.printStackTrace(System.out);
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

				getIndexByConf(indexConf).saveOne(indexEntry);
			}
		}

		return prev == null;
	}


	private Future<Result> deleteImpl(boolean aFailFast, Iterable<Document> aDocuments)
	{
		Result result = new Result();
		WriteTask task = new WriteTask(this, "delete")
		{
			@Override
			public void call()
			{
				for (Document doc : aDocuments)
				{
					ArrayMapEntry prev = mTree.remove(new ArrayMapEntry(getDocumentKey(doc, false)));

					if (prev != null)
					{
						Document prevDoc = deleteExternal(prev, true);

						if (prevDoc == null)
						{
							prevDoc = prev.getValue();
						}

						deleteIndexEntries(prevDoc);
					}
					else if (aFailFast)
					{
						throw new DocumentNotFoundException(doc.get("_id"));
					}
				}
			}
		};
		return mExecutor.submit(task, result);
	}


	private Document deleteExternal(ArrayMapEntry aEntry, boolean aRestoreOldValue)
	{
		Document prev = null;

		if (aEntry != null && aEntry.getType() == TYPE_EXTERNAL)
		{
			Document header = aEntry.getValue();
			BlockPointer bp = new BlockPointer().unmarshalDocument(header);

			if (aRestoreOldValue)
			{
				RuntimeDiagnostics.collectStatistics(Operation.READ_EXT, 1);

				prev = mDocumentSupplier.get().fromByteArray(mDatabase.getBlockAccessor().readBlock(bp));
			}

			RuntimeDiagnostics.collectStatistics(Operation.FREE_EXT, 1);

			mDatabase.getBlockAccessor().freeBlock(bp);
		}

		return prev;
	}


	private Document unmarshalDocument(ArrayMapEntry aEntry, Document aDestination)
	{
		if (aEntry.getType() == TYPE_EXTERNAL)
		{
			RuntimeDiagnostics.collectStatistics(Operation.READ_EXT, 1);

			Document header = aEntry.getValue();

			BlockPointer bp = new BlockPointer().unmarshalDocument(header);

			return aDestination.putAll(new Document().fromByteArray(mDatabase.getBlockAccessor().readBlock(bp)));
		}

		return aDestination.putAll(aEntry.getValue());
	}


	ArrayList<Document> findImpl(Document aFilter)
	{
		try (ReadLock lock = mLock.readLock())
		{
			ArrayList<Document> list = new ArrayList<>();
			mTree.find(list, aFilter, entry -> unmarshalDocument(entry, mDocumentSupplier.get()));
			return list;
		}
		finally
		{
			log.dec();
		}
	}


	private Future<ArrayList<Document>> findMany(boolean aFailFast, Iterable<Document> aDocuments)
	{
		ArrayList<Document> result = new ArrayList<>();
		ReadTask task = new ReadTask(this, "find")
		{
			@Override
			public void call()
			{
				for (Document doc : aDocuments)
				{
					ArrayMapEntry entry = new ArrayMapEntry(getDocumentKey(doc, false));

					if (mTree.get(entry))
					{
						result.add(unmarshalDocument(entry, doc));
					}
					else
					{
						if (aFailFast)
						{
							throw new DocumentNotFoundException();
						}
						result.add(null);
					}
				}
			}
		};
		return mExecutor.submit(task, result);
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

	public ScanResult _getStats()
	{
		ScanResult scanResult = new ScanResult();
		mTree.scan(scanResult);
		System.out.println(scanResult);

		return scanResult;
//		return Document.of("depth:0, nodes:0, leafs:0, documents:0, externals:0, logicalSize:0, physicalSize:0, nodeFill: 0, leafFill: 0, nodeDegree: 0, pendingWrites:0, cacheSize:0");
	}


	/**
	 * Set the supplier of new Document instances. This allow custom implementations of Document to be created. Default
	 * <code>new Document()</code>.
	 */
	public RaccoonCollection withDocumentSupplier(Supplier<Document> aSupplier)
	{
		mDocumentSupplier = aSupplier;
		return this;
	}


	/**
	 * Set the supplier of new document ID values. Default is <code>ObjectId.randomId()</code>.
	 */
	public RaccoonCollection withIdSupplier(Supplier<Object> aSupplier)
	{
		mKeySupplier = aSupplier;
		return this;
	}


	public void createIndex(Document aConfiguration, Document aFields)
	{
		log.i("create index");
		log.inc();

		Array id = Array.of(getCollectionId(), mKeySupplier.get());

		Document indexConf = new Document()
			.put("_id", id)
			.put("configuration", aConfiguration)
			.put("fields", aFields);

		mDatabase.getCollection("$indices").saveOne(indexConf);

		mDatabase.mIndices.put(id.getObjectId(0), id.getObjectId(1), indexConf);

//		try (WriteLock lock = mLock.writeLock())
//		{
//			RaccoonCollection collection = mDatabase.getCollection(INDEX_COLLECTION);
//
//			collection.find(new Document().put("collection", getCollectionId()));
//
//			Document conf = new Document().put("_id", new Document().put("collection", mConfiguration.getObjectId("object")).put("_id", mIdSupplier.get())).put("configuration", aDocument);
//			collection.save(conf);
//
//			System.out.println(conf);
//		}
//		finally
//		{
//			Log.dec();
//		}
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


	private void deleteIndexEntries(Document aPrevDoc)
	{
		for (Document indexConf : mDatabase.mIndices.values(getCollectionId()))
		{
//			ArrayList<Array> result = new ArrayList<>();
//			generatePermutations(indexConf, aPrevDoc, new Array(), 0, result);
//			for (Array values : result)
//			{
//			}

			try
			{
				List<Document> list = getIndexByConf(indexConf).find().get();
				for (Document doc : list)
				{
//					if (doc.get("_ref").equals(aPrevDoc.get("_id")))
					{
						getIndexByConf(indexConf).findOneAndDelete(doc);
						break;
					}
				}
			}
			catch (Exception e)
			{
				e.printStackTrace(System.out);
			}
		}
	}


	RaccoonCollection getIndexByConf(Document aIndexConf)
	{
		return mDatabase.getCollection("index:" + aIndexConf.getArray("_id"));
	}
}
