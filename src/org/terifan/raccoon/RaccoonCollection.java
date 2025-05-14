package org.terifan.raccoon;

import org.terifan.raccoon.result.DeleteManyResult;
import org.terifan.raccoon.result.InsertManyResult;
import org.terifan.raccoon.result.SaveManyResult;
import org.terifan.raccoon.result.DropResult;
import org.terifan.raccoon.result.CommitResult;
import org.terifan.raccoon.result.ReplaceManyResult;
import java.io.IOException;
import org.terifan.raccoon.exceptions.DocumentNotFoundException;
import org.terifan.raccoon.exceptions.DuplicateKeyException;
import org.terifan.raccoon.btree.BTree;
import org.terifan.raccoon.document.ObjectId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Spliterators;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.terifan.logging.Logger;
import org.terifan.raccoon.RuntimeDiagnostics.Operation;
import org.terifan.raccoon.blockdevice.BlockAccessor;
import org.terifan.raccoon.blockdevice.BlockPointer;
import org.terifan.raccoon.blockdevice.BlockType;
import org.terifan.raccoon.blockdevice.compressor.CompressorAlgorithm;
import org.terifan.raccoon.btree.BTreeConfiguration;
import org.terifan.raccoon.btree.BTreeIterator;
import org.terifan.raccoon.btree.BTreeLeafNode;
import org.terifan.raccoon.btree.BTreeVisitor;
import org.terifan.raccoon.btree.ArrayMapEntry;
import org.terifan.raccoon.btree.ArrayMapEntry.Type;
import org.terifan.raccoon.btree.OpState;
import org.terifan.raccoon.document.Array;
import org.terifan.raccoon.document.Document;
import org.terifan.raccoon.exceptions.DatabaseException;
import org.terifan.raccoon.result.SaveOneResult;

// db.getCollection("people").createIndex(Document.of("name:malesByName,unique:false,sparse:true,clone:true,filter:[{gender:{$eq:male}}],fields:[{firstName:1},{lastName:1}]"));

public final class RaccoonCollection implements Iterable<Document>
{
	final static Logger log = Logger.getLogger();

	private ExecutorService mExecutor = Executors.newFixedThreadPool(1);

	private final BTreeConfiguration mConfiguration;
	private Supplier<Document> mDocumentSupplier;
	private Supplier<Object> mKeySupplier;
	private RaccoonDatabase mDatabase;
	private BTree mTree;


	RaccoonCollection(RaccoonDatabase aDatabase, BTreeConfiguration aConfiguration)
	{
		mDatabase = aDatabase;
		mConfiguration = aConfiguration;

		mDocumentSupplier = () -> new Document();
		mKeySupplier = () -> ObjectId.randomId();

		mTree = new BTree(getBlockAccessor(), mConfiguration);
	}


	/**
	 * Find documents from the collection updating the document instances provided.
	 *
	 * @return if all was found
	 */
	public boolean tryFindMany(Document... aDocuments)
	{
		return tryFindMany(Arrays.asList(aDocuments));
	}


	/**
	 * Find documents from the collection updating the document instances provided.
	 *
	 * @return if all was found
	 */
	public boolean tryFindMany(List<Document> aDocuments)
	{
		AtomicBoolean result = new AtomicBoolean(true);
		ReadTask task = new ReadTask(this, "tryFindMany")
		{
			@Override
			public void call()
			{
				for (Document document : aDocuments)
				{
					ArrayMapEntry entry = new ArrayMapEntry().setKey(document);
					mTree.get(entry);
					if (entry.getState() == OpState.MATCH)
					{
						document.putAll(readExternalEntry(entry));
					}
					else
					{
						result.set(false);
					}
				}
			}
		};
		try
		{
			return mExecutor.submit(task, result).get().get();
		}
		catch (InterruptedException | ExecutionException e)
		{
			throw new DatabaseException(e);
		}
	}


	/**
	 * Atomically find documents from the collection.
	 *
	 * @return documents matching the criteria provided
	 */
	public Future<ArrayList<Document>> findMany(Document... aDocuments) throws DocumentNotFoundException
	{
		ArrayList<Document> result = new ArrayList<>();
		ReadTask task = new ReadTask(this, "findMany")
		{
			@Override
			public void call()
			{
				for (Document document : aDocuments)
				{
					ArrayMapEntry entry = new ArrayMapEntry().setKeyAndValue(document);
					mTree.get(entry);
					if (entry.getState() == OpState.NO_MATCH)
					{
						throw new DocumentNotFoundException(entry.getKeyInstance());
					}
					result.add(readExternalEntry(entry));
				}
			}
		};
		return mExecutor.submit(task, result);
	}


	/**
	 * Find a document from the collection or throws an exception if not found.
	 *
	 * @return the found document
	 */
	public Future<Document> findOne(Document aDocument)
	{
		ArrayMapEntry entry = new ArrayMapEntry().setKeyAndValue(aDocument);
		WriteTask task = new WriteTask(this, "findOne")
		{
			@Override
			public void call()
			{
				mTree.get(entry);
				if (entry.getState() == OpState.NO_MATCH)
				{
					throw new DocumentNotFoundException(entry.toKeyString());
				}
				aDocument.putAll(readExternalEntry(entry));
			}
		};
		return mExecutor.submit(task, aDocument);
	}


	/**
	 * Find one document from the collection updating the provided document returning true if it was found.
	 *
	 * @return if a document was found
	 */
	public boolean tryFindOne(Document aDocument)
	{
		ArrayMapEntry entry = new ArrayMapEntry().setKeyAndValue(aDocument);

		AtomicBoolean result = new AtomicBoolean(false);
		ReadTask task = new ReadTask(this, "tryFindOne")
		{
			@Override
			public void call()
			{
				mTree.get(entry);

				if (entry.getState() == OpState.MATCH)
				{
					aDocument.putAll(readExternalEntry(entry));
					result.set(true);
				}
			}
		};
		try
		{
			return mExecutor.submit(task, result).get().get();
		}
		catch (InterruptedException | ExecutionException e)
		{
			throw new DatabaseException(e);
		}
	}


	/**
	 * Insert or replace a document in this collection returning a summary of changes.
	 *
	 * @return a summary of changes
	 */
	public Future<SaveOneResult> saveOne(Document aDocument)
	{
		SaveOneResult result = new SaveOneResult();
		WriteTask task = new WriteTask(this, "saveOne")
		{
			@Override
			public void call()
			{
				createKeys(aDocument);

				ArrayMapEntry entry = new ArrayMapEntry().setKeyAndValue(aDocument);
				writeExternalEntry(entry);
				mTree.put(entry);
				if (entry.getState() == OpState.UPDATE)
				{
					deleteIndexEntries(deleteExternal(entry, true));
					result.updated.add(aDocument);
				}
				else
				{
					result.inserted.add(aDocument);
				}
			}
		};
		return mExecutor.submit(task, result);
	}


	/**
	 * Insert or replace documents in this collection returning a summary of changes.
	 *
	 * @return a summary of changes
	 */
	public Future<SaveManyResult> saveMany(Document... aDocuments)
	{
		return saveMany(Arrays.asList(aDocuments));
	}


	/**
	 * Insert or replace documents in this collection returning a summary of changes.
	 *
	 * @return a summary of changes
	 */
	public Future<SaveManyResult> saveMany(List<Document> aDocuments)
	{
		SaveManyResult result = new SaveManyResult();
		WriteTask task = new WriteTask(this, "saveMany")
		{
			@Override
			public void call()
			{
				for (Document document : aDocuments)
				{
					createKeys(document);

					ArrayMapEntry entry = new ArrayMapEntry().setKeyAndValue(document);
					writeExternalEntry(entry);
					mTree.put(entry);
					if (entry.getState() == OpState.UPDATE)
					{
						deleteIndexEntries(deleteExternal(entry, true));
					}
					if (entry.getState() == OpState.INSERT)
					{
						result.inserted.add(document);
					}
					else
					{
						result.updated.add(document);
					}
				}
			}
		};
		return mExecutor.submit(task, result);
	}


	/**
	 * Insert a document in this collection or throws an exception if the document already exists.
	 *
	 * @return a summary of changes
	 */
	public Future<Document> insertOne(Document aDocument)
	{
		ArrayMapEntry entry = new ArrayMapEntry().setKeyAndValue(aDocument);
		WriteTask task = new WriteTask(this, "insertOne")
		{
			@Override
			public void call()
			{
				mTree.get(entry);
				if (entry.getState() == OpState.MATCH)
				{
					throw new DuplicateKeyException();
				}
				writeExternalEntry(entry);
				mTree.put(entry);
				if (entry.getState() == OpState.UPDATE)
				{
					deleteIndexEntries(deleteExternal(entry, true));
				}
			}
		};
		return mExecutor.submit(task, aDocument);
	}


	/**
	 * Atomically insert many documents in this collection returning a summary of changes.
	 *
	 * @return a summary of changes
	 */
	public Future<List<Document>> insertMany(Document... aDocuments)
	{
		return insertMany(Arrays.asList(aDocuments));
	}


	/**
	 * Atomically insert many documents in this collection returning a summary of changes.
	 *
	 * @return a summary of changes
	 */
	public Future<List<Document>> insertMany(List<Document> aDocuments)
	{
		WriteTask task = new WriteTask(this, "insertMany")
		{
			@Override
			public void call()
			{
				ArrayList<ArrayMapEntry> entries = new ArrayList<>();
				for (Document document : aDocuments)
				{
					ArrayMapEntry entry = new ArrayMapEntry().setKeyAndValue(document);
					mTree.get(entry);
					if (entry.getState() == OpState.NO_MATCH)
					{
						throw new DocumentNotFoundException(entry.toKeyString());
					}
				}
				for (ArrayMapEntry entry : entries)
				{
					writeExternalEntry(entry);
					mTree.put(entry);
				}
			}
		};
		return mExecutor.submit(task, aDocuments);
	}


	/**
	 * Insert a single Document ensuring that it doesn't already exists.
	 *
	 * @return if a document was inserted
	 */
	public boolean tryInsertOne(Document aDocument)
	{
		ArrayMapEntry entry = new ArrayMapEntry().setKeyAndValue(aDocument);
		ArrayMapEntry existing = new ArrayMapEntry().setKey(entry.getKey(), entry.getKeyType());
		AtomicBoolean result = new AtomicBoolean(false);
		ReadTask task = new ReadTask(this, "tryInsertOne")
		{
			@Override
			public void call()
			{
				mTree.get(existing);

				if (existing.getState() == OpState.NO_MATCH)
				{
					mTree.put(entry);
					result.set(true);
				}
			}
		};
		try
		{
			return mExecutor.submit(task, result).get().get();
		}
		catch (InterruptedException | ExecutionException e)
		{
			throw new DatabaseException(e);
		}
	}


	/**
	 * Insert a single Document ensuring that it doesn't already exists or updated the aExisting with the existing Document.
	 *
	 * @return if a document was inserted
	 */
	public boolean tryInsertOne(Document aDocument, Document aExisting)
	{
		ArrayMapEntry entry = new ArrayMapEntry().setKeyAndValue(aDocument);
		ArrayMapEntry existing = new ArrayMapEntry().setKey(entry.getKey(), entry.getKeyType());

		AtomicBoolean result = new AtomicBoolean(false);
		ReadTask task = new ReadTask(this, "tryInsertOne")
		{
			@Override
			public void call()
			{
				mTree.get(existing);

				if (entry.getState() == OpState.NO_MATCH)
				{
					mTree.put(entry);
					result.set(true);
				}
				else
				{
					aExisting.putAll(readExternalEntry(existing));
				}
			}
		};
		try
		{
			return mExecutor.submit(task, result).get().get();
		}
		catch (InterruptedException | ExecutionException e)
		{
			throw new DatabaseException(e);
		}
	}


	/**
	 * Replace one document in this collection or throws an exception if not found.
	 *
	 * @return a summary of changes
	 */
	public Future<Document> replaceOne(Document aDocument) throws DocumentNotFoundException
	{
		ArrayMapEntry entry = new ArrayMapEntry().setKeyAndValue(aDocument);
		WriteTask task = new WriteTask(this, "replaceOne")
		{
			@Override
			public void call()
			{
				mTree.get(entry);
				if (entry.getState() == OpState.NO_MATCH)
				{
					throw new DocumentNotFoundException(entry.toKeyString());
				}
				writeExternalEntry(entry);
				mTree.put(entry);
				deleteIndexEntries(deleteExternal(entry, true));
			}
		};
		return mExecutor.submit(task, aDocument);
	}


	/**
	 * Replace one document in this collection and return if it was replaced.
	 *
	 * @return if a document was replaced
	 */
	public boolean tryReplaceOne(Document aDocument)
	{
		ArrayMapEntry entry = new ArrayMapEntry().setKeyAndValue(aDocument);

		AtomicBoolean result = new AtomicBoolean(true);
		WriteTask task = new WriteTask(this, "tryReplaceOne")
		{
			@Override
			public void call()
			{
				mTree.get(entry);
				if (entry.getState() == OpState.MATCH)
				{
					throw new DuplicateKeyException();
				}
				writeExternalEntry(entry);
				mTree.put(entry);
				if (entry.getState() == OpState.UPDATE)
				{
					deleteIndexEntries(deleteExternal(entry, true));
				}
				result.set(entry.getState() == OpState.UPDATE);
			}
		};
		try
		{
			return mExecutor.submit(task, result).get().get();
		}
		catch (InterruptedException | ExecutionException e)
		{
			throw new DatabaseException(e);
		}
	}


	/**
	 * Atomically replace documents in this collection and return if all was replaced.
	 *
	 * @return if all was replaced
	 */
	public boolean tryReplaceMany(Document... aDocuments)
	{
		return tryReplaceMany(Arrays.asList(aDocuments));
	}


	/**
	 * Atomically replace documents in this collection and return if all was replaced.
	 *
	 * @return if all was replaced
	 */
	public boolean tryReplaceMany(List<Document> aDocuments)
	{
		AtomicBoolean result = new AtomicBoolean(true);
		WriteTask task = new WriteTask(this, "tryReplaceMany")
		{
			@Override
			public void call()
			{
				for (Document document : aDocuments)
				{
					ArrayMapEntry entry = new ArrayMapEntry().setKeyAndValue(document);
					writeExternalEntry(entry);
					mTree.put(entry);
					if (entry.getState() == OpState.UPDATE)
					{
						deleteIndexEntries(deleteExternal(entry, true));
					}
					else
					{
						result.set(false);
					}
				}
			}
		};
		try
		{
			return mExecutor.submit(task, result).get().get();
		}
		catch (InterruptedException | ExecutionException e)
		{
			throw new DatabaseException(e);
		}
	}


	/**
	 * Atomically replace documents in this collection returning a summary of changes.
	 *
	 * @return a summary of changes
	 */
	public Future<List<Document>> replaceMany(Document... aDocuments)
	{
		return replaceMany(Arrays.asList(aDocuments));
	}


	/**
	 * Atomically replace documents in this collection returning a summary of changes.
	 *
	 * @return a summary of changes
	 */
	public Future<List<Document>> replaceMany(List<Document> aDocuments) throws DocumentNotFoundException
	{
		WriteTask task = new WriteTask(this, "replaceMany")
		{
			@Override
			public void call()
			{
				ArrayList<ArrayMapEntry> entries = new ArrayList<>();
				for (Document document : aDocuments)
				{
					ArrayMapEntry entry = new ArrayMapEntry().setKeyAndValue(document);
					mTree.get(entry);
					if (entry.getState() == OpState.NO_MATCH)
					{
						throw new DocumentNotFoundException();
					}
					entries.add(entry);
				}
				for (ArrayMapEntry entry : entries)
				{
					writeExternalEntry(entry);
					mTree.put(entry);
					deleteIndexEntries(deleteExternal(entry, true));
				}
			}
		};
		return mExecutor.submit(task, aDocuments);
	}


	/**
	 * Delete one document or throws an exception if not found.
	 *
	 * @return a summary of changes
	 */
	public Future<Document> deleteOne(Document aDocument) throws DocumentNotFoundException
	{
		ArrayMapEntry entry = new ArrayMapEntry().setKeyAndValue(aDocument);
		WriteTask task = new WriteTask(this, "deleteOne")
		{
			@Override
			public void call()
			{
				mTree.remove(entry);
				if (entry.getState() != OpState.REMOVED)
				{
					throw new DocumentNotFoundException(entry.getKeyInstance());
				}
				deleteIndexEntries(deleteExternal(entry, true));
			}
		};
		return mExecutor.submit(task, aDocument);
	}


	/**
	 * Delete many documents and return if all was deleted.
	 *
	 * @return if all was deleted
	 */
	public boolean tryDeleteMany(Document... aDocument)
	{
		return tryDeleteMany(Arrays.asList(aDocument));
	}


	/**
	 * Delete many documents and return if all was deleted.
	 *
	 * @return if all was deleted
	 */
	public boolean tryDeleteMany(List<Document> aDocuments)
	{
		AtomicBoolean result = new AtomicBoolean(true);
		WriteTask task = new WriteTask(this, "tryDeleteMany")
		{
			@Override
			public void call()
			{
				for (Document document : aDocuments)
				{
					ArrayMapEntry entry = new ArrayMapEntry().setKeyAndValue(document);
					mTree.remove(entry);
					if (entry.getState() == OpState.REMOVED)
					{
						Document prevDoc = deleteExternal(entry, true);
						if (prevDoc != null)
						{
							deleteIndexEntries(entry.getValueInstance());
						}
					}
					else
					{
						result.set(false);
					}
				}
			}
		};
		try
		{
			return mExecutor.submit(task, result).get().get();
		}
		catch (InterruptedException | ExecutionException e)
		{
			throw new DatabaseException(e);
		}
	}


	/**
	 * Delete one document and return if it was deleted.
	 *
	 * @return if a document was deleted
	 */
	public boolean tryDeleteOne(Document aDocument)
	{
		AtomicBoolean result = new AtomicBoolean();
		WriteTask task = new WriteTask(this, "tryDeleteOne")
		{
			@Override
			public void call()
			{
				ArrayMapEntry entry = new ArrayMapEntry().setKeyAndValue(aDocument);
				mTree.remove(entry);
				if (entry.getState() == OpState.REMOVED)
				{
					deleteIndexEntries(deleteExternal(entry, true));
					result.set(true);
				}
			}
		};
		try
		{
			return mExecutor.submit(task, result).get().get();
		}
		catch (Exception e)
		{
			throw new DatabaseException(e);
		}
	}


	/**
	 * Atomically find a document and then delete it.
	 *
	 * @return the deleted document
	 */
	public Future<Document> findOneAndDelete(Document aDocument)
	{
		ArrayMapEntry entry = new ArrayMapEntry().setKeyAndValue(aDocument);

		WriteTask task = new WriteTask(this, "findOneAndDelete")
		{
			@Override
			public void call()
			{
				mTree.remove(entry);

				if (entry.getState() == OpState.MATCH)
				{
					Document prevDoc = deleteExternal(entry, true);
					if (prevDoc == null)
					{
						prevDoc = entry.getValueInstance();
					}
					aDocument.clear().putAll(prevDoc);
					deleteIndexEntries(prevDoc);
				}
			}
		};
		return mExecutor.submit(task, aDocument);
	}


	/**
	 * Atomically delete many documents returning a summary of changes.
	 *
	 * @return a summary of changes
	 */
	public Future<List<Document>> deleteMany(Document... aDocuments)
	{
		return deleteMany(Arrays.asList(aDocuments));
	}


	/**
	 * Atomically delete many documents returning a summary of changes.
	 *
	 * @return a summary of changes
	 */
	public Future<List<Document>> deleteMany(List<Document> aDocuments)
	{
		WriteTask task = new WriteTask(this, "deleteMany")
		{
			@Override
			public void call()
			{
				ArrayList<ArrayMapEntry> entries = new ArrayList<>();
				for (Document document : aDocuments)
				{
					ArrayMapEntry entry = new ArrayMapEntry().setKeyAndValue(document);
					mTree.get(entry);
					if (entry.getState() == OpState.NO_MATCH)
					{
						throw new DocumentNotFoundException(entry.getKeyInstance());
					}
					entries.add(entry);
				}
				for (ArrayMapEntry entry : entries)
				{
					mTree.remove(entry);
					deleteIndexEntries(deleteExternal(entry, true));
				}
			}
		};
		return mExecutor.submit(task, aDocuments);
	}


	/**
	 * Atomically return all documents in this collection.
	 *
	 * @return all documents
	 */
	public Future<ArrayList<Document>> find()
	{
		ArrayList<Document> result = new ArrayList<>();
		ReadTask task = new ReadTask(this, "find")
		{
			@Override
			public void call()
			{
				mTree.visit(new BTreeVisitor(){
					@Override
					public boolean leaf(BTreeLeafNode aNode)
					{
						aNode.forEachEntry(e -> result.add(readExternalEntry(e)));
						return true;
					}
				});
			}
		};
		return mExecutor.submit(task, result);
	}


//	public ArrayList<Document> find(Document aFilter) throws InterruptedException, ExecutionException
//	{
//		log.i("find {}", aFilter);
//		log.inc();
//
////		System.out.println(aQuery);
////		System.out.println(mDatabase.mIndices);
//		Document bestIndex = null;
//		int matchingFields = 0;
//		ArrayList<String> queryKeys = aFilter.keySet();
//
//		HashMap<ObjectId, Document> indices = mDatabase.mIndices.get(getCollectionId());
//		if (indices != null)
//		{
//			for (Document conf : indices.values())
//			{
//				ArrayList<String> indexKeys = conf.getDocument("fields").keySet();
//
//				int i = 0;
//				for (; i < Math.min(indexKeys.size(), queryKeys.size()) && indexKeys.get(i).equals(queryKeys.get(i)); i++)
//				{
//				}
//
//				if (i > matchingFields || i == matchingFields && bestIndex != null && indexKeys.size() < conf.getDocument("fields").size())
//				{
//					bestIndex = conf;
//					matchingFields = i;
//				}
//			}
//		}
//
////		System.out.println(matchingFields+" "+queryKeys.size());
////		System.out.println(bestIndex);
//		if (bestIndex != null)
//		{
//			System.out.println("INDEX: " + bestIndex);
//
//			return mDatabase.getCollection("index:" + bestIndex.getArray("_id")).findImpl(aFilter);
//		}
//
//		return findImpl(aFilter);
//	}


	/**
	 * Return the number of documents in this collection.
	 *
	 * @return documents in the collection
	 */
	public Future<AtomicLong> size()
	{
		AtomicLong result = new AtomicLong();
		ReadTask task = new ReadTask(this, "size")
		{
			@Override
			public void call()
			{
				result.set(mTree.size());
			}
		};
		return mExecutor.submit(task, result);
	}


	/**
	 * Returning a live iterator over all documents in this collection.
	 * <p>
	 * The iterator will reflect changes made to the collection while being iterated.
	 * </p>
	 * @return an iterator over all documents in this collection
	 */
	@Override
	public Iterator<Document> iterator()
	{
		// fix this
		try
		{
			size().get();
		}
		catch (Exception e)
		{
			e.printStackTrace(System.out);
		}

		return new BTreeIterator(mTree)
		{
//			@Override
//			protected Document unmarshal(ArrayMapEntry aEntry)
//			{
//				return unmarshalDocument(aEntry);
//			}
			@Override
			protected void remove(ArrayMapEntry aEntry)
			{
				mTree.remove(aEntry);
				if (aEntry.getState() == OpState.REMOVED)
				{
					deleteIndexEntries(deleteExternal(aEntry, true));
				}
			}
		};
	}


	public <K extends Document> Stream<Document> stream()
	{
		return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator(), 0), false);
	}


	/**
	 * Returning a live iterator over all keys in this collection. Only the _id field will be populated in each document.
	 * <p>
	 * The iterator will reflect changes made to the collection while being iterated.
	 * </p>
	 * @return an iterator over all keys in this collection
	 */
	public Iterable<Document> keys()
	{
		return () ->  new BTreeIterator(mTree)
		{
//			@Override
//			protected Document unmarshal(ArrayMapEntry aEntry)
//			{
//				return mDocumentSupplier.get().put("_id", aEntry.getKey().get());
//			}
			@Override
			protected void remove(ArrayMapEntry aEntry)
			{
				mTree.remove(aEntry);
				if (aEntry.getState() == OpState.REMOVED)
				{
					deleteIndexEntries(deleteExternal(aEntry, true));
				}
			}
		};
	}


	/**
	 * Iterate live over all documents in this collection.
	 * <p>
	 * The iterator will reflect changes made to the collection while being iterated.
	 * </p>
	 */
	public void foreach(Consumer<Document> aConsumer)
	{
        for (Document doc : this)
		{
            aConsumer.accept(doc);
        }
    }


	/**
	 * Delete all documents in this collection.
	 */
	public Future<DropResult> drop() throws IOException, InterruptedException, ExecutionException
	{
		DropResult result = new DropResult();
		WriteTask task = new WriteTask(this, "drop")
		{
			@Override
			public void call()
			{
				try
				{
					mTree.drop(e -> deleteExternal(e, false));
					mDatabase.removeCollectionImpl(RaccoonCollection.this);
				}
				catch (IOException | InterruptedException | ExecutionException e)
				{
					throw new IllegalStateException(e);
				}
				mDatabase = null;
				mTree = null;
				mTree = null;
			}
		};
		return mExecutor.submit(task, result);
	}


	void close() throws InterruptedException
	{
		try
		{
			mExecutor.shutdown();
			mExecutor.awaitTermination(1, TimeUnit.HOURS);
		}
		finally
		{
			mExecutor = null;
		}
		mTree.close();
		mTree = null;
	}


//	boolean isChanged()
//	{
//		return mTree.isChanged();
//	}


	public Future<String> flush()
	{
		WriteTask task = new WriteTask(this, "flush")
		{
			@Override
			public void call()
			{
				mTree.flush();
			}
		};
		return mExecutor.submit(task, "flush");
	}


	Future<String> rollback()
	{
		WriteTask task = new WriteTask(this, "rollback")
		{
			@Override
			public void call()
			{
				mTree.rollback();
			}
		};
		return mExecutor.submit(task, "rollback");
	}


	/**
	 * Commit changes to the block device returning a summary of changes.
	 *
	 * @return a summary of changes
	 */
	public Future<CommitResult> commit()
	{
		return commit(() ->
		{
		});
	}


	Future<CommitResult> commit(Runnable aOnModifiedAction)
	{
		CommitResult result = new CommitResult();
		WriteTask task = new WriteTask(this, "commit")
		{
			@Override
			public void call()
			{
				if (mTree.commit())
				{
					aOnModifiedAction.run();
				}
			}
		};
		return mExecutor.submit(task, result);
	}


	BTree _getImplementation()
	{
		return mTree;
	}


	BTreeConfiguration getConfiguration()
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


//	private void insertOrUpdate(ArrayMapEntry aEntry, boolean aPermitInsert, boolean aPermitUpdate)
//	{
//		if (aPermitInsert && !aPermitUpdate)
//		{
//			mTree.get(aEntry);
//
//			if (aEntry.getState() == OpState.MATCH)
//			{
//				throw new DuplicateKeyException();
//			}
//		}
//
//		writeExternalEntry(aEntry);
//		mTree.put(aEntry);
//
//		if (aEntry.getState() == OpState.UPDATE)
//		{
//			Document prev = deleteExternal(aEntry, true);
//
//			deleteIndexEntries(prev);
//		}
//
////		for (Document indexConf : mDatabase.mIndices.values(getCollectionId()))
////		{
////			boolean unique = indexConf.getDocument("configuration").get("unique", false);
////			boolean clone = indexConf.getDocument("configuration").get("clone", false);
////
////			ArrayList<Array> result = new ArrayList<>();
////			generatePermutations(indexConf, aEntry, new Array(), 0, result);
////			for (Array values : result)
////			{
////				Document indexEntry = new Document().put("_id", values);
////
////				if (unique)
////				{
////					indexEntry.put("_ref", aEntry.getKey());
////
////					try
////					{
////						Document existing = getIndexByConf(indexConf).findOne(new Document().put("_id", values)).get();
////
////						if (existing != null && !aEntry.getKey().equals(existing.get("_ref")))
////						{
////							throw new UniqueConstraintException("Collection index <" + indexConf.getObjectId("_id") + ">, existing ID <" + existing.get("_ref") + ">, saving ID <" + aEntry.get("_id") + ">, values " + values.toJson());
////						}
////					}
////					catch (InterruptedException | ExecutionException | UniqueConstraintException e)
////					{
////						e.printStackTrace(System.out);
////					}
////				}
////				else
////				{
////					values.add(aEntry.getKey());
////				}
////
////				if (clone)
////				{
////					indexEntry.put("_clone", aEntry);
////				}
////
////				getIndexByConf(indexConf).saveOne(indexEntry);
////			}
////		}
//	}


//	private ArrayList<Document> findImpl(Document aFilter) throws InterruptedException, ExecutionException
//	{
//		ArrayList<Document> result = new ArrayList<>();
//		ReadTask task = new ReadTask(this, "size")
//		{
//			@Override
//			public void call()
//			{
//				mTree.find(result, aFilter, entry -> unmarshalDocument(entry, mDocumentSupplier.get()));
//			}
//		};
//		return mExecutor.submit(task, result).get();
//	}

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

//	public ScanResult _getStats()
//	{
//		ScanResult scanResult = new ScanResult();
//		new _Scanner().scan(mTree, scanResult);
//		System.out.println(scanResult);
//
//		return scanResult;
////		return Document.of("depth:0, nodes:0, leafs:0, documents:0, externals:0, logicalSize:0, physicalSize:0, nodeFill: 0, leafFill: 0, nodeDegree: 0, pendingWrites:0, cacheSize:0");
//	}


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

		mDatabase.mIndices.put(id, indexConf);

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
			for (Object value : aDocument.findMany(confs.keySet().toArray()[aPosition].toString()))
			{
				Array tmp = aIndexValues.clone();
				tmp.add(value);
				generatePermutations(aIndexConf, aDocument, tmp, aPosition + 1, aResult);
			}
		}
	}


	private void deleteIndexEntries(Document aPrevDoc)
	{
//		for (Document indexConf : mDatabase.mIndices.values(getCollectionId()))
//		{
////			ArrayList<Array> result = new ArrayList<>();
////			generatePermutations(indexConf, aPrevDoc, new Array(), 0, result);
////			for (Array values : result)
////			{
////			}
//
//			try
//			{
//				getIndexByConf(indexConf).forEach(doc ->
//				{
////					if (doc.get("_ref").equals(aPrevDoc.get("_id")))
//					{
//						getIndexByConf(indexConf).findOneAndDelete(doc);
//					}
//				});
//			}
//			catch (Exception e)
//			{
//				e.printStackTrace(System.out);
//			}
//		}
	}


	RaccoonCollection getIndexByConf(Document aIndexConf)
	{
		return mDatabase.getCollection("index:" + aIndexConf.getArray("_id"));
	}


//	private Document unmarshalDocument(ArrayMapEntry aEntry)
//	{
//		if (aEntry.getValueType() == Type.BLOCKPOINTER)
//		{
//			RuntimeDiagnostics.collectStatistics(Operation.READ_EXT, 1);
//			return new Document().fromByteArray(mDatabase.getBlockAccessor().readBlock(aEntry.getValueInstance()));
//		}
//
//		return aEntry.getValueInstance();
//	}


	private Document deleteExternal(ArrayMapEntry aEntry, boolean aRestoreOldValue)
	{
		Document prev = null;

		if (aEntry != null)
		{
			if (aEntry.getValueType() == Type.BLOCKPOINTER)
			{
				BlockPointer bp = aEntry.getValueInstance();

				if (aRestoreOldValue)
				{
					RuntimeDiagnostics.collectStatistics(Operation.READ_EXT, 1);
					prev = mDocumentSupplier.get().fromByteArray(mDatabase.getBlockAccessor().readBlock(aEntry.getValueInstance()));
				}

				RuntimeDiagnostics.collectStatistics(Operation.FREE_EXT, 1);
				mDatabase.getBlockAccessor().freeBlock(bp);
			}
			else
			{
				prev = aEntry.getValueInstance();
			}
		}

		return prev;
	}


	private Document readExternalEntry(ArrayMapEntry aEntry)
	{
		Document prev = null;

		if (aEntry != null)
		{
			if (aEntry.getValueType() == Type.BLOCKPOINTER)
			{
				RuntimeDiagnostics.collectStatistics(Operation.READ_EXT, 1);
				prev = mDocumentSupplier.get().fromByteArray(mDatabase.getBlockAccessor().readBlock(aEntry.getValueInstance()));
			}
			else
			{
				prev = aEntry.getInstance();
			}
		}

		return prev;
	}


	private void writeExternalEntry(ArrayMapEntry aEntry)
	{
		if (aEntry.getMarshalledLength() > mTree.getConfiguration().getLimitEntrySize())
		{
			RuntimeDiagnostics.collectStatistics(Operation.WRITE_EXT, 1);
			aEntry.setValueInstance(mDatabase.getBlockAccessor().writeBlock(aEntry.getValue(), BlockType.EXTERNAL, 0, CompressorAlgorithm.LZJB.ordinal()));
		}
	}


	public Future<String> printTree()
	{
		WriteTask task = new WriteTask(this, "printTree")
		{
			@Override
			public void call()
			{
				mTree.printTree();
			}
		};
		return mExecutor.submit(task, null);
	}


	protected void createKeys(Document aDocument)
	{
		if (!aDocument.containsKey("_id"))
		{
			aDocument.put("_id", mKeySupplier.get());
		}
	}


	public Map<ObjectId, Document> toMap()
	{
		try
		{
			return find().get().stream().collect(Collectors.toMap(d -> d.getObjectId("_id"), Function.identity()));
		}
		catch (InterruptedException | ExecutionException e)
		{
			throw new DatabaseException(e);
		}
	}
}
