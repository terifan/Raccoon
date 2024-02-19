package org.terifan.raccoon;

import org.terifan.raccoon.exceptions.LobNotFoundException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.terifan.raccoon.blockdevice.lob.LobAccessException;
import org.terifan.raccoon.blockdevice.lob.LobByteChannel;
import org.terifan.raccoon.blockdevice.lob.LobConsumer;
import org.terifan.raccoon.blockdevice.lob.LobOpenOption;
import org.terifan.raccoon.document.Document;
import org.terifan.raccoon.document.ObjectId;


public class RaccoonDirectory<K>
{
	private final RaccoonCollection mCollection;
	private final RaccoonDatabase mDatabase;


	RaccoonDirectory(RaccoonDatabase aDatabase, RaccoonCollection aCollection)
	{
		mCollection = aCollection;
		mDatabase = aDatabase;
	}


	RaccoonCollection getCollection()
	{
		return mCollection;
	}


	public LobByteChannel open(K aId, LobOpenOption aLobOpenOption) throws IOException, InterruptedException, ExecutionException
	{
		return open(aId, aLobOpenOption, null);
	}


	public LobByteChannel open(K aId, LobOpenOption aLobOpenOption, Document aOptions) throws IOException, InterruptedException, ExecutionException
	{
		LobByteChannel lob = tryOpen(aId, aLobOpenOption, aOptions);

		if (lob == null)
		{
			throw new LobNotFoundException(aId);
		}

		return lob;
	}


	public LobByteChannel tryOpen(K aId, LobOpenOption aLobOpenOption) throws IOException, InterruptedException, ExecutionException
	{
		return tryOpen(aId, aLobOpenOption, null);
	}


	public LobByteChannel tryOpen(K aId, LobOpenOption aLobOpenOption, Document aOptions) throws IOException, InterruptedException, ExecutionException
	{
		Document entry = new Document().put("_id", aId);

		if (!mCollection.tryFindOne(entry) && aLobOpenOption == LobOpenOption.READ)
		{
			return null;
		}

		return new LobByteChannel(mCollection.getBlockAccessor(), entry, aLobOpenOption, aOptions).setCloseAction(ch -> mCollection.saveOne(entry));
	}


	public void delete(K aId) throws IOException, InterruptedException, ExecutionException
	{
		Document entry = new Document().put("_id", aId);

		if (!mCollection.tryFindOne(entry))
		{
			throw new FileNotFoundException(aId.toString());
		}

		LobByteChannel lob = tryOpen(aId, LobOpenOption.REPLACE);
		if (lob != null)
		{
			lob.delete();
		}

		mCollection.deleteOne(entry);
	}


	public byte[] readAllBytes(K aObjectId) throws IOException, InterruptedException, ExecutionException
	{
		return readAllBytes(aObjectId, null);
	}


	public byte[] readAllBytes(K aObjectId, Document oMetadata) throws IOException, InterruptedException, ExecutionException
	{
		try (LobByteChannel channel = open(aObjectId, LobOpenOption.READ))
		{
			Document metadata = channel.getMetadata();
			if (oMetadata != null)
			{
				oMetadata.putAll(metadata);
			}
			return channel.readAllBytes();
		}
		catch (IOException e)
		{
			throw new LobAccessException(e);
		}
	}


	public void writeAllBytes(K aObjectId, byte[] aContent, Document aMetadata) throws IOException, InterruptedException, ExecutionException
	{
		try (LobByteChannel channel = open(aObjectId, LobOpenOption.REPLACE))
		{
			if (aMetadata != null)
			{
				channel.getMetadata().putAll(aMetadata);
			}
			channel.writeAllBytes(aContent);
		}
		catch (IOException e)
		{
			throw new LobAccessException(e);
		}
	}


	public void forEach(LobConsumer aConsumer) throws IOException, InterruptedException, ExecutionException
	{
//		mCollection.listAll().forEach(e -> aConsumer.accept(e.get("_id"), e.getDocument(LobByteChannel.IX_METADATA)));
		mCollection.forEach(e -> aConsumer.accept(e.get("_id"), e.getDocument("7")));
	}


	public List<ObjectId> list() throws IOException, InterruptedException, ExecutionException
	{
		ArrayList<ObjectId> list = new ArrayList<>();
		mCollection.forEach(e -> list.add(e.getObjectId("_id")));
		return list;
	}


	public Future<AtomicLong> size()
	{
		return mCollection.size();
	}


	public void drop() throws IOException, InterruptedException, ExecutionException
	{
		if (size().get().get() != 0)
		{
			throw new IllegalStateException("The directory is not empty.");
		}

		mDatabase.removeDirectoryImpl(this);
	}
}
