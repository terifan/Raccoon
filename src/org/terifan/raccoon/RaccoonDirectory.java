package org.terifan.raccoon;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.terifan.raccoon.blockdevice.LobAccessException;
import org.terifan.raccoon.blockdevice.LobByteChannel;
import org.terifan.raccoon.blockdevice.LobConsumer;
import org.terifan.raccoon.blockdevice.LobOpenOption;
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


	public LobByteChannel open(K aId, LobOpenOption aLobOpenOption) throws IOException
	{
		return open(aId, aLobOpenOption, null);
	}


	public LobByteChannel open(K aId, LobOpenOption aLobOpenOption, Document aOptions) throws IOException
	{
		LobByteChannel lob = tryOpen(aId, aLobOpenOption, aOptions);

		if (lob == null)
		{
			throw new LobNotFoundException(aId);
		}

		return lob;
	}


	public LobByteChannel tryOpen(K aId, LobOpenOption aLobOpenOption) throws IOException
	{
		return tryOpen(aId, aLobOpenOption, null);
	}


	public LobByteChannel tryOpen(K aId, LobOpenOption aLobOpenOption, Document aOptions) throws IOException
	{
		Document entry = new Document().put("_id", aId);

		if (!mCollection.tryFindOne(entry) && aLobOpenOption == LobOpenOption.READ)
		{
			return null;
		}

		return new LobByteChannel(mCollection.getBlockAccessor(), entry, aLobOpenOption, aOptions).setCloseAction(ch -> mCollection.saveOne(entry));
	}


	public void delete(K aId) throws IOException
	{
		Document entry = new Document().put("_id", aId);

		if (!mCollection.tryFindOne(entry))
		{
			throw new FileNotFoundException(aId.toString());
		}

		LobByteChannel lob = tryOpen(aId, LobOpenOption.READ);
		if (lob != null)
		{
			lob.delete();
		}

		mCollection.deleteOne(entry);
	}


	public byte[] readAllBytes(K aObjectId)
	{
		return readAllBytes(aObjectId, null);
	}


	public byte[] readAllBytes(K aObjectId, Document oMetadata)
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


	public void writeAllBytes(K aObjectId, byte[] aContent, Document aMetadata)
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


	public void forEach(LobConsumer aConsumer) throws IOException
	{
//		mCollection.listAll().forEach(e -> aConsumer.accept(e.get("_id"), e.getDocument(LobByteChannel.IX_METADATA)));
		mCollection.find().forEach(e -> aConsumer.accept(e.get("_id"), e.getDocument("7")));
	}


	public List<ObjectId> list() throws IOException
	{
		return (List<ObjectId>)mCollection.find().stream().map(e -> e.getObjectId("_id")).collect(Collectors.toList());
	}


	public long size()
	{
		return mCollection.size();
	}


	public void drop()
	{
		if (size() != 0)
		{
			throw new IllegalStateException("The directory is not empty.");
		}

		mDatabase.removeDirectoryImpl(this);
	}
}
