package org.terifan.raccoon;

import org.terifan.raccoon.document.Document;
import org.terifan.raccoon.io.managed.IManagedBlockDevice;
import org.terifan.raccoon.storage.BlockAccessor;


public abstract class BTreeStorage implements AutoCloseable
{
	abstract BlockAccessor getBlockAccessor();
}
