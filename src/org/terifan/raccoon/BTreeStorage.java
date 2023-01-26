package org.terifan.raccoon;

import org.terifan.raccoon.storage.BlockAccessor;


public abstract class BTreeStorage
{
	abstract BlockAccessor getBlockAccessor();
}
