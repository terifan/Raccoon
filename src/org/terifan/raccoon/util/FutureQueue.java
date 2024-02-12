package org.terifan.raccoon.util;

import java.util.ArrayList;
import java.util.concurrent.Future;


public class FutureQueue extends ArrayList<Future> implements AutoCloseable
{
	@Override
	public void close() throws Exception
	{
		for (Future f : this)
		{
			f.get();
		}
	}
}
