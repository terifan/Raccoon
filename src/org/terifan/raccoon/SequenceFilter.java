package org.terifan.raccoon;


public interface SequenceFilter<T>
{
	Sequence<T> query(Query aQuery);
}
