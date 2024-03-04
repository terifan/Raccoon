package org.terifan.raccoon.btree;


public enum OpState
{
	MATCH,
	NO_MATCH,
	INSERT,
	UPDATE,
	DELETE,
	OVERFLOW
}
