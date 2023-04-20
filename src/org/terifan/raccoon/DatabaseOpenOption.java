package org.terifan.raccoon;


public enum DatabaseOpenOption
{
	/**
	 * Open an already existing file.
	 */
	OPEN,
	/**
	 * Open an already existing file or create a new file if none exists.
	 */
	CREATE,
	/**
	 * Create a new file replacing any existing file.
	 */
	REPLACE,
	/**
	 * Open an already existing file in read-only mode.
	 */
	READ_ONLY
}