package org.terifan.raccoon;


public enum OpenOption
{
	/**
	 * Open an already existing file or abort if file not found
	 */
	OPEN,
	/**
	 * Open an already existing file or create a file if none exists
	 */
	CREATE,
	/**
	 * Create a new file replacing any existing file
	 */
	CREATE_NEW,
	/**
	 * Open an already existing file in read-only mode or abort if file not found
	 */
	READ_ONLY
}