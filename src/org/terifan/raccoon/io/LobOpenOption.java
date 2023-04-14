package org.terifan.raccoon.io;


public enum LobOpenOption
{
	/**
	 * Open for read-only access.
	 */
	READ,
	/**
	 * Open for write access, bytes will be written to the beginning of the blob. This mode also allow reading.
	 */
	WRITE,
	/**
	 * Open for write access, bytes will be written to the end of the blob rather than the beginning. This mode also allow reading.
	 */
	APPEND,
	/**
	 * Create a new blob removing existing content if it already exists. This mode also allow reading.
	 */
	REPLACE,
	/**
	 * Create a new blob, failing if the blob already exists. This mode also allow reading.
	 */
	CREATE
}
