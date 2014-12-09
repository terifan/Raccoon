package org.terifan.v1.security;


/**
 * CipherMode defines how CipherInputStream and CipherOutputStream process data.
 */
public enum CipherMode
{
	/** Electronic Code Book */
	ECB,
	/** Cipher Block Chaining */
	CBC;
	/* * Error-Propagating Block Chaining */
	//EPBC;
}