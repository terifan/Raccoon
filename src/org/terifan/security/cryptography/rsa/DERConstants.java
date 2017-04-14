package org.terifan.security.cryptography.rsa;


interface DERConstants
{
	// Classes
	public final static int UNIVERSAL = 0x00;
	public final static int APPLICATION = 0x40;
	public final static int CONTEXT = 0x80;
	public final static int PRIVATE = 0xC0;

	// Constructed Flag
	public final static int CONSTRUCTED = 0x20;

	// Tag and data types
	public final static int ANY = 0x00;
	public final static int BOOLEAN = 0x01;
	public final static int INTEGER = 0x02;
	public final static int BIT_STRING = 0x03;
	public final static int OCTET_STRING = 0x04;
	public final static int NULL = 0x05;
	public final static int OBJECT_IDENTIFIER = 0x06;
	public final static int REAL = 0x09;
	public final static int ENUMERATED = 0x0a;
	public final static int RELATIVE_OID = 0x0d;

	public final static int SEQUENCE = 0x10;
	public final static int SET = 0x11;

	public final static int NUMERIC_STRING = 0x12;
	public final static int PRINTABLE_STRING = 0x13;
	public final static int T61_STRING = 0x14;
	public final static int VIDEOTEX_STRING = 0x15;
	public final static int IA5_STRING = 0x16;
	public final static int GRAPHIC_STRING = 0x19;
	public final static int ISO646_STRING = 0x1A;
	public final static int GENERAL_STRING = 0x1B;

	public final static int UTF8_STRING = 0x0C;
	public final static int UNIVERSAL_STRING = 0x1C;
	public final static int BMP_STRING = 0x1E;

	public final static int UTC_TIME = 0x17;
	public final static int GENERALIZED_TIME = 0x18;
}
