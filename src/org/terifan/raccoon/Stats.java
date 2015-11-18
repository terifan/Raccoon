package org.terifan.raccoon;


public class Stats 
{
	public static volatile int blockRead;
	public static volatile int blockWrite;
	public static volatile int blockAlloc;
	public static volatile int blockFree;
	public static volatile int splitLeaf;
	public static volatile int upgradeHoleToLeaf;
	public static volatile int removeValue;
	public static volatile int putValueLeaf;
	public static volatile int putValue;
	public static volatile int getValue;
	public static volatile int pointerEncode;
	public static volatile int pointerDecode;
	public static volatile int indexNodeCreation;
	public static volatile int leafNodeCreation;

	
	public static void clear()
	{
		blockRead = 0;
		blockWrite = 0;
		blockAlloc = 0;
		blockFree = 0;
		splitLeaf = 0;
		upgradeHoleToLeaf = 0;
		removeValue = 0;
		putValueLeaf = 0;
		putValue = 0;
		getValue = 0;
		pointerEncode = 0;
		pointerDecode = 0;
		indexNodeCreation = 0;
		leafNodeCreation = 0;
	}
	
	
	public static String print()
	{
		return "blockAlloc=" + blockAlloc
			+ ", blockFree=" + blockFree
			+ ", blockRead=" + blockRead
			+ ", blockWrite=" + blockWrite
			+ ", splitLeaf=" + splitLeaf
			+ ", upgradeHoleToLeaf=" + upgradeHoleToLeaf
			+ ", removeValue=" + removeValue
			+ ", putValueLeaf=" + putValueLeaf
			+ ", putValue=" + putValue
			+ ", getValue=" + getValue
			+ ", pointerEncode=" + pointerEncode
			+ ", pointerDecode=" + pointerDecode
			+ ", indexNodeCreation=" + indexNodeCreation
			+ ", leafNodeCreation=" + leafNodeCreation;
	}
}
