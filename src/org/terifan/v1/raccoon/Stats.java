package org.terifan.v1.raccoon;


public class Stats 
{
	public static int blockRead;
	public static int blockWrite;
	public static int blockAlloc;
	public static int blockFree;
	public static int splitLeaf;
	public static int upgradeHoleToLeaf;
	public static int removeValue;
	public static int putValueLeaf;
	public static int putValue;
	public static int getValue;
	public static int pointerEncode;
	public static int pointerDecode;
	public static int indexNodeCreation;
	public static int leafNodeCreation;

	
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
