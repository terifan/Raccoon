package org.terifan.raccoon.core;


public class ScanResult
{
	public int tables;
	public int records;
	public int indexBlocks;
	public int blobs;
	public int blobIndices;
	public int blobData;


	@Override
	public String toString()
	{
		return "ScanResult{" + "tables=" + tables + ", records=" + records + ", indexBlocks=" + indexBlocks + ", blobs=" + blobs + ", blobIndices=" + blobIndices + ", blobData=" + blobData + '}';
	}
}
