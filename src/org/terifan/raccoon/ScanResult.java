package org.terifan.raccoon;


public class ScanResult
{
	int tables;
	int records;
	int indexBlocks;
	int blobs;
	int blobIndices;
	int blobData;


	@Override
	public String toString()
	{
		return "ScanResult{" + "tables=" + tables + ", records=" + records + ", indexBlocks=" + indexBlocks + ", blobs=" + blobs + ", blobIndices=" + blobIndices + ", blobData=" + blobData + '}';
	}
}
