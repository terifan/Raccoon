package org.terifan.raccoon;

import java.util.concurrent.atomic.AtomicReference;
import org.terifan.raccoon.ScanResult;
import org.terifan.raccoon.BTreeNodeVisitor.CancelVisitor;
import org.terifan.raccoon.util.Log;


public class BTreeScanner
{
	public static String integrityCheck(BTree aImplementation)
	{
		Log.i("integrity check");

		AtomicReference<String> result = new AtomicReference<>();

		new BTreeNodeVisitor().visitAll(aImplementation, aNode ->
		{
			String tmp = aNode.mMap.integrityCheck();
			if (tmp != null)
			{
				result.set(tmp);
				throw new CancelVisitor(null);
			}
		});

		return result.get();
	}


	public static ScanResult scan(BTree aImplementation, ScanResult aScanResult)
	{
		aScanResult.tables++;

		scan(aImplementation, aImplementation.getRoot(), aScanResult, 0);

		return aScanResult;
	}


	private static void scan(BTree aImplementation, BTreeNode aNode, ScanResult aScanResult, int aLevel)
	{
		if (aNode instanceof BTreeIndex)
		{
			BTreeIndex indexNode = (BTreeIndex)aNode;

			int fillRatio = indexNode.mMap.getUsedSpace() * 100 / aImplementation.getConfiguration().getInt("indexSize");
			aScanResult.log.append("{" + (aNode.mBlockPointer == null ? "" : aNode.mBlockPointer.getBlockIndex0()) + ":" + fillRatio + "%" + "}");

			boolean first = true;
			aScanResult.log.append("'");
			for (ArrayMapEntry entry : indexNode.mMap)
			{
				if (!first)
				{
					aScanResult.log.append(":");
				}
				first = false;
				String s = new String(entry.getKey()).replaceAll("[^\\w]*", "").replace("'", "").replace("_", "");
				aScanResult.log.append(s.isEmpty() ? "*" : s);
			}
			aScanResult.log.append("'");

			if (indexNode.mMap.size() == 1)
			{
				aScanResult.log.append("#000#ff0#000");
			}
			else if (fillRatio > 100)
			{
				aScanResult.log.append(indexNode.mModified ? "#a00#a00#fff" : "#666#666#fff");
			}
			else
			{
				aScanResult.log.append(indexNode.mModified ? "#f00#f00#fff" : "#888#fff#000");
			}

			first = true;
			aScanResult.log.append("[");

			for (int i = 0, sz = indexNode.mMap.size(); i < sz; i++)
			{
				if (!first)
				{
					aScanResult.log.append(",");
				}
				first = false;

				BTreeNode child = indexNode.getNode(aImplementation, i);

				ArrayMapEntry entry = new ArrayMapEntry();
				indexNode.mMap.get(i, entry);
				indexNode.mChildNodes.put(new MarshalledKey(entry.getKey()), child);

				scan(aImplementation, child, aScanResult, aLevel + 1);
			}

			aScanResult.log.append("]");
		}
		else
		{
			int fillRatio = aNode.mMap.getUsedSpace() * 100 / aImplementation.getConfiguration().getInt("leafSize");

			aScanResult.log.append("{" + (aNode.mBlockPointer == null ? "" : aNode.mBlockPointer.getBlockIndex0()) + ":" + fillRatio + "%" + "}");
			aScanResult.log.append("[");

			boolean first = true;

			for (ArrayMapEntry entry : aNode.mMap)
			{
				if (!first)
				{
					aScanResult.log.append(",");
				}
				first = false;
				aScanResult.log.append("'" + new String(entry.getKey()).replaceAll("[^\\w]*", "").replace("_", "") + "'");
			}

			aScanResult.log.append("]");

			if (fillRatio > 100)
			{
				aScanResult.log.append(aNode.mModified ? "#a00#a00#fff" : "#666#666#fff");
			}
			else
			{
				aScanResult.log.append(aNode.mModified ? "#f00#f00#fff" : "#888#fff#000");
			}
		}
	}
}
