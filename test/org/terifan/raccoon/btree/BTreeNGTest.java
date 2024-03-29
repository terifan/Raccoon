package org.terifan.raccoon.btree;

import java.util.Map;
import org.terifan.raccoon.document.Document;
import org.terifan.raccoon.blockdevice.BlockAccessor;
import org.testng.annotations.Test;
import org.terifan.raccoon.blockdevice.storage.MemoryBlockStorage;
import static org.terifan.raccoon._Tools.createSecureStorage;
import static org.testng.Assert.*;
import static org.terifan.raccoon._Tools.doc;


public class BTreeNGTest
{
//	@Test
//	public void testOpenCloseSecureBTree() throws Exception
//	{
//		Map<Long, byte[]> data;
//
//		_ArrayMapKey key = new _ArrayMapKey("key");
//		Document value = doc(5);
//
//		try (MemoryBlockStorage device = new MemoryBlockStorage(512))
//		{
//			try (BlockAccessor storage = createSecureStorage(device); BTree tree = new BTree(storage, new BTreeConfiguration()))
//			{
//				tree.put(new _ArrayMapEntry(key, value, TYPE_DOCUMENT));
//				tree.commit();
//				storage.getBlockDevice().getMetadata().put("conf", tree.getConfiguration());
//				storage.getBlockDevice().commit();
//
//				System.out.println(tree.getConfiguration());
//			}
//			data = device.getStorage();
//		}
//
//		System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
//
//		try (MemoryBlockStorage device = new MemoryBlockStorage(512, data))
//		{
//			try (BlockAccessor storage = createSecureStorage(device); BTree tree = new BTree(storage, new BTreeConfiguration(storage.getBlockDevice().getMetadata().getDocument("conf"))))
//			{
//				ArrayMapEntry entry = tree.get(key);
//				assertEquals(entry.mState, OpState.MATCH);
//				assertEquals(entry.entry.getValue(), value);
//				storage.getBlockDevice().commit();
//			}
//		}
//	}


//	@Test
//	public void testShrink() throws Exception
//	{
//		Document value = doc(5);
//
//		try (BlockStorage device = new MemoryBlockStorage(512))
//		{
//			try (BlockAccessor storage = createSecureStorage(device); BTree tree = new BTree(storage, BTree.createDefaultConfig(512)))
//			{
//				tree.put(new ArrayMapEntry(new ArrayMapKey("key1"), Document.of("text:something"), TYPE_DOCUMENT));
//				tree.put(new ArrayMapEntry(new ArrayMapKey("key2"), Document.of("text:something"), TYPE_DOCUMENT));
//				tree.put(new ArrayMapEntry(new ArrayMapKey("key3"), Document.of("text:something"), TYPE_DOCUMENT));
//				tree.put(new ArrayMapEntry(new ArrayMapKey("key4"), Document.of("text:something"), TYPE_DOCUMENT));
//				tree.put(new ArrayMapEntry(new ArrayMapKey("key5"), Document.of("text:something"), TYPE_DOCUMENT));
//				tree.put(new ArrayMapEntry(new ArrayMapKey("key6"), Document.of("text:something"), TYPE_DOCUMENT));
//				tree.put(new ArrayMapEntry(new ArrayMapKey("key7"), Document.of("text:something"), TYPE_DOCUMENT));
//				tree.put(new ArrayMapEntry(new ArrayMapKey("key8"), Document.of("text:something"), TYPE_DOCUMENT));
//
//				ScanResult scanResult = new ScanResult();
//				tree.scan(scanResult);
//				System.out.println(scanResult.log);
//
//				// {key1,key2,key3,key4,key5,key6,key7,key8}
//				// {*,key5}{key1,key2,key3,key4},{key5,key6,key7,key8}
//				// {*,key3,key5}{key1,key2},{key3,key4},{key5,key6,key7,key8}
//				// {*,key3,key5,key7}{key1,key2},{key3,key4},{key5,key6},{key7,key8}
//				// {*,key5}{*,key3},{*,key7}{key1,key2},{key3,key4},{key5,key6},{key7,key8}
//
//				tree._upgrade();
//
//				scanResult = new ScanResult();
//				tree.scan(scanResult);
//				System.out.println(scanResult.log);
//
//				tree._grow();
//
//				scanResult = new ScanResult();
//				tree.scan(scanResult);
//				System.out.println(scanResult.log);
//
////				tree._shrink();
////
////				scanResult = new ScanResult();
////				tree.scan(scanResult);
////				System.out.println(scanResult.log);
////
////				tree._downgrade();
////
////				scanResult = new ScanResult();
////				tree.scan(scanResult);
////				System.out.println(scanResult.log);
//
//				tree.commit();
//				storage.getBlockDevice().getMetadata().put("conf", tree.getConfiguration());
//				storage.getBlockDevice().commit();
//			}
//		}


//		Logger.getLogger().setLevel(Level.ALL);
//
//		MemoryBlockStorage storage = new MemoryBlockStorage(512);
//		BlockAccessor blockAccessor = new BlockAccessor(new ManagedBlockDevice(storage));
//
//		BTree tree = new BTree(blockAccessor, BTree.createDefaultConfig(512));
//		BTreeInteriorNode node = new BTreeInteriorNode(tree, 2, new ArrayMap(4096));
//		BTreeInteriorNode child1 = new BTreeInteriorNode(tree, 1, new ArrayMap(4096));
//		BTreeInteriorNode child2 = new BTreeInteriorNode(tree, 1, new ArrayMap(4096));
//		child1.mChildNodes.put(ArrayMapKey.EMPTY, null);
//		child1.mChildNodes.put(new ArrayMapKey("apa"), null);
//		child2.mChildNodes.put(ArrayMapKey.EMPTY, null);
//		child2.mChildNodes.put(new ArrayMapKey("banan"), null);
//		node.mChildNodes.put(ArrayMapKey.EMPTY, child1);
//		node.mChildNodes.put(new ArrayMapKey("banan"), child2);
//
//		System.out.println(node.size());
//
//		BTreeInteriorNode shrinkNode = node.shrink();
//
//		System.out.println(shrinkNode.mChildNodes.size());
//	}
}
