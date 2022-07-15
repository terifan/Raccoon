package test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import org.terifan.raccoon.BTreeTableImplementation;
import org.terifan.raccoon.Database;
import org.terifan.raccoon.DatabaseOpenOption;
import org.terifan.raccoon.ScanResult;
import org.terifan.raccoon.annotations.Column;
import org.terifan.raccoon.annotations.Entity;
import org.terifan.raccoon.io.physical.MemoryBlockDevice;
import org.terifan.raccoon.annotations.Id;
import org.terifan.treegraph.HorizontalLayout;
import org.terifan.treegraph.TreeRenderer;
import org.terifan.treegraph.util.TextSlice;
import org.terifan.treegraph.util.VerticalImageFrame;


public class TestTiny
{
	public final static Random RND = new Random(1);

	private static VerticalImageFrame mTreeFrame;
	private static HashMap<String,String> mEntries;


//	public static void main(String ... args)
//	{
//		try
//		{
//			MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);
//
//			try (Database db = new Database(blockDevice, DatabaseOpenOption.CREATE_NEW))
//			{
//				String key = "alpha";
//				String value = Helper.createString(rnd);
//
//				db.save(new KeyValue(key, value));
//
//				KeyValue out = db.get(new KeyValue(key));
//
//				System.out.println(out);
//
//				dump(db);
//			}
//		}
//		catch (Throwable e)
//		{
//			e.printStackTrace(System.out);
//		}
//	}

	public static void main(String... args)
	{
		try
		{
			mTreeFrame = new VerticalImageFrame();

			for (;;)
			{
//				mTreeFrame = new VerticalImageFrame();

				test();

//				mTreeFrame.getFrame().dispose();
			}
		}
		catch (Exception e)
		{
			e.printStackTrace(System.out);
		}
	}


	public static void test() throws Exception
	{
		mEntries = new HashMap<>();

		MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);

//		Log.setLevel(LogLevel.DEBUG);

		try (Database db = new Database(blockDevice, DatabaseOpenOption.CREATE_NEW))
		{
			ArrayList<String> list = new ArrayList<>(Arrays.asList(
//				"Circus", "Banana", "Whale", "Xenon", "Open", "Rupee", "Silver", "Leap", "Ear", "Apple", "Yellow", "Turquoise", "Japanese", "Quality", "Nose", "Gloves", "Head", "Zebra", "Female", "Internal", "Jalapeno", "Urban", "Vapor", "Dove", "Mango", "Knife"
//				, "Clemens", "Bobby", "Wort", "Xor", "Order", "Ranger", "Surfing", "Love", "Eliot", "Asian", "Year", "Tank", "Jeans", "Queer", "Nickle", "Goat", "Happy", "Zink", "Furniture", "Immense", "Jehova", "Under", "Vital", "Dragon", "Many", "King"
//				, "Clemens", "Bread", "Wild", "Xanthe", "Opera", "River", "Sand", "Leach", "Electron", "Accuracy", "Yearning", "Tangent", "Jelly", "Queen", "Number", "Guts", "Harbor", "Zulu", "Fulfill", "Import", "Jupiter", "Ultra", "Voice", "Down", "Metal", "Knight"
//				, "Clear", "Breach", "Wilshire", "Xanthopsia", "Operation", "Robot", "Sugar", "Leather", "Ellipse", "Agree", "Yeisk", "Tartar", "Jigger", "Quelt", "Nutrition", "Gustus", "Hardner", "Zurvan", "Flead", "Instant", "Justis", "Umbrella", "Voltage", "Dwarf", "Misty", "Kart"
//				, "Christian", "Break", "Wilson", "Xanthoma", "Oven", "Rock", "Sudder", "Leap", "Eighty", "Alphabet", "Yekaterinburg", "Tassie", "Jewels", "Quernstone", "Nurses", "Goofer", "Hareem", "Zurek", "Flipper", "Intellectual", "Jitney", "Umbelled", "Vinyl", "Dwell", "Mold", "Karate"

				"able", "about", "above", "across", "act", "active", "activity", "actor", "add", "afraid", "after", "again", "age", "ago", "agree", "air", "all", "alone", "along", "already", "always", "am", "amount", "an", "and", "angry", "another", "answer", "any", "anyone", "anything", "anytime", "appear", "apple", "are", "area", "arm", "army", "around", "arrive", "art", "as", "ask", "at", "attack", "aunt", "autumn", "away", "baby", "back", "bad", "bag", "ball", "bank", "base", "basket", "bath", "be", "bean", "bear", "beautiful", "bed", "bedroom", "beer", "before", "begin", "behave", "behind", "bell", "below", "besides", "best", "better", "between", "big", "bird", "birth", "birthday", "bit", "bite", "black", "bleed", "block", "blood", "blow", "blue", "board", "boat", "body", "boil", "bone", "book", "border", "born", "borrow", "both", "bottle", "bottom", "bowl", "box", "boy", "branch", "brave", "bread", "break", "breakfast", "breathe", "bridge", "bright", "bring", "brother", "brown", "brush", "build", "burn", "bus", "business", "busy", "but", "buy", "by", "cake", "call", "can", "candle", "cap", "car", "card", "care", "careful", "careless", "carry", "case", "cat", "catch", "central", "century", "certain", "chair", "chance", "change", "chase", "cheap", "cheese", "chicken", "child", "children", "chocolate", "choice", "choose", "circle", "city", "class", "clean", "clear", "clever", "climb", "clock", "close", "cloth", "clothes", "cloud", "cloudy", "coat", "coffee", "coin", "cold", "collect", "colour", "comb", "come", "comfortable", "common", "compare", "complete", "computer", "condition", "contain", "continue", "control", "cook", "cool", "copper", "corn", "corner", "correct", "cost", "count", "country", "course", "cover", "crash", "cross", "cry", "cup", "cupboard", "cut", "dance", "danger", "dangerous", "dark", "daughter", "day", "dead", "decide", "decrease", "dedicated", "deep", "deer", "depend", "desk", "destroy", "develop", "die", "different", "difficult", "dinner", "direction", "dirty", "discover", "dish", "do", "dog", "door", "double", "down", "draw", "dream", "dress", "drink", "drive", "drop", "dry", "duck", "dust", "duty", "each", "ear", "early", "earn", "earth", "east", "easy", "eat", "education", "effect", "egg", "eight", "either", "electric", "elephant", "else", "empty", "end", "enemy", "enjoy", "enough", "enter", "entrance", "equal", "escape", "even", "evening", "event", "ever", "every", "everybody", "everyone", "exact", "examination", "example", "except", "excited", "exercise", "expect", "expensive", "explain", "extremely", "eye", "face", "fact", "fail", "fall", "false", "family", "famous", "far", "farm", "fast", "fat", "father", "fault", "fear", "feed", "feel", "female", "few", "fever", "fight", "fill", "film", "find", "fine", "finger", "finish", "fire", "first", "fish", "fit", "five", "fix", "flag", "flat", "float", "floor", "flour", "flower", "fly", "fold", "food", "fool", "foot", "football", "for", "force", "foreign", "forest", "forget", "forgive", "fork", "form", "four", "fox", "free", "freedom", "freeze", "fresh", "friend", "friendly", "from", "front", "fruit", "full", "fun", "funny", "furniture", "further", "future", "game", "garden", "gate", "general", "gentleman", "get", "gift", "give", "glad", "glass", "go", "goat", "god", "gold", "good", "goodbye", "grandfather", "grandmother", "grass", "grave", "gray", "great", "green", "ground", "group", "grow", "gun", "hair", "half", "hall", "hammer", "hand", "happen", "happy", "hard", "hat", "hate", "have", "he", "head", "healthy", "hear", "heart", "heaven", "heavy", "height", "hello", "help", "hen", "her", "here", "hers", "hide", "high", "hill", "him", "his", "hit", "hobby", "hold", "hole", "holiday", "home", "hope", "horse", "hospital", "hot", "hotel", "hour", "house", "how", "hundred", "hungry", "hurry", "hurt", "husband", "ice", "idea", "if", "important", "in", "increase", "inside", "into", "introduce", "invent", "invite", "iron", "is", "island", "it", "its", "jelly", "job", "join", "juice", "jump", "just", "keep", "key", "kid", "kill", "kind", "king", "kitchen", "knee", "knife", "knock", "know", "ladder", "lady", "lamp", "land", "large", "last", "late", "lately", "laugh", "lazy", "lead", "leaf", "learn", "leave", "left", "leg", "lend", "length", "less", "lesson", "let", "letter", "library", "lie", "life", "light", "like", "lion", "lip", "list", "listen", "little", "live", "lock", "lonely", "long", "look", "lose", "lot", "low", "love", "lower", "luck", "machine", "main", "make", "male", "man", "many", "map", "mark", "market", "marry", "matter", "may", "me", "meal", "mean", "measure", "meat", "medicine", "meet", "member", "mention", "method", "middle", "milk", "mill", "million", "mind", "mine", "minute", "miss", "mistake", "mix", "model", "modern", "moment", "money", "monkey", "month", "moon", "more", "morning", "most", "mother", "mountain", "mouse", "mouth", "move", "much", "music", "must", "my", "name", "narrow", "nation", "nature", "near", "nearly", "neck", "need", "needle", "neighbour", "neither", "net", "new", "never", "news", "newspaper", "next", "nice", "night", "nine", "no", "noble", "noise", "none", "nor", "north", "nose", "not", "nothing", "notice", "now", "number", "obey", "object", "ocean", "of", "off", "offer", "office", "often", "oil", "old", "on", "one", "only", "open", "opposite", "or", "orange", "order", "other", "our", "out", "outside", "over", "own", "page", "pain", "paint", "pair", "pan", "paper", "parent", "park", "part", "partner", "party", "pass", "past", "path", "pay", "peace", "pen", "pencil", "people", "pepper", "per", "perfect", "period", "person", "petrol", "photograph", "piano", "pick", "picture", "piece", "pig", "pill", "pin", "pink", "place", "plane", "plant", "plastic", "plate", "play", "please", "pleased", "plenty", "pocket", "point", "poison", "police", "polite", "pool", "poor", "popular", "position", "possible", "potato", "pour", "power", "present", "press", "pretty", "prevent", "price", "prince", "prison", "private", "prize", "probably", "problem", "produce", "promise", "proper", "protect", "provide", "public", "pull", "punish", "pupil", "push", "put", "queen", "question", "quick", "quiet", "quite", "radio", "rain", "rainy", "raise", "reach", "read", "ready", "real", "really", "receive", "record", "red", "remember", "remind", "remove", "rent", "repair", "repeat", "reply", "report", "rest", "restaurant", "result", "return", "rice", "rich", "ride", "right", "ring", "rise", "road", "rob", "rock", "room", "round", "rubber", "rude", "rule", "ruler", "run", "rush", "sad", "safe", "sail", "salt", "same", "sand", "save", "say", "school", "science", "scissors", "search", "seat", "second", "see", "seem", "sell", "send", "sentence", "serve", "seven", "several", "sex", "shade", "shadow", "shake", "shape", "share", "sharp", "she", "sheep", "sheet", "shelf", "shine", "ship", "shirt", "shoe", "shoot", "shop", "short", "should", "shoulder", "shout", "show", "sick", "side", "signal", "silence", "silly", "silver", "similar", "simple", "since", "sing", "single", "sink", "sister", "sit", "six", "size", "skill", "skin", "skirt", "sky", "sleep", "slip", "slow", "small", "smell", "smile", "smoke", "snow", "so", "soap", "sock", "soft", "some", "someone", "something", "sometimes", "son", "soon", "sorry", "sound", "soup", "south", "space", "speak", "special", "speed", "spell", "spend", "spoon", "sport", "spread", "spring", "square", "stamp", "stand", "star", "start", "station", "stay", "steal", "steam", "step", "still", "stomach", "stone", "stop", "store", "storm", "story", "strange", "street", "strong", "structure", "student", "study", "stupid", "subject", "substance", "successful", "such", "sudden", "sugar", "suitable", "summer", "sun", "sunny", "support", "sure", "surprise", "sweet", "swim", "sword", "table", "take", "talk", "tall", "taste", "taxi", "tea", "teach", "team", "tear", "telephone", "television", "tell", "ten", "tennis", "terrible", "test", "than", "that", "the", "their", "theirs", "then", "there", "therefore", "these", "thick", "thin", "thing", "think", "third", "this", "those", "though", "threat", "three", "tidy", "tie", "title", "to", "today", "toe", "together", "tomorrow", "tonight", "too", "tool", "tooth", "top", "total", "touch", "town", "train", "tram", "travel", "tree", "trouble", "true", "trust", "try", "turn", "twice", "type", "ugly", "uncle", "under", "understand", "unit", "until", "up", "use", "useful", "usual", "usually", "wait", "wake", "walk", "want", "warm", "was", "wash", "waste", "watch", "water", "way", "we", "weak", "wear", "weather", "wedding", "week", "vegetable", "weight", "welcome", "well", "were", "very", "west", "wet", "what", "wheel", "when", "where", "which", "while", "white", "who", "why", "wide", "wife", "wild", "will", "village", "win", "wind", "window", "wine", "winter", "wire", "wise", "wish", "visit", "with", "without", "voice", "woman", "wonder", "word", "work", "world", "worry", "yard", "yell", "yesterday", "yet", "you", "young", "your", "zero", "zoo"
			));

//			int seed = -218589678;
			int seed = new Random().nextInt();

			System.out.println("seed=" + seed);
			Collections.shuffle(list, new Random(seed));

			for (String s : list)
			{
				insert(db, s);
			}

			boolean all = true;
			for (String key : mEntries.keySet())
			{
				try
				{
					all &= db.get(new KeyValue(key)).mValue.equals(mEntries.get(key));
				}
				catch (Exception e)
				{
					all = false;
					System.out.println("missing: " + key);
					e.printStackTrace(System.out);
					break;
				}
			}
			System.out.println(all ? "All keys found" : "Missing keys");

			db.commit();
		}

		try (Database db = new Database(blockDevice, DatabaseOpenOption.READ_ONLY))
		{
			boolean all = true;
			for (String key : mEntries.keySet())
			{
				try
				{
					all &= db.get(new KeyValue(key)) != null;
				}
				catch (Exception e)
				{
					System.out.println(key);
					e.printStackTrace(System.out);
					break;
				}
			}
			System.out.println(all ? "All keys found" : "Missing keys");
		}

		try (Database db = new Database(blockDevice, DatabaseOpenOption.OPEN))
		{
			dump(db, "x");

			int size = db.getTable(KeyValue.class).size();
			List<String> keys = new ArrayList<>(mEntries.keySet());

			int seed = new Random().nextInt();

			System.out.println("seed=" + seed);
			Collections.shuffle(keys, new Random(seed));

			for (String key : keys)
			{
				mEntries.remove(key);

				try
				{
					boolean removed = db.remove(new KeyValue(key));
					if(!removed)throw new IllegalStateException(key);
					if (BTreeTableImplementation.STOP) throw new IllegalStateException();
				}
//				catch (Exception e)
//				{
//					System.out.println(key);
//					e.printStackTrace(System.out);
//					break;
//				}
				finally
				{
//					dump(db, key);
				}

				if (db.getTable(KeyValue.class).size() != --size) throw new IllegalStateException("size: " + db.getTable(KeyValue.class).size() + ", expected: " + size);

//				if (key.equals("Apple")) throw new IllegalStateException();
			}
		}
	}


	private static void insert(Database aDatabase, String aKey) throws IOException
	{
		String value = Helper.createString(RND);

		mEntries.put(aKey, value);

		aDatabase.save(new KeyValue(aKey, value));

//		dump(aDatabase, aKey);

		if (BTreeTableImplementation.STOP) throw new IllegalStateException();

//		if (rnd.nextInt(10) < 3)
//		{
//			mTreeFrame.add(new TextSlice("committing"));
//			aDatabase.commit();
//		}
	}


	private static void dump(Database aDatabase, String aKey) throws IOException
	{
		String description = aDatabase.scan(new ScanResult()).getDescription();

		mTreeFrame.add(new TextSlice(aKey));
		mTreeFrame.add(new TreeRenderer(description).render(new HorizontalLayout()));
	}


	@Entity(name = "KeyValue", implementation = "btree")
	public static class KeyValue
	{
		@Id(name="id", index = 0) String mKey;
		@Column(name="value") String mValue;

		public KeyValue()
		{
		}

		public KeyValue(String aKey)
		{
			mKey = aKey;
		}

		public KeyValue(String aKey, String aValue)
		{
			mKey = aKey;
			mValue = aValue;
		}

		@Override
		public String toString()
		{
			return "[" + mKey + "=" + mValue + "]";
		}
	}


	@Entity(name = "fruits", implementation = "btree")
	public static class Fruit
	{
		@Column(name = "id") Long mId;
		@Id String mName;
		@Column(name = "weight") double mWeight;
		@Column(name = "cost") Double mCost;
//		@Column(name = "size") Dimension mDimension;
		@Column(name = "x") int[][] x;


		public Fruit()
		{
		}


		public Fruit(Long aId)
		{
			mId = aId;
		}


		public Fruit(String aName, double aWeight)
		{
			mId = System.nanoTime();
			mName = aName;
			mWeight = aWeight;
//			mDimension = new Dimension(new Random().nextInt(100), new Random().nextInt(100));
		}


		@Override
		public String toString()
		{
//			return "MyEntity{" + "id=" + mId + ", name=" + mName + ", weight=" + mWeight + ", dim=" + mDimension + '}';
			return "MyEntity{" + "id=" + mId + ", name=" + mName + ", weight=" + mWeight + '}';
		}
	}
}
