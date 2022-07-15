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

	private static boolean log = !true;


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

//			for (;;)
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

		try (Database db = new Database(blockDevice, DatabaseOpenOption.CREATE_NEW))
		{
			ArrayList<String> list = new ArrayList<>(Arrays.asList(

				// 100
//				  "Circus", "Banana", "Whale", "Xenon", "Open", "Rupee", "Silver", "Leap", "Ear", "Apple", "Yellow", "Turquoise", "Japanese", "Quality", "Nose", "Gloves", "Head", "Zebra", "Female", "Internal", "Jalapeno", "Urban", "Vapor", "Dove", "Mango", "Knife"
//				, "Clemens", "Bobby", "Wort", "Xor", "Order", "Ranger", "Surfing", "Love", "Eliot", "Asian", "Year", "Tank", "Jeans", "Queer", "Nickle", "Goat", "Happy", "Zink", "Furniture", "Immense", "Jehova", "Under", "Vital", "Dragon", "Many", "King"
//				, "Clemens", "Bread", "Wild", "Xanthe", "Opera", "River", "Sand", "Leach", "Electron", "Accuracy", "Yearning", "Tangent", "Jelly", "Queen", "Number", "Guts", "Harbor", "Zulu", "Fulfill", "Import", "Jupiter", "Ultra", "Voice", "Down", "Metal", "Knight"
//				, "Clear", "Breach", "Wilshire", "Xanthopsia", "Operation", "Robot", "Sugar", "Leather", "Ellipse", "Agree", "Yeisk", "Tartar", "Jigger", "Quelt", "Nutrition", "Gustus", "Hardner", "Zurvan", "Flead", "Instant", "Justis", "Umbrella", "Voltage", "Dwarf", "Misty", "Kart"
//				, "Christian", "Break", "Wilson", "Xanthoma", "Oven", "Rock", "Sudder", "Leap", "Eighty", "Alphabet", "Yekaterinburg", "Tassie", "Jewels", "Quernstone", "Nurses", "Goofer", "Hareem", "Zurek", "Flipper", "Intellectual", "Jitney", "Umbelled", "Vinyl", "Dwell", "Mold", "Karate"

				// 1000
//				"able", "about", "above", "across", "act", "active", "activity", "actor", "add", "afraid", "after", "again", "age", "ago", "agree", "air", "all", "alone", "along", "already", "always", "am", "amount", "an", "and", "angry", "another", "answer", "any", "anyone", "anything", "anytime", "appear", "apple", "are", "area", "arm", "army", "around", "arrive", "art", "as", "ask", "at", "attack", "aunt", "autumn", "away", "baby", "back", "bad", "bag", "ball", "bank", "base", "basket", "bath", "be", "bean", "bear", "beautiful", "bed", "bedroom", "beer", "before", "begin", "behave", "behind", "bell", "below", "besides", "best", "better", "between", "big", "bird", "birth", "birthday", "bit", "bite", "black", "bleed", "block", "blood", "blow", "blue", "board", "boat", "body", "boil", "bone", "book", "border", "born", "borrow", "both", "bottle", "bottom", "bowl", "box", "boy", "branch", "brave", "bread", "break", "breakfast", "breathe", "bridge", "bright", "bring", "brother", "brown", "brush", "build", "burn", "bus", "business", "busy", "but", "buy", "by", "cake", "call", "can", "candle", "cap", "car", "card", "care", "careful", "careless", "carry", "case", "cat", "catch", "central", "century", "certain", "chair", "chance", "change", "chase", "cheap", "cheese", "chicken", "child", "children", "chocolate", "choice", "choose", "circle", "city", "class", "clean", "clear", "clever", "climb", "clock", "close", "cloth", "clothes", "cloud", "cloudy", "coat", "coffee", "coin", "cold", "collect", "colour", "comb", "come", "comfortable", "common", "compare", "complete", "computer", "condition", "contain", "continue", "control", "cook", "cool", "copper", "corn", "corner", "correct", "cost", "count", "country", "course", "cover", "crash", "cross", "cry", "cup", "cupboard", "cut", "dance", "danger", "dangerous", "dark", "daughter", "day", "dead", "decide", "decrease", "dedicated", "deep", "deer", "depend", "desk", "destroy", "develop", "die", "different", "difficult", "dinner", "direction", "dirty", "discover", "dish", "do", "dog", "door", "double", "down", "draw", "dream", "dress", "drink", "drive", "drop", "dry", "duck", "dust", "duty", "each", "ear", "early", "earn", "earth", "east", "easy", "eat", "education", "effect", "egg", "eight", "either", "electric", "elephant", "else", "empty", "end", "enemy", "enjoy", "enough", "enter", "entrance", "equal", "escape", "even", "evening", "event", "ever", "every", "everybody", "everyone", "exact", "examination", "example", "except", "excited", "exercise", "expect", "expensive", "explain", "extremely", "eye", "face", "fact", "fail", "fall", "false", "family", "famous", "far", "farm", "fast", "fat", "father", "fault", "fear", "feed", "feel", "female", "few", "fever", "fight", "fill", "film", "find", "fine", "finger", "finish", "fire", "first", "fish", "fit", "five", "fix", "flag", "flat", "float", "floor", "flour", "flower", "fly", "fold", "food", "fool", "foot", "football", "for", "force", "foreign", "forest", "forget", "forgive", "fork", "form", "four", "fox", "free", "freedom", "freeze", "fresh", "friend", "friendly", "from", "front", "fruit", "full", "fun", "funny", "furniture", "further", "future", "game", "garden", "gate", "general", "gentleman", "get", "gift", "give", "glad", "glass", "go", "goat", "god", "gold", "good", "goodbye", "grandfather", "grandmother", "grass", "grave", "gray", "great", "green", "ground", "group", "grow", "gun", "hair", "half", "hall", "hammer", "hand", "happen", "happy", "hard", "hat", "hate", "have", "he", "head", "healthy", "hear", "heart", "heaven", "heavy", "height", "hello", "help", "hen", "her", "here", "hers", "hide", "high", "hill", "him", "his", "hit", "hobby", "hold", "hole", "holiday", "home", "hope", "horse", "hospital", "hot", "hotel", "hour", "house", "how", "hundred", "hungry", "hurry", "hurt", "husband", "ice", "idea", "if", "important", "in", "increase", "inside", "into", "introduce", "invent", "invite", "iron", "is", "island", "it", "its", "jelly", "job", "join", "juice", "jump", "just", "keep", "key", "kid", "kill", "kind", "king", "kitchen", "knee", "knife", "knock", "know", "ladder", "lady", "lamp", "land", "large", "last", "late", "lately", "laugh", "lazy", "lead", "leaf", "learn", "leave", "left", "leg", "lend", "length", "less", "lesson", "let", "letter", "library", "lie", "life", "light", "like", "lion", "lip", "list", "listen", "little", "live", "lock", "lonely", "long", "look", "lose", "lot", "low", "love", "lower", "luck", "machine", "main", "make", "male", "man", "many", "map", "mark", "market", "marry", "matter", "may", "me", "meal", "mean", "measure", "meat", "medicine", "meet", "member", "mention", "method", "middle", "milk", "mill", "million", "mind", "mine", "minute", "miss", "mistake", "mix", "model", "modern", "moment", "money", "monkey", "month", "moon", "more", "morning", "most", "mother", "mountain", "mouse", "mouth", "move", "much", "music", "must", "my", "name", "narrow", "nation", "nature", "near", "nearly", "neck", "need", "needle", "neighbour", "neither", "net", "new", "never", "news", "newspaper", "next", "nice", "night", "nine", "no", "noble", "noise", "none", "nor", "north", "nose", "not", "nothing", "notice", "now", "number", "obey", "object", "ocean", "of", "off", "offer", "office", "often", "oil", "old", "on", "one", "only", "open", "opposite", "or", "orange", "order", "other", "our", "out", "outside", "over", "own", "page", "pain", "paint", "pair", "pan", "paper", "parent", "park", "part", "partner", "party", "pass", "past", "path", "pay", "peace", "pen", "pencil", "people", "pepper", "per", "perfect", "period", "person", "petrol", "photograph", "piano", "pick", "picture", "piece", "pig", "pill", "pin", "pink", "place", "plane", "plant", "plastic", "plate", "play", "please", "pleased", "plenty", "pocket", "point", "poison", "police", "polite", "pool", "poor", "popular", "position", "possible", "potato", "pour", "power", "present", "press", "pretty", "prevent", "price", "prince", "prison", "private", "prize", "probably", "problem", "produce", "promise", "proper", "protect", "provide", "public", "pull", "punish", "pupil", "push", "put", "queen", "question", "quick", "quiet", "quite", "radio", "rain", "rainy", "raise", "reach", "read", "ready", "real", "really", "receive", "record", "red", "remember", "remind", "remove", "rent", "repair", "repeat", "reply", "report", "rest", "restaurant", "result", "return", "rice", "rich", "ride", "right", "ring", "rise", "road", "rob", "rock", "room", "round", "rubber", "rude", "rule", "ruler", "run", "rush", "sad", "safe", "sail", "salt", "same", "sand", "save", "say", "school", "science", "scissors", "search", "seat", "second", "see", "seem", "sell", "send", "sentence", "serve", "seven", "several", "sex", "shade", "shadow", "shake", "shape", "share", "sharp", "she", "sheep", "sheet", "shelf", "shine", "ship", "shirt", "shoe", "shoot", "shop", "short", "should", "shoulder", "shout", "show", "sick", "side", "signal", "silence", "silly", "silver", "similar", "simple", "since", "sing", "single", "sink", "sister", "sit", "six", "size", "skill", "skin", "skirt", "sky", "sleep", "slip", "slow", "small", "smell", "smile", "smoke", "snow", "so", "soap", "sock", "soft", "some", "someone", "something", "sometimes", "son", "soon", "sorry", "sound", "soup", "south", "space", "speak", "special", "speed", "spell", "spend", "spoon", "sport", "spread", "spring", "square", "stamp", "stand", "star", "start", "station", "stay", "steal", "steam", "step", "still", "stomach", "stone", "stop", "store", "storm", "story", "strange", "street", "strong", "structure", "student", "study", "stupid", "subject", "substance", "successful", "such", "sudden", "sugar", "suitable", "summer", "sun", "sunny", "support", "sure", "surprise", "sweet", "swim", "sword", "table", "take", "talk", "tall", "taste", "taxi", "tea", "teach", "team", "tear", "telephone", "television", "tell", "ten", "tennis", "terrible", "test", "than", "that", "the", "their", "theirs", "then", "there", "therefore", "these", "thick", "thin", "thing", "think", "third", "this", "those", "though", "threat", "three", "tidy", "tie", "title", "to", "today", "toe", "together", "tomorrow", "tonight", "too", "tool", "tooth", "top", "total", "touch", "town", "train", "tram", "travel", "tree", "trouble", "true", "trust", "try", "turn", "twice", "type", "ugly", "uncle", "under", "understand", "unit", "until", "up", "use", "useful", "usual", "usually", "wait", "wake", "walk", "want", "warm", "was", "wash", "waste", "watch", "water", "way", "we", "weak", "wear", "weather", "wedding", "week", "vegetable", "weight", "welcome", "well", "were", "very", "west", "wet", "what", "wheel", "when", "where", "which", "while", "white", "who", "why", "wide", "wife", "wild", "will", "village", "win", "wind", "window", "wine", "winter", "wire", "wise", "wish", "visit", "with", "without", "voice", "woman", "wonder", "word", "work", "world", "worry", "yard", "yell", "yesterday", "yet", "you", "young", "your", "zero", "zoo"

				// 2000
				"able", "about", "absence", "absorption", "acceleration", "acceptance", "accessory", "accident", "account", "acid", "across", "act", "acting", "active", "actor", "addition", "address", "adjacent", "adjustment", "adventure", "advertisement", "advice", "after", "afterthought", "again", "against", "age", "agency", "agent", "ago", "agreement", "air", "airplane", "alcohol", "algebra", "all", "allowance", "almost", "along", "also", "alternative", "aluminium", "always", "ambition", "ammonia", "among", "amount", "amplitude", "amusement", "anchor", "and", "anesthetic", "angle", "angry", "animal", "ankle", "another", "answer", "ant", "any", "anybody", "anyhow", "anyone", "anything", "anywhere", "apparatus", "appendage", "apple", "application", "approval", "approximation", "April", "arbitrary", "arbitration", "arc", "arch", "area", "argument", "arithmetic", "arm", "army", "arrangement", "art", "as", "asbestos", "ash", "asset", "assistant", "at", "attack", "attempt", "attention", "attraction", "August", "authority", "autobus", "automatic", "automobile", "awake", "average", "awkward", "axis", "baby", "back", "backbone", "backwoods", "bad", "bag", "balance", "balcony", "bale", "ball", "ballet", "band", "bang", "bank", "bankrupt", "bar", "bark", "barrel", "base", "based", "basin", "basing", "basket", "bath", "be", "beak", "beaker", "beard", "beat", "beautiful", "because", "become", "bed", "bedroom", "bee", "beef", "beer", "beeswax", "before", "behavior", "behind", "belief", "bell", "belt", "bent", "berry", "bet", "between", "bill", "biology", "bird", "birefringence", "birth", "birthday", "birthright", "bit", "bite", "bitter", "black", "blackberry", "blackbird", "blackboard", "blade", "blame", "blanket", "blood", "bloodvessel", "blow", "blue", "bluebell", "board", "boat", "body", "boiling", "bomb", "bone", "book", "bookkeeper", "boot", "both", "bottle", "bottom", "box", "boy", "brain", "brake", "branch", "brass", "brave", "bread", "break", "breakfast", "breast", "breath", "brick", "bridge", "bright", "broken", "broker", "brother", "brown", "brush", "brushwood", "bubble", "bucket", "bud", "budget", "builder", "building", "bulb", "bunch", "buoyancy", "burial", "burn", "burned", "burner", "burning", "burst", "business", "busy", "but", "butter", "buttercup", "button", "by", "cafe", "cake", "calculation", "calendar", "call", "camera", "canvas", "capacity", "capital", "card", "cardboard", "care", "carefree", "caretaker", "carpet", "carriage", "cart", "carter", "cartilage", "case", "cast", "cat", "catarrh", "cause", "cave", "cavity", "cell", "ceremony", "certain", "certificate", "chain", "chair", "chalk", "champagne", "chance", "change", "character", "charge", "chauffeur", "cheap", "check", "cheese", "chemical", "chemist", "chemistry", "chest", "chief", "child", "chimney", "chin", "china", "chocolate", "choice", "chorus", "church", "cigarette", "circle", "circuit", "circulation", "circumference", "circus", "citron", "civilization", "claim", "claw", "clay", "clean", "clear", "cleavage", "clever", "client", "climber", "clip", "clock", "clockwork", "cloth", "clothier", "clothing", "cloud", "club", "coal", "coat", "cocktail", "code", "coffee", "cognac", "coil", "cold", "collar", "collection", "college", "collision", "colony", "color", "column", "comb", "combination", "combine", "come", "comfort", "committee", "common", "commonsense", "communications", "company", "comparison", "competition", "complaint", "complete", "complex", "component", "compound", "concept", "concrete", "condition", "conductor", "congruent", "connection", "conscious", "conservation", "consignment", "constant", "consumer", "continuous", "contour", "control", "convenient", "conversion", "cook", "cooked", "cooker", "cooking", "cool", "copper", "copy", "copyright", "cord", "cork", "corner", "correlation", "corrosion", "cost", "cotton", "cough", "country", "court", "cow", "cover", "crack", "credit", "creeper", "crime", "crop", "cross", "cruel", "crush", "cry", "crying", "cunning", "cup", "cupboard", "current", "curtain", "curve", "cushion", "cusp", "customs", "cut", "damage", "damping", "dance", "dancer", "dancing", "danger", "dark", "date", "daughter", "day", "daylight", "dead", "dear", "death", "debit", "debt", "December", "decision", "deck", "decrease", "deep", "defect", "deficiency", "deflation", "degenerate", "degree", "degree", "delicate", "delivery", "demand", "denominator", "density", "department", "dependent", "deposit", "desert", "design", "designer", "desire", "destruction", "detail", "determining", "dew", "development", "diameter", "difference", "different", "difficulty", "digestion", "dike", "dilution", "dinner", "dip", "direct", "direction", "dirty", "disappearance", "discharge", "discount", "discovery", "discussion", "disease", "disgrace", "disgust", "dislike", "dissipation", "distance", "distribution", "disturbance", "ditch", "dive", "division", "divisor", "divorce", "do", "dog", "doll", "domesticating", "Dominion", "door", "doubt", "down", "downfall", "drain", "drawer", "dreadful", "dream", "dress", "dressing", "drift", "drink", "driver", "driving", "drop", "dropped", "dropper", "dry", "duct", "dull", "dust", "duster", "duty", "dynamite", "each", "ear", "early", "earring", "earth", "earthwork", "east", "easy", "economy", "edge", "education", "effect", "efficiency", "effort", "egg", "eight", "either", "elastic", "electric", "electricity", "eleven", "elimination", "Embassy", "Empire", "employer", "empty", "encyclopedia", "end", "enemy", "engine", "engineer", "enough", "envelope", "environment", "envy", "equal", "equation", "erosion", "error", "eruption", "evaporation", "even", "evening", "event", "ever", "evergreen", "every", "everybody", "everyday", "everyone", "everything", "everywhere", "exact", "example", "exchange", "excitement", "exercise", "existence", "expansion", "experience", "experiment", "expert", "explanation", "explosion", "export", "expression", "extinction", "eye", "eyeball", "eyebrow", "eyelash", "face", "fact", "factor", "failure", "fair", "fall", "false", "family", "famous", "fan", "far", "farm", "farmer", "fastening", "fat", "father", "fatherland", "fault", "fear", "feather", "February", "feeble", "feeling", "female", "ferment", "fertile", "fertilizing", "fever", "fiber", "fiction", "field", "fifteen", "fifth", "fifty", "fight", "figure", "fin", "financial", "finger", "fingerprint", "fire", "firearm", "fired", "firefly", "fireman", "fireplace", "firework", "firing", "first", "fish", "fisher/fisherman", "five", "fixed", "flag", "flame", "flash", "flask", "flat", "flesh", "flight", "flint", "flood", "floor", "flour", "flow", "flower", "fly", "focus", "fold", "folder", "foliation", "food", "foolish", "foot", "football", "footlights", "footman", "footnote", "footprint", "footstep", "for", "force", "forecast", "forehead", "foreign", "forgiveness", "fork", "form", "forty", "forward", "four", "fourteen", "fourth", "fowl", "fraction", "fracture", "frame", "free", "frequent", "fresh", "friction", "Friday", "friend", "from", "front", "frost", "frozen", "fruit", "full", "fume", "funnel", "funny", "fur", "furnace", "furniture", "fusion", "future", "garden", "gardener", "gas", "gasworks", "gate", "general", "generation", "geography", "geology", "geometry", "germ", "germinating", "get", "gill", "girl", "give", "glacier", "gland", "glass", "glove", "glycerin", "go", "goat", "god", "gold", "goldfish", "good", "goodlooking", "goodnight", "government", "grain", "gram", "grand", "grass", "grateful", "grating", "gravel", "grease", "great", "green", "grey/gray", "grief", "grip", "grocery", "groove", "gross", "ground", "group", "growth", "guarantee", "guard", "guess", "guide", "gum", "gun", "gunboat", "gunmetal", "gunpowder", "habit", "hair", "half", "hammer", "hand", "handbook", "handkerchief", "handle", "handwriting", "hanger", "hanging", "happy", "harbor", "hard", "harmony", "hat", "hate", "have", "he", "head", "headdress", "headland", "headstone", "headway", "healthy", "hearing", "heart", "heat", "heated", "heater", "heating", "heavy", "hedge", "help", "here", "hereafter", "herewith", "high", "highlands", "highway", "hill", "himself", "hinge", "hire", "hiss", "history", "hold", "hole", "holiday", "hollow", "home", "honest", "honey", "hoof", "hook", "hope", "horn", "horse", "horseplay", "horsepower", "hospital", "host", "hotel", "hour", "hourglass", "house", "houseboat", "housekeeper", "how", "however", "human", "humor", "hundred", "hunt", "hurry", "hurt", "husband", "hyena", "hygiene", "hysteria", "I", "ice", "idea", "if", "igneous", "ill", "image", "imagination", "Imperial", "import", "important", "impulse", "impurity", "in", "inasmuch", "inclusion", "income", "increase", "index", "individual", "indoors", "industry", "inferno", "infinity", "inflation", "influenza", "inheritance", "ink", "inland", "inlet", "inner", "innocent", "input", "insect", "inside", "instep", "institution", "instrument", "insulator", "insurance", "integer", "intelligent", "intercept", "interest", "international", "interpretation", "intersection", "into", "intrusion", "invention", "inverse", "investigation", "investment", "invitation", "iron", "island", "itself", "jam", "January", "jaw", "jazz", "jealous", "jelly", "jerk", "jewel", "jeweler", "join", "joiner", "joint", "journey", "judge", "jug", "juice", "July", "jump", "June", "jury", "justice", "keep", "keeper", "kennel", "kettle", "key", "kick", "kidney", "kilo", "kind", "king", "kiss", "kitchen", "knee", "knife", "knock", "knot", "knowledge", "lace", "lag", "lake", "lame", "lamp", "land", "landmark", "landslip", "language", "large", "last", "late", "latitude", "latitude", "laugh", "laughing", "law", "lava", "lawyer", "layer", "lazy", "lead", "leaf", "learner", "learning", "least", "leather", "lecture", "left", "leg", "legal", "length", "lens", "less", "lesson", "let", "letter", "level", "lever", "liability", "library", "license", "lid", "life", "lift", "light", "lighthouse", "like", "lime", "limestone", "limit", "line", "linen", "link", "lip", "liqueur", "liquid", "list", "liter", "little", "liver", "living", "load", "loan", "local", "lock", "locker", "locking", "locus", "long", "longitude", "longitude", "look", "loose", "loss", "loud", "low", "love", "luck", "lump", "lunch", "lung", "macaroni", "machine", "madam", "magic", "magnetic", "magnitude", "make", "malaria", "male", "man", "manager", "manhole", "mania", "manner", "many", "map", "marble", "March", "margin", "mark", "marked", "market", "marriage", "married", "mass", "mast", "match", "material", "mathematics", "mattress", "mature", "may", "May", "meal", "mean", "meaning", "measure", "meat", "medical", "medicine", "medium", "meeting", "melt", "member", "memory", "meow", "mess", "message", "metabolism", "metal", "meter", "microscope", "middle", "military", "milk", "mill", "million", "mind", "mine", "miner", "mineral", "minute", "minute", "mist", "mixed", "mixture", "model", "modern", "modest", "momentum", "Monday", "money", "monkey", "monopoly", "month", "mood", "moon", "moral", "more", "morning", "most", "mother", "motion", "mountain", "moustache", "mouth", "move", "much", "mud", "multiple", "multiplication", "murder", "muscle", "museum", "music", "myself", "nail", "name", "narrow", "nasty", "nation", "natural", "nature", "navy", "near", "nearer", "neat", "necessary", "neck", "need", "needle", "neglect", "neighbor", "nerve", "nest", "net", "network", "neutron", "new", "news", "newspaper", "next", "nice", "nickel", "nicotine", "night", "nine", "no", "nobody", "node", "noise", "normal", "north", "nose", "nostril", "not", "note", "noted", "nothing", "now", "November", "nowhere", "nucleus", "number", "numerator", "nurse", "nut", "obedient", "observation", "October", "of", "off", "offer", "office", "officer", "offspring", "oil", "old", "olive", "omelet", "on", "once", "oncoming", "one", "oneself", "onlooker", "only", "onto", "open", "opera", "operation", "opinion", "opium", "opposite", "or", "orange", "orchestra", "orchestra", "order", "ore", "organ", "organism", "organization", "origin", "ornament", "other", "out", "outburst", "outcome", "outcrop", "outcry", "outdoor", "outer", "outgoing", "outhouse", "outlaw", "outlet", "outlier", "outline", "outlook", "output", "outside", "outskirts", "outstretched", "oval", "oven", "over", "overacting", "overall", "overbalancing", "overbearing", "overcoat", "overcome", "overdo", "overdressed", "overfull", "overhanging", "overhead", "overland", "overlap", "overleaf", "overloud", "overseas", "overseer", "overshoe", "overstatement", "overtake", "overtaxed", "overtime", "overturned", "overuse", "overvalued", "overweight", "overworking", "own", "owner", "oxidation", "packing", "pad", "page", "pain", "paint", "painter", "painting", "pair", "pajamas", "pan", "paper", "paradise", "paraffin", "paragraph", "parallel", "parcel", "parent", "park", "part", "particle", "parting", "partner", "party", "passage", "passport", "past", "paste", "patent", "path", "patience", "payment", "peace", "pedal", "pen", "pencil", "pendulum", "penguin", "pension", "people", "perfect", "person", "petal", "petroleum", "phonograph", "physical", "Physics", "Physiology", "piano", "picture", "pig", "pin", "pincushion", "pipe", "piston", "place", "plain", "plan", "plane", "plant", "plaster", "plate", "platinum", "play", "played", "playing", "plaything", "please", "pleased", "pleasure", "plough/plow", "plug", "pocket", "poetry", "point", "pointer", "pointing", "poison", "police", "policeman", "polish", "political", "pollen", "pool", "poor", "population", "porcelain", "porter", "position", "possible", "post", "postman", "postmark", "postmaster", "postoffice", "pot", "potash", "potato", "potter", "powder", "power", "practice", "praise", "prayer", "present", "President", "pressure", "price", "prick", "priest", "prime", "Prince", "Princess", "print", "printer", "prison", "prisoner", "private", "probability", "probable", "process", "produce", "producer", "product", "profit", "program", "progress", "projectile", "projection", "promise", "proof", "propaganda", "property", "prose", "protest", "proud", "Psychology", "public", "pull", "pulley", "pump", "punishment", "pupil", "purchase", "pure", "purpose", "Purr", "push", "put", "pyramid", "quack", "quality", "quantity", "quarter", "queen", "question", "quick", "quiet", "quinine", "quite", "quotient", "race", "radiation", "radio", "radium", "rail", "rain", "raining", "range", "rat", "rate", "ratio", "ray", "reaction", "reader", "reading", "reading", "ready", "reagent", "real", "reason", "receipt", "receiver", "reciprocal", "record", "rectangle", "recurring", "red", "reference", "referendum", "reflux", "regret", "regular", "reinforcement", "relation", "relative", "religion", "remark", "remedy", "rent", "repair", "representative", "reproduction", "repulsion", "request", "residue", "resistance", "resolution", "respect", "responsible", "rest", "restaurant", "result", "retail", "reward", "revenge", "reversible", "rheumatism", "rhythm", "rice", "rich", "right", "rigidity", "ring", "rise", "rival", "river", "road", "rock", "rod", "roll", "roller", "roof", "room", "root", "rot", "rotation", "rough", "round", "Royal", "rub", "rubber", "rude", "rule", "ruler", "rum", "run", "runaway", "rust", "sac", "sad", "safe", "sail", "sailor", "salad", "sale", "salt", "same", "sample", "sand", "sardine", "satisfaction", "saturated", "Saturday", "saucer", "saving", "say", "scale", "scale", "scarp", "schist", "school", "science", "scissors", "scratch", "screen", "screw", "sea", "seal", "seaman", "search", "seat", "second", "second", "secondhand", "secret", "secretary", "secretion", "section", "security", "sedimentary", "see", "seed", "seem", "selection", "self", "selfish", "send", "sense", "sensitivity", "sentence", "sepal", "separate", "September", "serious", "serum", "servant", "service", "set", "seven", "sex", "shade", "shadow", "shake", "shale", "shame", "share", "sharp", "shave", "shear", "sheep", "sheet", "shelf", "shell", "ship", "shirt", "shock", "shocked", "shocking", "shoe", "shore", "short", "shorthand", "shoulder", "show", "shut", "side", "sideboard", "sidewalk", "sight", "sign", "silk", "sill", "silver", "similarity", "simple", "since", "sir", "sister", "six", "sixteen", "size", "skin", "skirt", "skull", "sky", "slate", "sleep", "sleeve", "slide", "slip", "slope", "slow", "small", "smash", "smell", "smile", "smoke", "smooth", "snake", "sneeze", "snow", "snowing", "so", "soap", "social", "society", "sock", "soft", "soil", "soldier", "solid", "solution", "solvent", "some", "somebody", "someday", "somehow", "someone", "something", "sometime", "somewhat", "somewhere", "son", "song", "sorry", "sort", "sound", "soup", "south", "space", "spade", "spark", "special", "specialization", "specimen", "speculation", "spirit", "spit", "splash", "sponge", "spoon", "sport", "spot", "spring", "square", "stable", "stage", "stain", "stair", "stalk", "stamen", "stamp", "star", "start", "statement", "station", "statistics", "steady", "steam", "steamer", "steel", "stem", "step", "stick", "sticky", "stiff", "still", "stimulus", "stitch", "stocking", "stomach", "stone", "stop", "stopper", "stopping", "stoppingup", "store", "storm", "story", "straight", "strain", "strange", "straw", "stream", "street", "strength", "stress", "stretch", "stretcher", "strike", "string", "strong", "structure", "study", "subject", "substance", "substitution", "subtraction", "success", "successive", "such", "suchlike", "sucker", "sudden", "sugar", "suggestion", "sum", "summer", "sun", "sunburn", "Sunday", "sunlight", "sunshade", "supply", "support", "surface", "surgeon", "surprise", "suspension", "suspicious", "sweet", "sweetheart", "swelling", "swim", "swing", "switch", "sympathetic", "system", "table", "tail", "tailor", "take", "talk", "talking", "tall", "tame", "tap", "tapioca", "taste", "tax", "taxi", "tea", "teacher", "teaching", "tear", "telegram", "telephone", "ten", "tendency", "tent", "term", "terrace", "test", "texture", "than", "that", "the", "theater", "then", "theory", "there", "thermometer", "thick", "thickness", "thief", "thimble", "thin", "thing", "third", "thirteen", "thirty", "this", "thorax", "though", "thought", "thousand", "thread", "threat", "three", "throat", "through", "thrust", "thumb", "thunder", "Thursday", "ticket", "tide", "tie", "tight", "till", "time", "tin", "tired", "tissue", "to", "toast", "tobacco", "today", "toe", "together", "tomorrow", "tongs", "tongue", "tonight", "too", "tooth", "top", "torpedo", "total", "touch", "touching", "towel", "tower", "town", "trade", "trader", "tradesman", "traffic", "tragedy", "train", "trainer", "training", "transmission", "transparent", "transport", "trap", "travel", "tray", "treatment", "tree", "triangle", "trick", "trouble", "troubled", "troubling", "trousers", "truck", "true", "tube", "Tuesday", "tune", "tunnel", "turbine", "turn", "turning", "twelve", "twenty", "twice", "twin", "twist", "two", "typist", "ugly", "umbrella", "unconformity", "under", "underclothing", "undercooked", "undergo", "undergrowth", "undermined", "undersigned", "undersized", "understanding", "understatement", "undertake", "undervalued", "undo", "unit", "universe", "university", "unknown", "up", "upkeep", "uplift", "upon", "upright", "uptake", "use", "used", "waiter", "waiting", "waiting", "valency", "walk", "wall", "valley", "value", "valve", "vanilla", "vapor", "war", "variable", "warm", "vascular", "wash", "waste", "wasted", "watch", "water", "waterfall", "wave", "wax", "way", "weak", "weather", "wedge", "Wednesday", "week", "weekend", "vegetable", "weight", "welcome", "well", "velocity", "verse", "very", "vessel", "west", "vestigial", "wet", "what", "whatever", "wheel", "when", "whenever", "where", "whereas", "whereby", "wherever", "whether", "which", "whichever", "while", "whip", "whisky", "whistle", "white", "whitewash", "who", "whoever", "wholesale", "why", "victim", "victory", "wide", "widow", "view", "viewpoint", "wife", "wild", "will", "wind", "window", "windpipe", "wine", "wing", "winter", "violent", "violin", "wire", "visa", "wise", "vitamin", "with", "within", "without", "vodka", "voice", "volt", "volume", "woman", "wood", "woodwork", "wool", "word", "work", "worker", "workhouse", "working", "world", "worm", "vortex", "vote", "wound", "wreck", "wrist", "writer", "writing", "wrong", "yawn", "year", "yearbook", "yellow", "yes", "yesterday", "you", "young", "yourself", "zebra", "zinc", "zookeeper", "zoology"
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
			if (!log) dump(db, "x");

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
					if (log) dump(db, key);
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

		if (log) dump(aDatabase, aKey);

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
