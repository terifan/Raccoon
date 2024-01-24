package test_raccoon;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Random;
import java.util.UUID;
import org.terifan.logging.Level;
import org.terifan.logging.Logger;
import org.terifan.raccoon.document.ObjectId;
import org.terifan.raccoon.document.Array;
import org.terifan.raccoon.document.Document;


public class _Person
{
	private final static String[][] FIRST_NAMES =
	{
		{
			"James", "Robert", "John", "Michael", "David", "William", "Richard", "Joseph", "Thomas", "Charles", "Christopher", "Daniel",
			"Matthew", "Anthony", "Mark", "Donald", "Steven", "Paul", "Andrew", "Joshua", "Kenneth", "Kevin", "Brian", "George", "Timothy",
			"Ronald", "Edward", "Jason", "Jeffrey", "Ryan", "Jacob", "Gary", "Nicholas", "Eric", "Jonathan", "Stephen", "Larry", "Justin",
			"Scott", "Brandon", "Benjamin", "Samuel", "Gregory", "Alexander", "Frank", "Patrick", "Raymond", "Jack", "Dennis", "Jerry",
			"Tyler", "Aaron", "Jose", "Adam", "Nathan", "Henry", "Douglas", "Zachary", "Peter", "Kyle", "Ethan", "Walter", "Noah", "Jeremy",
			"Christian", "Keith", "Roger", "Terry", "Gerald", "Harold", "Sean", "Austin", "Carl", "Arthur", "Lawrence", "Dylan", "Jesse",
			"Jordan", "Bryan", "Billy", "Joe", "Bruce", "Gabriel", "Logan", "Albert", "Willie", "Alan", "Juan", "Wayne", "Elijah", "Randy",
			"Roy", "Vincent", "Ralph", "Eugene", "Russell", "Bobby", "Mason", "Philip", "Louis"
		},
		{
			"Mary", "Patricia", "Jennifer", "Linda", "Elizabeth", "Barbara", "Susan", "Jessica", "Sarah", "Karen", "Lisa", "Nancy", "Betty",
			"Margaret", "Sandra", "Ashley", "Kimberly", "Emily", "Donna", "Michelle", "Carol", "Amanda", "Dorothy", "Melissa", "Deborah",
			"Stephanie", "Rebecca", "Sharon", "Laura", "Cynthia", "Kathleen", "Amy", "Angela", "Shirley", "Anna", "Brenda", "Pamela", "Emma",
			"Nicole", "Helen", "Samantha", "Katherine", "Christine", "Debra", "Rachel", "Carolyn", "Janet", "Catherine", "Maria", "Heather",
			"Diane", "Ruth", "Julie", "Olivia", "Joyce", "Virginia", "Victoria", "Kelly", "Lauren", "Christina", "Joan", "Evelyn", "Judith",
			"Megan", "Andrea", "Cheryl", "Hannah", "Jacqueline", "Martha", "Gloria", "Teresa", "Ann", "Sara", "Madison", "Frances",
			"Kathryn", "Janice", "Jean", "Abigail", "Alice", "Julia", "Judy", "Sophia", "Grace", "Denise", "Amber", "Doris", "Marilyn",
			"Danielle", "Beverly", "Isabella", "Theresa", "Diana", "Natalie", "Brittany", "Charlotte", "Marie", "Kayla", "Alexis", "Lori"
		}
	};
	private final static String[] LAST_NAMES =
	{
		"Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez",
		"Gonzalez", "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson", "White", "Harris",
		"Sanchez", "Clark", "Ramirez", "Lewis", "Robinson", "Walker", "Young", "Allen", "King", "Wright", "Scott", "Torres", "Nguyen",
		"Hill", "Flores", "Green", "Adams", "Nelson", "Baker", "Hall", "Rivera", "Campbell", "Mitchell", "Carter", "Roberts", "Gomez",
		"Phillips", "Evans", "Turner", "Diaz", "Parker", "Cruz", "Edwards", "Collins", "Reyes", "Stewart", "Morris", "Morales", "Murphy",
		"Cook", "Rogers", "Gutierrez", "Ortiz", "Morgan", "Cooper", "Peterson", "Bailey", "Reed", "Kelly", "Howard", "Ramos", "Kim", "Cox",
		"Ward", "Richardson", "Watson", "Brooks", "Chavez", "Wood", "James", "Bennett", "Gray", "Mendoza", "Ruiz", "Hughes", "Price",
		"Alvarez", "Castillo", "Sanders", "Patel", "Myers", "Long", "Ross", "Foster", "Jimenez"
	};
	private final static String[] FRUITS =
	{
		"Longan", "Pear", "Black currant", "Jujube", "Orange", "Avocado", "Lime", "Passion fruit", "Coconut", "Tangerine", "Goji berry",
		"Cherry", "Lychee", "Cranberry", "Prickly pear", "Banana", "Mandarin", "Loquat", "Blackberry", "Quince", "Apricot", "Grapefruit",
		"Dragonfruit", "Melon", "Papaya", "Jamun", "Apple", "Jackfruit", "Blueberry", "Watermelon", "Pineapple", "Lemon", "Grape",
		"Sapodilla", "Plum", "Mango", "Persimmon", "Nectarine", "Peach", "Raspberry", "Guava", "Strawberry", "Grapes", "Red currant", "Fig",
		"Mulberry", "Satsuma", "Palm fruit", "Olive", "Pomegranate", "Pumpkin", "Sweet lemon", "Kiwi", "Tamarind", "Dates"
	};
	private final static String[] COLOR_NAMES =
	{
		"AliceBlue", "AntiqueWhite", "Aqua", "Aquamarine", "Azure", "Beige", "Black", "BlanchedAlmond", "Blue", "BlueViolet", "Brown",
		"BurlyWood", "CadetBlue", "Chartreuse", "Chocolate", "Coral", "CornflowerBlue", "Cornsilk", "Crimson", "Cyan", "DarkBlue",
		"DarkCyan", "DarkGoldenRod", "DarkGray", "DarkGreen", "DarkGrey", "DarkKhaki", "DarkMagenta", "DarkOliveGreen", "DarkOrange",
		"DarkOrchid", "DarkRed", "DarkSalmon", "DarkSeaGreen", "DarkSlateBlue", "DarkSlateGray", "DarkSlateGrey", "DarkTurquoise",
		"DarkViolet", "DeepPink", "DeepSkyBlue", "DimGray", "DimGrey", "DodgerBlue", "FireBrick", "FloralWhite", "ForestGreen", "Fuchsia",
		"Gainsboro", "GhostWhite", "Gold", "GoldenRod", "Gray", "Green", "GreenYellow", "Grey", "HoneyDew", "HotPink", "IndianRed", "Indigo",
		"Ivory", "Khaki", "Lavender", "LavenderBlush", "LawnGreen", "LemonChiffon", "LightBlue", "LightCoral", "LightCyan",
		"LightGoldenRodYellow", "LightGray", "LightGreen", "LightGrey", "LightPink", "LightSalmon", "LightSeaGreen", "LightSkyBlue",
		"LightSlateGray", "LightSlateGrey", "LightSteelBlue", "LightYellow", "Lime", "LimeGreen", "Linen", "Magenta", "Maroon",
		"MediumAquaMarine", "MediumBlue", "MediumOrchid", "MediumPurple", "MediumSeaGreen", "MediumSlateBlue", "MediumSpringGreen",
		"MediumTurquoise", "MediumVioletRed", "MidnightBlue", "MintCream", "MistyRose", "Moccasin", "NavajoWhite", "Navy", "OldLace",
		"Olive", "OliveDrab", "Orange", "OrangeRed", "Orchid", "PaleGoldenRod", "PaleGreen", "PaleTurquoise", "PaleVioletRed", "PapayaWhip",
		"PeachPuff", "Peru", "Pink", "Plum", "PowderBlue", "Purple", "RebeccaPurple", "Red", "RosyBrown", "RoyalBlue", "SaddleBrown",
		"Salmon", "SandyBrown", "SeaGreen", "SeaShell", "Sienna", "Silver", "SkyBlue", "SlateBlue", "SlateGray", "SlateGrey", "Snow",
		"SpringGreen", "SteelBlue", "Tan", "Teal", "Thistle", "Tomato", "Turquoise", "Violet", "Wheat", "White", "WhiteSmoke", "Yellow",
		"YellowGreen"
	};
	private final static String[] FOOD_NAMES =
	{
		"Sesame chicken", "Nachos", "Seaweed salad", "Chili", "Pizza margherita", "Hummus", "Sweet potato fries", "Cheese quesadilla",
		"Chicken tenders", "Wonton soup", "Salmon avocado roll", "Garlic knots", "Bean burrito", "Cheesy fiesta potatoes", "Thai iced tea",
		"Milkshake", "Hamburger", "Chips and queso", "Traditional chicken wings", "Doughnuts", "White rice", "Tacos", "Chips and guacamole",
		"Crab rangoon", "Shrimp tempura roll", "Apple pie", "Cheese fries", "Greek salad", "Gyoza", "Spicy tuna roll", "Chicken sandwich",
		"Boneless chicken wings", "Onion rings", "Caesar salad", "Chicken tikka masala", "Waffle fries", "Macaroni and cheese",
		"Chicken quesadilla", "California roll", "Chicken nuggets", "Miso soup", "Edamame", "Garlic naan", "Mozzarella sticks", "Pad thai",
		"French fries", "Cheese pizza", "Hash browns", "Cheeseburger", "Burrito bowl"
	};
	private final static String[] COMPANY_NAMES =
	{
		"Clear Appeal", "Water Express", "West Barnes Pro", "Glass Empower", "Smarty Life", "Time Cop", "knoxfitness store", "Auto MAGMA",
		"Changing Faces", "Expansion Place", "Custom Extractors Ltd", "The Fetal Development", "Glass Advantage", "Ensure Bank",
		"Sapino Windows", "MagicBox", "Gold Dreams", "Glass Platinum", "Identa Windows", "Time on Your Side", "Regular Ticker",
		"Sales Market", "Zoey Copper", "The Big Dig Mining", "blucinematic stores", "Platinum Home", "SassySerene", "Continued Ontogeny",
		"Triple Play Mining", "Galaxy Mining", "PrimeFex Finance", "Alpha Shine", "Your Security First", "Rift Energy",
		"Trottego Custom Windows", "Maturation Place", "GioIntegrate Financing", "Signox Credit Services", "The Dollar Follow",
		"EquiFirst Capital", "Weary Wait", "The Whale View", "TinyWatch", "Sun Mortgage Co", "DelWen Custom Windows", "Panes And Frames",
		"Always On Time", "Foremost Global Supply", "TinyHelp Finance", "Falcon Mortgage Company", "Pinnacle Mines", "Hence Metal Windows",
		"Classic Glass", "Nocturnal Drone", "PerpetualWatch", "Precious Minutes", "Arctic Coal", "Wells Fargo Advisors", "So Refreshing",
		"Steamink stores", "IndustrialGrowth", "Finosure Mortgage", "Glass Bolt", "OpenBrook Financing", "ProFirst Mortgage", "Payback Pros",
		"High-Grade Processing", "Red Rock Windows", "Sparkle loan & Savings", "NewWatcher", "Glass Vista", "Transparent Funding",
		"Stop View", "Ore Wealth Corporation", "Cheeked Amazonas Place", "Treasure Plus", "Gold Bridge Mining", "Hydration Station",
		"Rex Supply", "SuperFront Windows", "Tint Visions", "Glass Alliance", "Heaven Dust", "Vegetative Expansion", "Horizon State Bank",
		"Iron Forge Mining", "Agrorays Industry", "anaconda glass", "LoanZone Trust Company", "Golden Miner", "Archway Home Lending",
		"Catenary Coal Co", "View Collective", "BlondAmazon", "Abundance Mining", "All star Glass", "Glass Total", "Double Observe Place",
		"Twin Oaks Mortgage", "King of Diamonds", "Breakwater Minerals Ltd", "Diamond Windows", "The Fierce", "Moments Count",
		"Galaxy mobiles", "The Lonely", "Water Empire", "Franco Nevada", "Golden Classic Mining", "KeenWatch", "Total Capital Index",
		"Glass Performance", "Clock Follow", "Coal Mining HQ", "Champion Windows", "Window Emporium", "Astra Bank", "BeautifulWatch",
		"Universal Brandz", "MotiveQuest Watch Co", "Pacific Coast Mortgage", "Premier Steel", "Best Windows", "Amazon Offerz",
		"White Gravity", "Chronomile", "Lower Armor", "District Wholesale", "Thick Growing", "The Swampy Armor", "Over the Earth",
		"American Choice", "Good View Collective", "Better In Bulk", "Best Sales", "vetra glass", "Tips from Kate", "Gold Fields",
		"urban cave", "Results Are Clear", "Camelot Amazon", "Unique Lending", "Beautiful Nintendo Co", "Anoka Capital Mortgage",
		"Galaxy Glass Tech", "The Right Loan for You", "SpaceWatch", "Astor Windows", "BrilliantClock", "SolitaryAmazon", "Ironclad Glass",
		"Fourclip magma", "Ameri-Mortgage Group Inc", "Titan Mortgage Company", "Clear Springs Water", "En Vogue Glass", "Carsmetic Tint",
		"Window Washers And Glass", "Great Western Bank", "Apex Platinum", "FrankWave Windows Co"
	};
	private final static String[] COUNTRY_NAMES =
	{
		"Afghanistan", "Albania", "Algeria", "Andorra", "Angola", "Antigua and Barbuda", "Argentina", "Armenia", "Australia", "Austria",
		"Azerbaijan", "Bahamas", "Bahrain", "Bangladesh", "Barbados", "Belarus", "Belgium", "Belize", "Benin", "Bhutan", "Bolivia",
		"Bosnia and Herzegovina", "Botswana", "Brazil", "Brunei", "Bulgaria", "Burkina Faso", "Burundi", "Côte d'Ivoire", "Cabo Verde",
		"Cambodia", "Cameroon", "Canada", "Central African Republic", "Chad", "Chile", "China", "Colombia", "Comoros",
		"Congo (Congo-Brazzaville)", "Costa Rica", "Croatia", "Cuba", "Cyprus", "Czechia (Czech Republic)",
		"Democratic Republic of the Congo", "Denmark", "Djibouti", "Dominica", "Dominican Republic", "Ecuador", "Egypt", "El Salvador",
		"Equatorial Guinea", "Eritrea", "Estonia", "Eswatini", "Ethiopia", "Fiji", "Finland", "France", "Gabon", "Gambia", "Georgia",
		"Germany", "Ghana", "Greece", "Grenada", "Guatemala", "Guinea", "Guinea-Bissau", "Guyana", "Haiti", "Holy See", "Honduras",
		"Hungary", "Iceland", "India", "Indonesia", "Iran", "Iraq", "Ireland", "Israel", "Italy", "Jamaica", "Japan", "Jordan", "Kazakhstan",
		"Kenya", "Kiribati", "Kuwait", "Kyrgyzstan", "Laos", "Latvia", "Lebanon", "Lesotho", "Liberia", "Libya", "Liechtenstein",
		"Lithuania", "Luxembourg", "Madagascar", "Malawi", "Malaysia", "Maldives", "Mali", "Malta", "Marshall Islands", "Mauritania",
		"Mauritius", "Mexico", "Micronesia", "Moldova", "Monaco", "Mongolia", "Montenegro", "Morocco", "Mozambique",
		"Myanmar (formerly Burma)", "Namibia", "Nauru", "Nepal", "Netherlands", "New Zealand", "Nicaragua", "Niger", "Nigeria",
		"North Korea", "North Macedonia", "Norway", "Oman", "Pakistan", "Palau", "Palestine State", "Panama", "Papua New Guinea", "Paraguay",
		"Peru", "Philippines", "Poland", "Portugal", "Qatar", "Romania", "Russia", "Rwanda", "Saint Kitts and Nevis", "Saint Lucia",
		"Saint Vincent and the Grenadines", "Samoa", "San Marino", "Sao Tome and Principe", "Saudi Arabia", "Senegal", "Serbia",
		"Seychelles", "Sierra Leone", "Singapore", "Slovakia", "Slovenia", "Solomon Islands", "Somalia", "South Africa", "South Korea",
		"South Sudan", "Spain", "Sri Lanka", "Sudan", "Suriname", "Sweden", "Switzerland", "Syria", "Tajikistan", "Tanzania", "Thailand",
		"Timor-Leste", "Togo", "Tonga", "Trinidad and Tobago", "Tunisia", "Turkey", "Turkmenistan", "Tuvalu", "Uganda", "Ukraine",
		"United Arab Emirates", "United Kingdom", "United States of America", "Uruguay", "Uzbekistan", "Vanuatu", "Venezuela", "Vietnam",
		"Yemen", "Zambia", "Zimbabwe"
	};
	private final static String[] LANGUAGE_NAMES =
	{
		"Abaza", "Abenaki", "Abkhaz", "Adyghe", "Afar", "Afrikaans", "Ainu", "Akan", "Albanian", "Aleut", "Amharic", "Apache", "Arabic",
		"Aragonese", "Aramaic (Ancient)", "Aramaic (Syriac)", "Aramaic (Neo-)", "Aranese", "Arapaho", "Argobba", "Armenian",
		"Aromanian (Vlach)", "Arrernte", "Assamese", "Asturian", "Avar", "Awngi", "Aymara", "Azerbaijani", "Balinese",
		"Balkar (Karachay-Balkar)", "Baluchi", "Bambara", "Bashkir", "Bassa", "Basque", "Beja", "Belarusian", "Bemba", "Bengali", "Berber",
		"Bhojpuri", "Blin", "Blackfoot", "Bosnian", "Breton", "Buginese", "Buhid", "Bulgarian", "Burmese", "Buryat", "Carrier", "Catalan",
		"Cayuga", "Cebuano", "Chagatai", "Chaha", "Chamorro", "Chechen", "Cherokee", "Cheyenne", "Chichewa", "Chickasaw", "Chipewyan",
		"Choctaw", "Comanche", "Cornish", "Corsican", "Cree", "Creek", "Croatian", "Czech", "Dakota", "Dangme", "Danish", "Dargwa", "Dari",
		"Dinka", "Dungan", "Dutch", "Dzongkha /", "Bhutanese", "Erzya", "Estonian", "Esperanto", "Ewe", "Eyak", "Faroese", "Fijian",
		"Finnish", "Flemish", "Fon", "French", "Frisian (North)", "Frisian (West)", "Friulan", "Fula", "Ga", "Galician", "Ganda", "Ge’ez",
		"Genoese", "Georgian", "German", "Godoberi", "Gooniyandi", "Greek", "Greenlandic", "Guernsey Norman", "Guarani", "Gujarati",
		"Gwich’in", "Haida", "Haitian Creole", "Hän", "Harari", "Hausa", "Hawaiian", "Hebrew", "Herero", "Hindi", "Hungarian", "Icelandic",
		"Igbo", "Ilocano", "Indonesian", "Ingush", "Inuktitut", "Iñupiaq", "Irish (Gaelic)", "Italian", "Japanese", "Javanese",
		"Jersey Norman", "Kabardian", "Kabyle", "Kaingang", "Kannada", "Kanuri", "Kapampangan", "Karakalpak", "Karelian", "Kashmiri",
		"Kashubian", "Kazakh", "Khakas", "Khmer", "Khoekhoe", "Kikuyu", "Kinyarwanda", "Kiribati", "Kirundi", "Komi", "Kongo", "Konkani",
		"Korean", "Kumyk", "Kurdish", "Kven", "Kwanyama", "Kyrgyz", "Ladin", "Ladino", "Lahnda", "Lakota", "Lao", "Latin", "Latvian", "Laz",
		"Lezgian", "Limburgish", "Lingala", "Lithuanian", "Livonian", "Lombard", "Low German/Low Saxon", "Luo", "Luxembourgish",
		"Maasai/Maa", "Macedonian", "Maldivian", "Maithili", "Malagasy", "Malay", "Malayalam", "Maltese", "Mandinka", "Manipuri", "Mansi",
		"Manx", "Maori", "Marathi", "Mari/Cheremis", "Marshallese", "Menominee", "Mirandese", "Mohawk", "Moksha", "Moldovan", "Mongolian",
		"Montagnais", "Nahuatl", "Naskapi", "Nauru", "Navajo", "Occitan", "Oshiwambo", "Nepali", "Newari", "Niuean", "Nogai", "Noongar",
		"Northern Sotho", "Norwegian", "Nyamwezi", "Nyoro", "Ojibwe", "O’odham", "Oriya", "Oromo", "Ossetian", "Palauan", "Pali",
		"Papiamento", "Pashto", "Persian", "Piedmontese", "Polish", "Portuguese", "Punjabi", "Quechua", "Raga", "Rapanui", "Rarotongan",
		"Romanian", "Romansh", "Romani", "Rotuman", "Russian", "Ruthenian", "Santali", "Samoan", "Sango", "Sanskrit", "Sardinian",
		"Sark Norman", "Scots", "Scottish Gaelic", "Selkup", "Serbian", "Shavante", "Shawnee", "Shona", "Shor", "Sicilian", "Sidamo",
		"Silesian", "Sindhi", "Sinhala", "Silt’e", "Slovak", "Slovenian", "Somali", "Soninke", "Sorbian (Lower)", "Sorbian (Upper)",
		"Southern Sotho", "South Slavey", "Spanish", "Sundanese", "Svan", "Swabish", "Swahili", "Swati", "Swedish", "Swiss German", "Syriac",
		"Tabassaran", "Tagalog", "Tahitian", "Tai Nüa", "Tajik", "Tamil", "Tatar", "Telugu", "Tetum", "Thai", "Tibetan", "Tigre", "Tigrinya",
		"Tlingit", "Tok Pisin", "Tonga", "Tongan", "Tsez", "Tsonga", "Tswana", "Tumbuka", "Turkish", "Turkmen", "Tuscarora", "Tuvaluan",
		"Tuvan", "Twi", "Udmurt", "Ukrainian", "Urdu", "Uyghur", "Uzbek", "Venda", "Venetian", "Veps", "Vietnamese", "Võro", "Votic",
		"Walloon", "Waray-Waray", "Welsh", "Wiradjuri", "Wolof", "Xamtanga", "Xhosa", "Yakut Sakha", "Yi", "Yiddish", "Yindjibarndi",
		"Yolngu", "Yoruba", "Yupik", "Zhuang", "Zulu", "Zuñi"
	};
	private final static String[] LOCATION_NAMES =
	{
		"Abasha", "Abu Dhabi", "Acapulco", "Addis Ababa", "Akrotiri", "Algiers", "Al Jahrah", "Amman", "Amsterdam", "Andorra La Vella",
		"Ankara", "Animas", "Antigua", "Assisi", "Athens", "Attopu", "Auckland", "Axim", "Baghdad", "Balls Pond", "Bangkok", "Barka",
		"Battambang", "Beijing", "Beirut", "Beltsy", "Berlin", "Berne", "Big Sandy", "Bitola", "Bluefields", "Bodden Town", "Bogota",
		"Bratislava", "Bridgetown", "Brussels", "Bucharest", "Budapest", "Buenos Aires", "Cairo", "Calabar", "Calydon", "Cannes", "Capellen",
		"Cape Town", "Caracas", "Carthage", "Casablanca", "Castletown", "Chittagong", "Cidra", "Cologne", "Constantine", "Copenhagen",
		"Cork", "Damascus", "Dangzai", "Danisher", "Darkhan", "Delhi", "Dori", "Dresden", "Dubai", "Edinburgh", "El Estor", "Fagatogo",
		"Fairy Dell", "Faro", "Freetown", "Geneina", "Geneva", "Georgetown", "Ghent", "Giza", "Gore", "Gothenburg", "Greymouth", "The Hague",
		"Halifax", "Hanoi", "Havana", "Helsinki", "High Rock", "Himera", "Hong Kong", "Honolulu", "Innsbruck", "Islamabad", "Istanbul",
		"Jakar", "Jakarta", "Jaww", "Jerusalem", "Kandy", "Kant", "Karachi", "Kathmandu", "Keflavik", "Kells", "Khiva", "Khost", "Kiev",
		"Killarney", "Kingston", "Kolari", "Kolding", "Kowloon", "Kuala Lumpur", "Kukes", "Kyoto", "Kythrea", "La Serena", "Leeds", "Lima",
		"Limerick", "London", "Lost", "Luxor", "Madrid", "Malaga", "Manila", "Macau", "Maputo", "Marbella", "Maribor", "Maripa", "Marrakesh",
		"Marseille", "Mecca", "Merthyr Tydfil", "Milan", "Minas", "Mogadishu", "Monte Carlo", "Moto'otua", "Moscow", "Mumbai", "Munich",
		"Naifaru", "Nairobi", "Nakasi 9 1/2 Miles", "Nazca", "Nazret", "Nord", "Odessa", "Omoka", "Oradea", "Oran", "Orange Walk", "Oxford",
		"Pagan", "Palmira", "Perth", "Pigg's Peak", "Piti", "Plymouth", "Point-Noire", "Port Royal", "Positano", "Prague", "Québec City",
		"Quetzaltenango", "Rakvere", "Rhodes Town", "Rio de Janeiro", "Riyadh", "Road Town", "Rome", "Rotterdam", "Sabaneta", "Saldus",
		"Salinas", "Samara", "San Lorenzo", "San Sebastian", "Sao Paulo", "Sarajevo", "Sarband", "Sekong", "Seoul", "Shanghai", "Shenzhen",
		"Siazan", "Šilute", "Sinpo", "Sisian", "Skien", "Sliven", "Snug Corner", "Sparta", "Strasbourg", "Stockholm", "Surat", "Taipei",
		"Taizz", "Tamboril", "Tehran", "Thunder Bay", "Toledo", "Toliara", "Tralee", "Trinidad", "Tripoli", "Tsavo", "Tutong", "Tyre",
		"Valletta", "Valparai", "Venice", "Veracruz", "Varrettes", "Vladivostok", "Wolfsberg", "Wuhu", "Xai-Xai", "Xilin Hot", "Xochimilco",
		"Yerba Buena", "Yokohama", "Zambezi", "Zaranj", "Zenica", "Zurich", "Zuwarah"
	};
	private final static String[] JOB_NAMES_PREFIX =
	{
		"Account", "Actor", "Actuary", "Administrative", "Admissions", "Aerospace", "Agriculture", "Animal", "Animal control",
		"Animal shelter", "Aquatic", "Architect", "Art", "Assistant", "B2B sales", "Barber", "Beautician", "Beekeeper", "Biochemist",
		"Biological", "Biologist", "Brand", "Breeder", "Budget", "Business", "Business development", "Business intelligence", "Call center",
		"Caregiver*", "Cashier*", "Casino", "Chemical", "Chief of", "Chief operations", "Civil", "Client", "Client services", "Collection",
		"College", "Compensation", "Compensations", "Compliance", "Computer", "Concierge", "Conservationist", "Content marketing",
		"Continuous improvement", "Cosmetology", "Cost", "Creative", "Credit", "Customer", "Customer care", "Customer service", "Dancer*",
		"Data", "Database", "Demand", "Demand generation", "Dental", "Dentist", "Dietitian", "Director", "Distribution", "Doctor*",
		"Editor*", "Electrical", "Electrologist", "English", "Environmental", "Esthetician*", "Executive", "Farming", "Fashion",
		"Fashion show", "Financial", "Financial services", "Flight attendant*", "Food", "Foresters*", "Front desk",
		"Front-end web developer*", "Geological", "Graphic", "Groomer*", "Guidance", "Hairdresser*", "Help desk", "Horticulturist", "Hotel",
		"Housekeeper*", "Human resources", "Human resources systems", "Illustrator", "Information security", "Instructional",
		"Insurance sales", "Investment banking", "IT", "Kennel", "Labor relations", "Librarian*", "Loan officer*", "Logistics", "Makeup",
		"Manager*", "Marine", "Marketing", "Math", "Mechanical", "Meeting", "Message", "Multimedia", "Nail", "Nuclear", "Nurse*",
		"Occupational therapy", "Office", "Operations", "Orthodontist*", "Painter*", "Personal", "Pet walker*", "Petroleum engineer*",
		"Pharmacist*", "Pharmacy", "Physical", "Plant biologist*", "Plant nursery", "Porter*", "President*", "Principal*", "Producer*",
		"Product", "Product marketing", "Project", "Public relations", "Real estate", "Regional sales", "Retail sales", "Safety", "Sales",
		"Salon manager*", "Science", "Senior process", "Service", "Singer*", "Skin care specialist*", "Social media", "Software",
		"Soil and plant", "Spa", "Store", "Substitute teacher*", "Superintendent*", "Supervisor*", "Supply chain", "Surgical",
		"Talent acquisition", "Tattoo artist*", "Team", "Technical support", "Telemarketer*", "Test", "Tour guide*", "Travel", "Tutor*",
		"Warehouse", "Web", "Wedding stylist*", "Veterinarian*", "Veterinary", "Vice principal*", "Wildlife", "Virtual assistant*",
		"Writer*", "Zoologist*"
	};
	private final static String[] JOB_NAMES_SUFFIX =
	{
		"adviser", "agent", "analyst", "architect", "artist", "assistant", "attendant", "biologist", "buyer", "care associate",
		"care specialist", "clerk", "consultant", "control officer", "coordinator", "designer", "developer", "development manager",
		"director", "ecologist", "estimator", "geneticist", "hygienist", "inspector", "instructor", "leader", "manager", "marketing manager",
		"media coordinator", "nutritionist", "ophthalmologist", "optimization specialist", "pathologist", "planner", "professor",
		"rehabilitator", "reporter", "representative", "resources consultant", "sales associate", "scientist", "scientist",
		"service manager", "services coordinator", "specialist", "stylist", "support representative", "teacher", "technician", "therapist",
		"trainer"
	};
	private final static String[] STATE_NAMES =
	{
		"Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado", "Connecticut", "Delaware", "Florida", "Georgia", "Hawaii",
		"Idaho", "Illinois", "Indiana", "Iowa", "Kansas", "Kentucky[D]", "Louisiana", "Maine", "Maryland", "Massachusetts[D]", "Michigan",
		"Minnesota", "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada", "NewHampshire", "NewJersey", "NewMexico", "NewYork",
		"NorthCarolina", "NorthDakota", "Ohio", "Oklahoma", "Oregon", "Pennsylvania[D]", "RhodeIsland", "SouthCarolina", "SouthDakota",
		"Tennessee", "Texas", "Utah", "Vermont", "Virginia[D]", "Washington", "WestVirginia", "Wisconsin", "Wyoming"
	};
	private final static String[] CITY_NAMES = LOCATION_NAMES;
	private final static String[] ADDRESS_NAMES_PREFIX =
	{
		"Adah", "Adeline", "Aglae", "Alessandra", "Anderson", "Ankunding", "Arjun", "Armani", "Arvel", "Austen", "Balistreri", "Barrows",
		"Bartell", "Bartoletti", "Baumbach", "Bechtelar", "Berge", "Bergstrom", "Bernadine", "Boyer", "Brenden", "Brett", "Brody", "Casper",
		"Chelsie", "Christophe", "Darby", "Darrel", "Daryl", "David", "Delbert", "Delphine", "Dibbert", "Dock", "Domenic", "Dooley",
		"Dorcas", "Drake", "Dulce", "Earnest", "Elliott", "Elva", "Emard", "Emiliano", "Emmerich", "Emmitt", "Erdman", "Ethan", "Fadel",
		"Favian", "Filiberto", "Fiona", "Franecki", "Friesen", "Garrison", "Gaylord", "Geoffrey", "Geovanni", "Gibson", "Goldner", "Goldner",
		"Grimes", "Gusikowski", "Haleigh", "Haley", "Hammes", "Hand", "Harvey", "Heaney", "Hilton", "Hoeger", "Holden", "Hoppe", "Hosea",
		"Jacinto", "Jackson", "Jaleel", "Jamel", "Jamir", "Jaskolski", "Jaydon", "Jennings", "Jerry", "Julian", "Justus", "Kassulke",
		"Kemmer", "Kiehn", "Kunze", "Labadie", "Langosh", "Langworth", "Lemke", "Lesch", "Leuschke", "Levi", "Little", "Lockman", "Lucious",
		"Maggio", "Malvina", "Mann", "Marc", "Maximilian", "Mayer", "McKenzie", "McLaughlin", "Merl", "Mertz", "Micaela", "Miller",
		"Minerva", "Mohr", "Mraz", "Murazik", "Myra", "Myrtice", "Nash", "Nienow", "Nikita", "Nitzsche", "Noe", "O'Kon", "Orval", "Parisian",
		"Parker", "Pfeffer", "Pollich", "Pouros", "Powlowski", "Quitzon", "Rashawn", "Raymundo", "Reese", "Rempel", "Rice", "Rippin",
		"Rodriguez", "Rogelio", "Rowan", "Royal", "Runte", "Russell", "Rutherford", "Sanford", "Satterfield", "Savannah", "Schmidt",
		"Schumm", "Sebastian", "Senger", "Shyann", "Simone", "Sipes", "Stanton", "Stracke", "Susan", "Swaniawski", "Sven", "Swift", "Thiel",
		"Tiana", "Torp", "Torphy", "Towne", "Turcotte", "Ulices", "Ulises", "Ullrich", "Upton", "Walsh", "Webster", "Westley", "Wiegand",
		"Wilford", "Wilkinson", "Wolff", "Von", "Wunsch", "Wyman", "Yasmine", "Zita"
	};
	private final static String[] ADDRESS_NAMES_SUFFIX =
	{
		"Bridge", "Brooks", "Bypass", "Cape", "Causeway", "Center", "Common", "Corners", "Course", "Court", "Courts", "Cove", "Coves",
		"Creek", "Crest", "Dam", "Drive", "Drives", "Estate", "Estates", "Expressway", "Extension", "Extensions", "Fall", "Falls", "Field",
		"Fields", "Flat", "Flats", "Forest", "Forge", "Forges", "Fork", "Fort", "Freeway", "Gardens", "Glen", "Greens", "Grove", "Groves",
		"Harbor", "Harbors", "Inlet", "Island", "Islands", "Isle", "Junctions", "Keys", "Knoll", "Lakes", "Land", "Landing", "Lane",
		"Lights", "Loaf", "Locks", "Lodge", "Manors", "Meadow", "Meadows", "Mills", "Mission", "Motorway", "Mountain", "Mountains", "Neck",
		"Oval", "Parkway", "Parkways", "Pass", "Passage", "Path", "Pine", "Pines", "Place", "Plain", "Plaza", "Plaza", "Point", "Points",
		"Port", "Ports", "Prairie", "Radial", "Ramp", "Rapid", "Rapids", "Roads", "Row", "Run", "Shore", "Shores", "Skyway", "Springs",
		"Spur", "Spurs", "Squares", "Station", "Stravenue", "Stream", "Street", "Summit", "Terrace", "Throughway", "Trace", "Trafficway",
		"Trail", "Turnpike", "Underpass", "Union", "Walk", "Valley", "Valleys", "Way", "Well", "Wells", "Via", "Viaduct", "Views", "Views",
		"Village", "Villages", "Vista", "Vista"
	};
	private final static String[] STREET_NAMES_PREFIX =
	{
		"Amber", "Angel", "Art", "Ash", "Bath", "Bay", "Beach", "Beachside", "Boulder", "Bridgeway", "Broad", "Brown", "Castle", "Central",
		"Chestnut", "Commercial", "Crescent", "Dew", "Dew", "Duchess", "Emerald", "Frost", "Grand", "Gray", "Haven", "Heart", "Heirloom",
		"Hind", "Ironwood", "Jade", "Java", "Knight", "Lavender", "Liberty", "Lilypad", "Lotus", "Low", "Love", "Lower", "Manor", "Meadow",
		"Merchant", "Museum", "New", "Oceanview", "Oval", "Pearl", "Phoenix", "Pioneer", "Princess", "Prospect", "Queen", "Redwood", "Rose",
		"Rosemary", "Saffron", "School", "Seacoast", "Star", "Station", "Temple", "Theater", "Trinity", "Union", "Upper", "Walnut", "Water",
		"West", "Wharf"
	};
	private final static String[] STREET_NAMES_SUFFIX =
	{
		"Avenue", "Boulevard", "Lane", "Passage", "Route", "Row", "Street", "Way"
	};
	private final static String[] NICK_NAMES =
	{
		"ada","ben", "adelard", "aglaran", "alcarinion", "amarthior", "amathiphant", "amdirthorn", "amlugeden", "amluginnog", "arahaeldaer",
		"ardir", "arlin", "avar", "awahairo", "bairrfhionn", "balen", "bathron", "bellamdir", "bercilak", "bregol", "brogamon", "calaeron",
		"cemmion", "cevion", "cidinnamarth", "cody", "costaro", "dagion", "delane", "deldhinion", "díllothanar", "dolchanar", "eccesindion",
		"edenor", "elanordil", "elgine", "elvedui", "engwo", "estolaben", "faeldir", "falch", "gaeardaer", "galadphen", "gathrodion",
		"gorvenal", "gurunam", "harnion", "heledthor", "hending", "hithuven", "hwandil", "ianion", "iphanthon", "kelleher", "lalaro",
		"lanwatan", "lassion", "laurealasso", "lavamben", "lennion", "leodegan", "lindaro", "loendir", "lueius", "lun", "maeluion",
		"maicion", "melrion", "minasdir", "moriarty", "morthor", "mundil", "nadhorchanar", "nestoron", "níco", "níthor", "norother",
		"nurtaro", "ogolben", "oleryd", "pamben", "pelleas", "racandur", "raicion", "sawyl", "sulben", "telioron", "thalachon", "pamo",
		"tharbachon", "tinwendur", "traherne", "ulund", "ulunion", "vane", "vanisauro", "vanya", "wilindion", "wiryaro", "yaro"
	};
	private final static String[] MAIL_HOSTS =
	{
		"google.com", "hotmail.com", "outlook.com", "titan.com", "protonmail.com", "yahoo.com", "zoho.com"
	};
	private final static String[] ROLES = new String[]
	{
		"Administrator", "Employee", "Manager"
	};
	private final static String[] TEAMS = new String[]
	{
		"Frontend", "Backend", "Supervisor", "QA"
	};


	public static Document createPerson(Random rnd)
	{
		int gender = rnd.nextInt(2);

		String firstName = createFirstName(gender, rnd);
		String lastName = createLastName(rnd);
		String company = createCompany(rnd);

		return new Document()
			.put("_id", ObjectId.randomId())
			.put("createDateTime", createNewDate(rnd))
			.put("changeDateTime", createNewDate(rnd))
			.put("version", rnd.nextInt(1000))
			.put("personal", new Document()
				.put("givenName", firstName)
				.put("surname", lastName)
				.put("gender", gender == 0 ? "Male" : "Female")
				.put("language", createLanguage(rnd))
				.put("birthday", createBirthdate(rnd))
				.put("displayName", createNickName(rnd))
				.put("healthInfo", createHealthInfo(rnd))
				.put("contacts", new Array()
					.add(new Document().put("type", "email").put("text", createEmail(rnd)))
					.add(new Document().put("type", "phone").put("text", createPhoneNumber(rnd)))
					.add(new Document().put("type", "mobilePhone").put("text", createPhoneNumber(rnd)))
				)
				.put("home", new Document()
					.put("street", createStreet(rnd))
					.put("address", createAddress(rnd))
					.put("postalCode", createPostalCode(rnd))
					.put("city", createCity(rnd))
					.put("state", createState(rnd))
					.put("country", createCountry(rnd))
				)
				.put("favorite", new Document()
					.put("color", createColorName(rnd))
					.put("fruit", createFruit(rnd))
					.put("number", rnd.nextInt(100))
					.put("food", createFoodName(rnd))
				)
				.put("account_balance", new BigDecimal((rnd.nextInt() % 10000) + "." + Math.abs(rnd.nextInt())))
			)
			.put("work", new Document()
				.put("company", company)
				.put("role", ROLES[rnd.nextInt(ROLES.length)])
				.put("team", TEAMS[rnd.nextInt(TEAMS.length)])
				.put("usageLocation", createLocation(rnd))
				.put("jobTitle", createJob(rnd))
				.put("contacts", new Array()
					.add(new Document().put("type", "email").put("text", (firstName + "." + lastName + "@" + company + ".com").toLowerCase().replace(" ", "_")))
					.add(new Document().put("type", "phone").put("text", createPhoneNumber(rnd)))
					.add(new Document().put("type", "mobilePhone").put("text", createPhoneNumber(rnd)))
				)
				.put("token", UUID.randomUUID())
			)
			.put("locationHistory", createGPSHistory(rnd));
	}


	private static LocalDateTime createNewDate(Random rnd)
	{
		return LocalDateTime.of(2000 + rnd.nextInt(20), 1 + rnd.nextInt(12), 1 + rnd.nextInt(28), rnd.nextInt(24), rnd.nextInt(60), rnd.nextInt(60));
	}


	private static LocalDate createBirthdate(Random rnd)
	{
		return LocalDate.of(1960 + rnd.nextInt(20), 1 + rnd.nextInt(12), 1 + rnd.nextInt(28));
	}


	private static String createFirstName(int aGender, Random rnd)
	{
		String[] tmp = FIRST_NAMES[aGender];
		return tmp[rnd.nextInt(tmp.length)];
	}


	private static String createLastName(Random rnd)
	{
		return LAST_NAMES[rnd.nextInt(LAST_NAMES.length)];
	}


	private static String createFruit(Random rnd)
	{
		return FRUITS[rnd.nextInt(FRUITS.length)];
	}


	private static String createColorName(Random rnd)
	{
		return COLOR_NAMES[rnd.nextInt(COLOR_NAMES.length)];
	}


	private static String createFoodName(Random rnd)
	{
		return FOOD_NAMES[rnd.nextInt(FOOD_NAMES.length)];
	}


	private static String createCompany(Random rnd)
	{
		return COMPANY_NAMES[rnd.nextInt(COMPANY_NAMES.length)];
	}


	private static String createCountry(Random rnd)
	{
		return COUNTRY_NAMES[rnd.nextInt(COUNTRY_NAMES.length)];
	}


	private static String createLanguage(Random rnd)
	{
		return LANGUAGE_NAMES[rnd.nextInt(LANGUAGE_NAMES.length)];
	}


	private static String createLocation(Random rnd)
	{
		return LOCATION_NAMES[rnd.nextInt(LOCATION_NAMES.length)];
	}


	private static String createJob(Random rnd)
	{
		String prefix = JOB_NAMES_PREFIX[rnd.nextInt(JOB_NAMES_PREFIX.length)];
		return prefix.endsWith("*") ? prefix.substring(0, prefix.length() - 1) : prefix + " " + JOB_NAMES_SUFFIX[rnd.nextInt(JOB_NAMES_SUFFIX.length)];
	}


	private static String createState(Random rnd)
	{
		return STATE_NAMES[rnd.nextInt(STATE_NAMES.length)];
	}


	private static String createCity(Random rnd)
	{
		return CITY_NAMES[rnd.nextInt(CITY_NAMES.length)];
	}


	private static String createPostalCode(Random rnd)
	{
		return "" + rnd.nextInt(10) + rnd.nextInt(10) + rnd.nextInt(10) + " " + rnd.nextInt(10) + rnd.nextInt(10);
	}


	private static String createAddress(Random rnd)
	{
		return ADDRESS_NAMES_PREFIX[rnd.nextInt(ADDRESS_NAMES_PREFIX.length)] + " " + ADDRESS_NAMES_SUFFIX[rnd.nextInt(ADDRESS_NAMES_SUFFIX.length)];
	}


	private static String createStreet(Random rnd)
	{
		return STREET_NAMES_PREFIX[rnd.nextInt(STREET_NAMES_PREFIX.length)] + " " + STREET_NAMES_SUFFIX[rnd.nextInt(STREET_NAMES_SUFFIX.length)];
	}


	private static String createEmail(Random rnd)
	{
		return createNickName(rnd) + "@" + MAIL_HOSTS[rnd.nextInt(MAIL_HOSTS.length)];
	}


	private static String createNickName(Random rnd)
	{
		String tmp = "";
		tmp += NICK_NAMES[rnd.nextInt(NICK_NAMES.length)];
		tmp += "_";
		tmp += NICK_NAMES[rnd.nextInt(NICK_NAMES.length)];
		tmp += rnd.nextInt(100);
		return tmp;
	}


	private static String createPhoneNumber(Random rnd)
	{
		String tmp = "0";
		tmp += (char)('1' + rnd.nextInt(9));
		tmp += (char)('0' + rnd.nextInt(10));
		tmp += "-";
		for (int i = 0; i < 10; i++)
		{
			tmp += (char)('0' + rnd.nextInt(10));
		}
		return tmp;
	}


	private static Document createGPS(Random rnd)
	{
		return new Document().put("lat", 90 * rnd.nextFloat()).put("lng", 90 * rnd.nextFloat());
	}


	private static Document createGPSTime(Random rnd)
	{
		return createGPS(rnd).put("time", OffsetDateTime.of(createNewDate(rnd), ZoneOffset.ofHours(rnd.nextInt(20) - 10)));
	}


	private static Object createGPSHistory(Random rnd)
	{
		Array tmp = new Array();
		for (int i = 2 + rnd.nextInt(9); --i >= 0;)
		{
			tmp.add(createGPSTime(rnd));
		}
		return tmp;
	}


	private static Document createHealthInfo(Random rnd)
	{
		Document tmp = new Document();
		tmp.put("weight", 50 + rnd.nextInt(70));
		tmp.put("height", 140 + rnd.nextInt(70));
		tmp.put("bloodType", new String[]{"A+","A-","B+","B-","AB+","AB-","O+","O-"}[rnd.nextInt(8)]);

		return tmp;
	}


	public static void main(String ... args)
	{
		try
		{
			Document person = _Person.createPerson(new Random());

			System.out.println(person);

			System.out.println("-".repeat(100));
			System.out.println("JSON: " + person.toJson().length());
			System.out.println("TSON: " + person.toTypedJson().length());
			System.out.println("BIN:  " + person.toByteArray().length);

			Logger.getLogger().hexDump(Level.ALL, person.toByteArray());
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.out);
		}
	}
}
