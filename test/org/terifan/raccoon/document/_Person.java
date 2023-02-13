package org.terifan.raccoon.document;

import java.time.LocalDateTime;
import java.util.Random;


public class _Person
{
	private final static String[][] FIRST_NAMES = {
		{"James","Robert","John","Michael","David","William","Richard","Joseph","Thomas","Charles","Christopher","Daniel","Matthew","Anthony","Mark","Donald","Steven","Paul","Andrew","Joshua","Kenneth","Kevin","Brian","George","Timothy","Ronald","Edward","Jason","Jeffrey","Ryan","Jacob","Gary","Nicholas","Eric","Jonathan","Stephen","Larry","Justin","Scott","Brandon","Benjamin","Samuel","Gregory","Alexander","Frank","Patrick","Raymond","Jack","Dennis","Jerry","Tyler","Aaron","Jose","Adam","Nathan","Henry","Douglas","Zachary","Peter","Kyle","Ethan","Walter","Noah","Jeremy","Christian","Keith","Roger","Terry","Gerald","Harold","Sean","Austin","Carl","Arthur","Lawrence","Dylan","Jesse","Jordan","Bryan","Billy","Joe","Bruce","Gabriel","Logan","Albert","Willie","Alan","Juan","Wayne","Elijah","Randy","Roy","Vincent","Ralph","Eugene","Russell","Bobby","Mason","Philip","Louis"},
		{"Mary","Patricia","Jennifer","Linda","Elizabeth","Barbara","Susan","Jessica","Sarah","Karen","Lisa","Nancy","Betty","Margaret","Sandra","Ashley","Kimberly","Emily","Donna","Michelle","Carol","Amanda","Dorothy","Melissa","Deborah","Stephanie","Rebecca","Sharon","Laura","Cynthia","Kathleen","Amy","Angela","Shirley","Anna","Brenda","Pamela","Emma","Nicole","Helen","Samantha","Katherine","Christine","Debra","Rachel","Carolyn","Janet","Catherine","Maria","Heather","Diane","Ruth","Julie","Olivia","Joyce","Virginia","Victoria","Kelly","Lauren","Christina","Joan","Evelyn","Judith","Megan","Andrea","Cheryl","Hannah","Jacqueline","Martha","Gloria","Teresa","Ann","Sara","Madison","Frances","Kathryn","Janice","Jean","Abigail","Alice","Julia","Judy","Sophia","Grace","Denise","Amber","Doris","Marilyn","Danielle","Beverly","Isabella","Theresa","Diana","Natalie","Brittany","Charlotte","Marie","Kayla","Alexis","Lori"}
	};
	private final static String[] LAST_NAMES = {"Smith","Johnson","Williams","Brown","Jones","Garcia","Miller","Davis","Rodriguez","Martinez","Hernandez","Lopez","Gonzalez","Wilson","Anderson","Thomas","Taylor","Moore","Jackson","Martin","Lee","Perez","Thompson","White","Harris","Sanchez","Clark","Ramirez","Lewis","Robinson","Walker","Young","Allen","King","Wright","Scott","Torres","Nguyen","Hill","Flores","Green","Adams","Nelson","Baker","Hall","Rivera","Campbell","Mitchell","Carter","Roberts","Gomez","Phillips","Evans","Turner","Diaz","Parker","Cruz","Edwards","Collins","Reyes","Stewart","Morris","Morales","Murphy","Cook","Rogers","Gutierrez","Ortiz","Morgan","Cooper","Peterson","Bailey","Reed","Kelly","Howard","Ramos","Kim","Cox","Ward","Richardson","Watson","Brooks","Chavez","Wood","James","Bennett","Gray","Mendoza","Ruiz","Hughes","Price","Alvarez","Castillo","Sanders","Patel","Myers","Long","Ross","Foster","Jimenez"};
	private final static String[] FRUITS = {"Longan","Pear","Black currant","Jujube","Orange","Avocado","Lime","Passion fruit","Coconut","Tangerine","Goji berry","Cherry","Lychee","Cranberry","Prickly pear","Banana","Mandarin","Loquat","Blackberry","Quince","Apricot","Grapefruit","Dragonfruit","Melon","Papaya","Jamun","Apple","Jackfruit","Blueberry","Watermelon","Pineapple","Lemon","Grape","Sapodilla","Plum","Mango","Persimmon","Nectarine","Peach","Raspberry","Guava","Strawberry","Grapes","Red currant","Fig","Mulberry","Satsuma","Palm fruit","Olive","Pomegranate","Pumpkin","Sweet lemon","Kiwi","Tamarind","Dates"};
	private final static String[] COLOR_NAMES = {"AliceBlue","AntiqueWhite","Aqua","Aquamarine","Azure","Beige","Black","BlanchedAlmond","Blue","BlueViolet","Brown","BurlyWood","CadetBlue","Chartreuse","Chocolate","Coral","CornflowerBlue","Cornsilk","Crimson","Cyan","DarkBlue","DarkCyan","DarkGoldenRod","DarkGray","DarkGreen","DarkGrey","DarkKhaki","DarkMagenta","DarkOliveGreen","DarkOrange","DarkOrchid","DarkRed","DarkSalmon","DarkSeaGreen","DarkSlateBlue","DarkSlateGray","DarkSlateGrey","DarkTurquoise","DarkViolet","DeepPink","DeepSkyBlue","DimGray","DimGrey","DodgerBlue","FireBrick","FloralWhite","ForestGreen","Fuchsia","Gainsboro","GhostWhite","Gold","GoldenRod","Gray","Green","GreenYellow","Grey","HoneyDew","HotPink","IndianRed","Indigo","Ivory","Khaki","Lavender","LavenderBlush","LawnGreen","LemonChiffon","LightBlue","LightCoral","LightCyan","LightGoldenRodYellow","LightGray","LightGreen","LightGrey","LightPink","LightSalmon","LightSeaGreen","LightSkyBlue","LightSlateGray","LightSlateGrey","LightSteelBlue","LightYellow","Lime","LimeGreen","Linen","Magenta","Maroon","MediumAquaMarine","MediumBlue","MediumOrchid","MediumPurple","MediumSeaGreen","MediumSlateBlue","MediumSpringGreen","MediumTurquoise","MediumVioletRed","MidnightBlue","MintCream","MistyRose","Moccasin","NavajoWhite","Navy","OldLace","Olive","OliveDrab","Orange","OrangeRed","Orchid","PaleGoldenRod","PaleGreen","PaleTurquoise","PaleVioletRed","PapayaWhip","PeachPuff","Peru","Pink","Plum","PowderBlue","Purple","RebeccaPurple","Red","RosyBrown","RoyalBlue","SaddleBrown","Salmon","SandyBrown","SeaGreen","SeaShell","Sienna","Silver","SkyBlue","SlateBlue","SlateGray","SlateGrey","Snow","SpringGreen","SteelBlue","Tan","Teal","Thistle","Tomato","Turquoise","Violet","Wheat","White","WhiteSmoke","Yellow","YellowGreen"};
	private final static String[] FOOD_NAMES = {"Sesame chicken","Nachos","Seaweed salad","Chili","Pizza margherita","Hummus","Sweet potato fries","Cheese quesadilla","Chicken tenders","Wonton soup","Salmon avocado roll","Garlic knots","Bean burrito","Cheesy fiesta potatoes","Thai iced tea","Milkshake","Hamburger","Chips and queso","Traditional chicken wings","Doughnuts","White rice","Tacos","Chips and guacamole","Crab rangoon","Shrimp tempura roll","Apple pie","Cheese fries","Greek salad","Gyoza","Spicy tuna roll","Chicken sandwich","Boneless chicken wings","Onion rings","Caesar salad","Chicken tikka masala","Waffle fries","Macaroni and cheese","Chicken quesadilla","California roll","Chicken nuggets","Miso soup","Edamame","Garlic naan","Mozzarella sticks","Pad thai","French fries","Cheese pizza","Hash browns","Cheeseburger","Burrito bowl"};
	private final static String[] COMPANY_NAMES = {"Clear Appeal","Water Express","West Barnes Pro","Glass Empower","Smarty Life","Time Cop","knoxfitness store","Auto MAGMA","Changing Faces","Expansion Place","Custom Extractors Ltd","The Fetal Development","Glass Advantage","Ensure Bank","Sapino Windows","MagicBox","Gold Dreams","Glass Platinum","Identa Windows","Time on Your Side","Regular Ticker","Sales Market","Zoey Copper","The Big Dig Mining","blucinematic stores","Platinum Home","SassySerene","Continued Ontogeny","Triple Play Mining","Galaxy Mining","PrimeFex Finance","Alpha Shine","Your Security First","Rift Energy","Trottego Custom Windows","Maturation Place","GioIntegrate Financing","Signox Credit Services","The Dollar Follow","EquiFirst Capital","Weary Wait","The Whale View","TinyWatch","Sun Mortgage Co","DelWen Custom Windows","Panes And Frames","Always On Time","Foremost Global Supply","TinyHelp Finance","Falcon Mortgage Company","Pinnacle Mines","Hence Metal Windows","Classic Glass","Nocturnal Drone","PerpetualWatch","Precious Minutes","Arctic Coal","Wells Fargo Advisors","So Refreshing","Steamink stores","IndustrialGrowth","Finosure Mortgage","Glass Bolt","OpenBrook Financing","ProFirst Mortgage","Payback Pros","High-Grade Processing","Red Rock Windows","Sparkle loan & Savings","NewWatcher","Glass Vista","Transparent Funding","Stop View","Ore Wealth Corporation","Cheeked Amazonas Place","Treasure Plus","Gold Bridge Mining","Hydration Station","Rex Supply","SuperFront Windows","Tint Visions","Glass Alliance","Heaven Dust","Vegetative Expansion","Horizon State Bank","Iron Forge Mining","Agrorays Industry","anaconda glass","LoanZone Trust Company","Golden Miner","Archway Home Lending","Catenary Coal Co","View Collective","BlondAmazon","Abundance Mining","All star Glass","Glass Total","Double Observe Place","Twin Oaks Mortgage","King of Diamonds","Breakwater Minerals Ltd","Diamond Windows","The Fierce","Moments Count","Galaxy mobiles","The Lonely","Water Empire","Franco Nevada","Golden Classic Mining","KeenWatch","Total Capital Index","Glass Performance","Clock Follow","Coal Mining HQ","Champion Windows","Window Emporium","Astra Bank","BeautifulWatch","Universal Brandz","MotiveQuest Watch Co","Pacific Coast Mortgage","Premier Steel","Best Windows","Amazon Offerz","White Gravity","Chronomile","Lower Armor","District Wholesale","Thick Growing","The Swampy Armor","Over the Earth","American Choice","Good View Collective","Better In Bulk","Best Sales","vetra glass","Tips from Kate","Gold Fields","urban cave","Results Are Clear","Camelot Amazon","Unique Lending","Beautiful Nintendo Co","Anoka Capital Mortgage","Galaxy Glass Tech","The Right Loan for You","SpaceWatch","Astor Windows","BrilliantClock","SolitaryAmazon","Ironclad Glass","Fourclip magma","Ameri-Mortgage Group Inc","Titan Mortgage Company","Clear Springs Water","En Vogue Glass","Carsmetic Tint","Window Washers And Glass","Great Western Bank","Apex Platinum","FrankWave Windows Co"};
	private final static String[] COUNTRY_NAMES = {"Afghanistan","Albania","Algeria","Andorra","Angola","Antigua and Barbuda","Argentina","Armenia","Australia","Austria","Azerbaijan","Bahamas","Bahrain","Bangladesh","Barbados","Belarus","Belgium","Belize","Benin","Bhutan","Bolivia","Bosnia and Herzegovina","Botswana","Brazil","Brunei","Bulgaria","Burkina Faso","Burundi","Côte d'Ivoire","Cabo Verde","Cambodia","Cameroon","Canada","Central African Republic","Chad","Chile","China","Colombia","Comoros","Congo (Congo-Brazzaville)","Costa Rica","Croatia","Cuba","Cyprus","Czechia (Czech Republic)","Democratic Republic of the Congo","Denmark","Djibouti","Dominica","Dominican Republic","Ecuador","Egypt","El Salvador","Equatorial Guinea","Eritrea","Estonia","Eswatini","Ethiopia","Fiji","Finland","France","Gabon","Gambia","Georgia","Germany","Ghana","Greece","Grenada","Guatemala","Guinea","Guinea-Bissau","Guyana","Haiti","Holy See","Honduras","Hungary","Iceland","India","Indonesia","Iran","Iraq","Ireland","Israel","Italy","Jamaica","Japan","Jordan","Kazakhstan","Kenya","Kiribati","Kuwait","Kyrgyzstan","Laos","Latvia","Lebanon","Lesotho","Liberia","Libya","Liechtenstein","Lithuania","Luxembourg","Madagascar","Malawi","Malaysia","Maldives","Mali","Malta","Marshall Islands","Mauritania","Mauritius","Mexico","Micronesia","Moldova","Monaco","Mongolia","Montenegro","Morocco","Mozambique","Myanmar (formerly Burma)","Namibia","Nauru","Nepal","Netherlands","New Zealand","Nicaragua","Niger","Nigeria","North Korea","North Macedonia","Norway","Oman","Pakistan","Palau","Palestine State","Panama","Papua New Guinea","Paraguay","Peru","Philippines","Poland","Portugal","Qatar","Romania","Russia","Rwanda","Saint Kitts and Nevis","Saint Lucia","Saint Vincent and the Grenadines","Samoa","San Marino","Sao Tome and Principe","Saudi Arabia","Senegal","Serbia","Seychelles","Sierra Leone","Singapore","Slovakia","Slovenia","Solomon Islands","Somalia","South Africa","South Korea","South Sudan","Spain","Sri Lanka","Sudan","Suriname","Sweden","Switzerland","Syria","Tajikistan","Tanzania","Thailand","Timor-Leste","Togo","Tonga","Trinidad and Tobago","Tunisia","Turkey","Turkmenistan","Tuvalu","Uganda","Ukraine","United Arab Emirates","United Kingdom","United States of America","Uruguay","Uzbekistan","Vanuatu","Venezuela","Vietnam","Yemen","Zambia","Zimbabwe"};
	private final static String[] LANGUAGE_NAMES = {"Abaza","Abenaki","Abkhaz","Adyghe","Afar","Afrikaans","Ainu","Akan","Albanian","Aleut","Amharic","Apache","Arabic","Aragonese","Aramaic (Ancient)","Aramaic (Syriac)","Aramaic (Neo-)","Aranese","Arapaho","Argobba","Armenian","Aromanian (Vlach)","Arrernte","Assamese","Asturian","Avar","Awngi","Aymara","Azerbaijani","Balinese","Balkar (Karachay-Balkar)","Baluchi","Bambara","Bashkir","Bassa","Basque","Beja","Belarusian","Bemba","Bengali","Berber","Bhojpuri","Blin","Blackfoot","Bosnian","Breton","Buginese","Buhid","Bulgarian","Burmese","Buryat","Carrier","Catalan","Cayuga","Cebuano","Chagatai","Chaha","Chamorro","Chechen","Cherokee","Cheyenne","Chichewa","Chickasaw","Chipewyan","Choctaw","Comanche","Cornish","Corsican","Cree","Creek","Croatian","Czech","Dakota","Dangme","Danish","Dargwa","Dari","Dinka","Dungan","Dutch","Dzongkha /","Bhutanese","Erzya","Estonian","Esperanto","Ewe","Eyak","Faroese","Fijian","Finnish","Flemish","Fon","French","Frisian (North)","Frisian (West)","Friulan","Fula","Ga","Galician","Ganda","Ge’ez","Genoese","Georgian","German","Godoberi","Gooniyandi","Greek","Greenlandic","Guernsey Norman","Guarani","Gujarati","Gwich’in","Haida","Haitian Creole","Hän","Harari","Hausa","Hawaiian","Hebrew","Herero","Hindi","Hungarian","Icelandic","Igbo","Ilocano","Indonesian","Ingush","Inuktitut","Iñupiaq","Irish (Gaelic)","Italian","Japanese","Javanese","Jersey Norman","Kabardian","Kabyle","Kaingang","Kannada","Kanuri","Kapampangan","Karakalpak","Karelian","Kashmiri","Kashubian","Kazakh","Khakas","Khmer","Khoekhoe","Kikuyu","Kinyarwanda","Kiribati","Kirundi","Komi","Kongo","Konkani","Korean","Kumyk","Kurdish","Kven","Kwanyama","Kyrgyz","Ladin","Ladino","Lahnda","Lakota","Lao","Latin","Latvian","Laz","Lezgian","Limburgish","Lingala","Lithuanian","Livonian","Lombard","Low German/Low Saxon","Luo","Luxembourgish","Maasai/Maa","Macedonian","Maldivian","Maithili","Malagasy","Malay","Malayalam","Maltese","Mandinka","Manipuri","Mansi","Manx","Maori","Marathi","Mari/Cheremis","Marshallese","Menominee","Mirandese","Mohawk","Moksha","Moldovan","Mongolian","Montagnais","Nahuatl","Naskapi","Nauru","Navajo","Occitan","Oshiwambo","Nepali","Newari","Niuean","Nogai","Noongar","Northern Sotho","Norwegian","Nyamwezi","Nyoro","Ojibwe","O’odham","Oriya","Oromo","Ossetian","Palauan","Pali","Papiamento","Pashto","Persian","Piedmontese","Polish","Portuguese","Punjabi","Quechua","Raga","Rapanui","Rarotongan","Romanian","Romansh","Romani","Rotuman","Russian","Ruthenian","Santali","Samoan","Sango","Sanskrit","Sardinian","Sark Norman","Scots","Scottish Gaelic","Selkup","Serbian","Shavante","Shawnee","Shona","Shor","Sicilian","Sidamo","Silesian","Sindhi","Sinhala","Silt’e","Slovak","Slovenian","Somali","Soninke","Sorbian (Lower)","Sorbian (Upper)","Southern Sotho","South Slavey","Spanish","Sundanese","Svan","Swabish","Swahili","Swati","Swedish","Swiss German","Syriac","Tabassaran","Tagalog","Tahitian","Tai Nüa","Tajik","Tamil","Tatar","Telugu","Tetum","Thai","Tibetan","Tigre","Tigrinya","Tlingit","Tok Pisin","Tonga","Tongan","Tsez","Tsonga","Tswana","Tumbuka","Turkish","Turkmen","Tuscarora","Tuvaluan","Tuvan","Twi","Udmurt","Ukrainian","Urdu","Uyghur","Uzbek","Venda","Venetian","Veps","Vietnamese","Võro","Votic","Walloon","Waray-Waray","Welsh","Wiradjuri","Wolof","Xamtanga","Xhosa","Yakut Sakha","Yi","Yiddish","Yindjibarndi","Yolngu","Yoruba","Yupik","Zhuang","Zulu","Zuñi"};
	private final static String[] LOCATION_NAMES = {"Abasha","Abu Dhabi","Acapulco","Addis Ababa","Akrotiri","Algiers","Al Jahrah","Amman","Amsterdam","Andorra La Vella","Ankara","Animas","Antigua","Assisi","Athens","Attopu","Auckland","Axim","Baghdad","Balls Pond","Bangkok","Barka","Battambang","Beijing","Beirut","Beltsy","Berlin","Berne","Big Sandy","Bitola","Bluefields","Bodden Town","Bogota","Bratislava","Bridgetown","Brussels","Bucharest","Budapest","Buenos Aires","Cairo","Calabar","Calydon","Cannes","Capellen","Cape Town","Caracas","Carthage","Casablanca","Castletown","Chittagong","Cidra","Cologne","Constantine","Copenhagen","Cork","Damascus","Dangzai","Danisher","Darkhan","Delhi","Dori","Dresden","Dubai","Edinburgh","El Estor","Fagatogo","Fairy Dell","Faro","Freetown","Geneina","Geneva","Georgetown","Ghent","Giza","Gore","Gothenburg","Greymouth","The Hague","Halifax","Hanoi","Havana","Helsinki","High Rock","Himera","Hong Kong","Honolulu","Innsbruck","Islamabad","Istanbul","Jakar","Jakarta","Jaww","Jerusalem","Kandy","Kant","Karachi","Kathmandu","Keflavik","Kells","Khiva","Khost","Kiev","Killarney","Kingston","Kolari","Kolding","Kowloon","Kuala Lumpur","Kukes","Kyoto","Kythrea","La Serena","Leeds","Lima","Limerick","London","Lost","Luxor","Madrid","Malaga","Manila","Macau","Maputo","Marbella","Maribor","Maripa","Marrakesh","Marseille","Mecca","Merthyr Tydfil","Milan","Minas","Mogadishu","Monte Carlo","Moto'otua","Moscow","Mumbai","Munich","Naifaru","Nairobi","Nakasi 9 1/2 Miles","Nazca","Nazret","Nord","Odessa","Omoka","Oradea","Oran","Orange Walk","Oxford","Pagan","Palmira","Perth","Pigg's Peak","Piti","Plymouth","Point-Noire","Port Royal","Positano","Prague","Québec City","Quetzaltenango","Rakvere","Rhodes Town","Rio de Janeiro","Riyadh","Road Town","Rome","Rotterdam","Sabaneta","Saldus","Salinas","Samara","San Lorenzo","San Sebastian","Sao Paulo","Sarajevo","Sarband","Sekong","Seoul","Shanghai","Shenzhen","Siazan","Šilute","Sinpo","Sisian","Skien","Sliven","Snug Corner","Sparta","Strasbourg","Stockholm","Surat","Taipei","Taizz","Tamboril","Tehran","Thunder Bay","Toledo","Toliara","Tralee","Trinidad","Tripoli","Tsavo","Tutong","Tyre","Valletta","Valparai","Venice","Veracruz","Varrettes","Vladivostok","Wolfsberg","Wuhu","Xai-Xai","Xilin Hot","Xochimilco","Yerba Buena","Yokohama","Zambezi","Zaranj","Zenica","Zurich","Zuwarah"};
	private final static String[] JOB_NAMES = {"Foresters","Biochemist","Food scientist","Animal geneticist","Farmer","Wildlife biologist","Horticulturist","Plant nursery attendant","Agriculture teacher","Plant biologist","Wildlife inspector","Soil and plant scientist","Beekeeper","Aquatic ecologist","Conservationist","Veterinarian","Zoologist","Groomer","Animal control officer","Kennel attendant","Pet walker","Animal sitter","Animal shelter manager","Veterinary assistant","Animal nutritionist","Biologist","Wildlife rehabilitator","Breeder","Veterinary pathologist","Veterinary ophthalmologist","Project manager","Sales manager","Actuary","Business teacher","Business reporter","Admissions representative","Office manager","Office clerk","Assistant buyer","Business development manager","Salon manager","Makeup artist","Nail technician","Message therapist","Barber","Beautician","Skin care specialist","Fashion designer","Esthetician","Electrologist","Cosmetology instructor","Hairdresser","Fashion show stylist","Spa manager","Wedding stylist","Call center agent","Client services coordinator","Technical support representative","Virtual assistant","Customer care associate","Retail sales associate","Cashier","Concierge","Customer service manager","Help desk assistant","Account coordinator","Service adviser","Bank teller","Front desk coordinator","Client services manager","Art director","Graphic designer","Writer","Editor","Illustrator","Public relations specialist","Actor","Singer","Producer","Web developer","Architect","Multimedia animator","Painter","Tattoo artist","Dancer","English teacher","College professor","Tutor","Test scorer","Test administrator","Assistant professor","Principal","Superintendent","Vice principal","Substitute teacher","Librarian","Math teacher","Science teacher","Instructional designer","Guidance counselor","Civil engineer","Mechanical engineer","Chemical engineer","Biological engineer","Nuclear engineer","Aerospace engineer","Electrical engineer","Environmental engineer","Geological engineer","Marine engineer","Petroleum engineer","Safety engineer","Product engineer","Compliance engineer","Senior process engineer","Financial planner","Financial adviser","Budget analyst","Credit analyst","Financial services representative","Financial manager","Cost estimator","Loan officer","Investment banking analyst","Financial auditor","Nurse","Doctor","Caregiver","Physical therapist","Personal trainer","Pharmacist","Pharmacy assistant","Dental hygienist","Orthodontist","Travel nurse","Dental assistant","Occupational therapy aide","Dentist","Surgical technologist","Dietitian","Travel agent","Housekeeper","Porter","Casino host","Hotel clerk","Meeting planner","Executive chef","Tour guide","Flight attendant","Human resources manager","Compensations and benefits manager","Administrative assistant","Human resources generalist","Talent acquisition coordinator","Executive recruiter","Human resources specialist","Human resources director","Human resources assistant","Labor relations specialist","Human resources consultant","International human resources associate","Human resources systems administrator","Compensation advisor","UX designer","UI developer","IT manager","Computer programmer","SQL developer","Software developer","Web administrator","Data architect","Business intelligence developer","Mobile application developer","Information security analyst","Front-end web developer","Java developer","Database manager","Software engineer","Chief of operations","Team leader","Manager","Executive","Director","Supervisor","Principal","President","Vice President","Team lead","Marketing coordinator","Marketing manager","Marketing assistant","Marketing director","Product marketing manager","Creative director","Demand generation director","Social media manager","Account manager","Brand manager","Content marketing manager","Marketing analyst","Marketing consultant","Social media coordinator","Search engine optimization specialist","Chief operations officer","Logistics coordinator","Warehouse supervisor","Supply chain specialist","Distribution supervisor","Supply chain coordinator","Operations assistant","Scrum master","Continuous improvement lead","Operations manager","Sales representative","Sales director","Sales manager","Insurance sales agent","Telemarketer","Retail salesperson","Store manager","Account executive","B2B sales specialist","Sales analyst","Real estate broker","Sales associate","Sales consultant","Collection agent","Regional sales manager"};
	private final static String[] STATE_NAMES = {"Alabama","Alaska","Arizona","Arkansas","California","Colorado","Connecticut","Delaware","Florida","Georgia","Hawaii","Idaho","Illinois","Indiana","Iowa","Kansas","Kentucky[D]","Louisiana","Maine","Maryland","Massachusetts[D]","Michigan","Minnesota","Mississippi","Missouri","Montana","Nebraska","Nevada","NewHampshire","NewJersey","NewMexico","NewYork","NorthCarolina","NorthDakota","Ohio","Oklahoma","Oregon","Pennsylvania[D]","RhodeIsland","SouthCarolina","SouthDakota","Tennessee","Texas","Utah","Vermont","Virginia[D]","Washington","WestVirginia","Wisconsin","Wyoming"};
	private final static String[] CITY_NAMES = LOCATION_NAMES;
	private final static String[] ADDRESS_NAMES = {"Adah Bridge","Adeline Trafficway","Aglae Roads","Alessandra Flats","Anderson Creek","Ankunding Street","Arjun Junctions","Armani Valley","Arvel Flat","Austen Station","Balistreri Pines","Balistreri Rapids","Barrows Valleys","Bartell Mountain","Bartell Parkway","Bartoletti Well","Baumbach Bypass","Bechtelar Branch","Berge Inlet","Bergstrom Trail","Bernadine Vista","Boyer Flats","Brenden Extensions","Brett Groves","Brody Mountains","Casper Land","Chelsie Trail","Christophe Island","Darby Island","Darrel Rapids","Daryl Wells","David Row","Delbert Stream","Delphine Coves","Dibbert Gardens","Dock Crest","Domenic Shores","Dooley Spurs","Dorcas Station","Drake Lights","Dulce Radial","Earnest Summit","Elliott Lane","Elva Vista","Emard Lane","Emiliano Ports","Emmerich Lodge","Emmitt Knoll","Erdman Rapid","Ethan Street","Fadel Estate","Favian Fall","Filiberto Trail","Fiona Dam","Franecki Grove","Franecki Views","Friesen Turnpike","Garrison Shore","Gaylord Field","Geoffrey Plaza","Geovanni Fields","Gibson Oval","Goldner Loaf","Goldner Village","Grimes Drives","Gusikowski Common","Gusikowski Villages","Haleigh Meadows","Haley Cove","Haley Neck","Hammes Inlet","Hand Plain","Harvey Causeway","Heaney Meadow","Hilton Manors","Hoeger Field","Hoeger Glen","Holden Station","Hoppe Drive","Hosea Prairie","Jacinto Skyway","Jackson Keys","Jaleel Freeway","Jamel Mission","Jamir Throughway","Jaskolski Point","Jaydon Drive","Jennings Course","Jerry Point","Julian Courts","Justus Port","Kassulke Terrace","Kemmer Brooks","Kiehn Fork","Kunze Forge","Labadie Throughway","Langosh Greens","Langosh Mountains","Langworth Locks","Lemke Crest","Lemke Trafficway","Lesch Forest","Leuschke Crest","Leuschke Islands","Levi Causeway","Little Landing","Little Viaduct","Lockman Isle","Lucious Inlet","Maggio Path","Malvina Oval","Mann Center","Marc Crest","Maximilian Motorway","Mayer Prairie","McKenzie Estate","McKenzie Via","McLaughlin Ramp","Merl Underpass","Mertz Center","Micaela Pass","Miller Path","Minerva Mountains","Mohr Squares","Mraz Estates","Murazik Mountains","Myra Valley","Myrtice Landing","Nash Rapid","Nienow Cape","Nikita Ports","Nitzsche Trace","Noe Passage","O'Kon Estate","O'Kon Way","Orval Harbor","Parisian Course","Parisian Villages","Parker Bridge","Pfeffer Motorway","Pollich Spurs","Pouros Motorway","Powlowski Cape","Quitzon Stravenue","Rashawn Estate","Raymundo Passage","Reese Corners","Rempel Roads","Rice Turnpike","Rippin Flats","Rodriguez Forge","Rogelio Trail","Rowan Causeway","Royal Motorway","Runte Brooks","Runte Harbor","Russell Pine","Rutherford Spurs","Sanford Mountains","Satterfield Springs","Savannah Loaf","Schmidt Parkways","Schumm Valley","Sebastian Parkways","Senger Vista","Shyann Drives","Simone Prairie","Sipes Manors","Stanton Forges","Stracke Mountain","Susan Locks","Sven Harbors","Swaniawski Forges","Swift Plaza","Thiel Village","Tiana Run","Torp Ports","Torphy Walk","Towne Landing","Turcotte Port","Ulices Turnpike","Ulises Points","Ullrich Place","Ullrich Union","Upton Court","Von Loaf","Von Parkways","Von Spur","Walsh Fort","Webster Flats","Westley Meadow","Wiegand Falls","Wilford Throughway","Wilkinson Extension","Wilkinson Views","Wolff Lakes","Wunsch Parkways","Wyman Mills","Yasmine Expressway","Zita Junctions"};
	private final static String[] STREET_NAMES = {"Amber Route","Angel Lane","Art Route","Ash Row","Bath Street","Bay View Boulevard","Beach Avenue","Beachside Avenue","Boulder Boulevard","Bridgeway Avenue","Broad Lane","Brown Passage","Castle Lane","Central Avenue","Chestnut Avenue","Commercial Lane","Commercial Street","Commercial Way","Crescent Avenue","Dew Lane","Dew Street","Duchess Avenue","Emerald Passage","Frost Lane","Grand Lane","Gray Way","Haven Boulevard","Heart Street","Heirloom Passage","Heirloom Street","Hind Lane","Hind Way","Ironwood Way","Jade Lane","Java Boulevard","Knight Way","Lavender Street","Liberty Row","Lilypad Passage","Lotus Avenue","Love Lane","Low Avenue","Lower Avenue","Lower Way","Manor Lane","Meadow Row","Merchant Avenue","Merchant Street","Museum Route","New Castle Lane","Oceanview Row","Oval Street","Pearl Passage","Phoenix Passage","Pioneer Avenue","Princess Passage","Prospect Passage","Queen Passage","Redwood Way","Rose Avenue","Rosemary Street","Saffron Way","School Avenue","Seacoast Boulevard","Seacoast Route","Seacoast Street","Star Boulevard","Station Way","Temple Lane","Theater Way","Trinity Route","Trinity Row","Union Row","Upper Avenue","Walnut Avenue","Walnut Lane","Water Avenue","West Avenue","Wharf Lane"};
	private final static String[] NICK_NAMES = {"adaben","adelard","aglarân","alcarinion","amarthior","amathiphant","amdirthorn","amlugeden","amluginnog","arahaeldaer","ardir","arlin","avar","awahairo","bairrfhionn","balen","bathron","bellamdir","bercilak","bregol","brogamon","calaeron","cemmion","cevion","cidinnamarth","cody","costaro","dagion","delane","deldhinion","díllothanar","dolchanar","eccesindion","edenor","elanordil","elgine","elvedui","engwo","estolaben","faeldir","falch","gaeardaer","galadphen","gathrodion","gorvenal","gurunam","harnion","heledthor","hending","hithuven","hwandil","ianion","iphanthon","kelleher","lalaro","lanwatan","lassion","laurealasso","lavamben","lennion","leodegan","lindaro","loendir","lueius","lun","maeluion","maicion","melrion","minasdir","moriarty","morthor","mundil","nadhorchanar","nestoron","níco","níthor","norother","nurtaro","ogolben","óleryd","pamben","pelleas","rácandur","raicion","sawyl","sulben","telioron","thalachon","þámo","tharbachon","tinwendur","traherne","ulund","ulunion","vane","vanisauro","vanya","wilindion","wiryaro","yaro"};
	private final static String[] MAIL_HOSTS = {"google.com","hotmail.com","outlook.com","titan.com","protonmail.com","yahoo.com","zoho.com"};



	public static Document createPerson(Random rnd)
	{
		int gender = rnd.nextInt(2);

		String firstName = createFirstName(gender, rnd);
		String lastName = createLastName(rnd);
		String company = createCompany(rnd);

		return new Document()
			.put("_id", rnd.nextInt(100000))
			.put("createDateTime", createNewDate(rnd))
			.put("createUserId", rnd.nextInt(1000))
			.put("changeDateTime", createNewDate(rnd))
			.put("changeUserId", rnd.nextInt(1000))
			.put("version", rnd.nextInt(1000))
			.put("personal", new Document()
				.put("givenName", firstName)
				.put("surname", lastName)
				.put("gender", gender==0?"Male":"Female")
				.put("language", createLanguage(rnd))
				.put("birthday", createOldDate(rnd))
				.put("displayName", createNickName(rnd))
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
					.put("locations", Array.of(createGPS(rnd),createGPS(rnd),createGPS(rnd)))
				)
				.put("favorite", new Document()
					.put("color", createColorName(rnd))
					.put("fruit", createFruit(rnd))
					.put("number", rnd.nextInt(100))
					.put("food", createFoodName(rnd))
				)
			)
			.put("work", new Document()
				.put("company", company)
				.put("role", new String[]{"Administrator","Auditor","Author","Visitor"}[rnd.nextInt(4)])
				.put("team", new String[]{"Frontend","Backend","Supervisor","QA"}[rnd.nextInt(4)])
				.put("usageLocation", createLocation(rnd))
				.put("jobTitle", createJob(rnd))
				.put("contacts", new Array()
					.add(new Document().put("type", "email").put("text", (firstName+"."+lastName+"@"+company+".com").toLowerCase().replace(" ", "_")))
					.add(new Document().put("type", "phone").put("text", createPhoneNumber(rnd)))
					.add(new Document().put("type", "mobilePhone").put("text", createPhoneNumber(rnd)))
				)
			)
			;
	}


	private static LocalDateTime createNewDate(Random rnd)
	{
		return LocalDateTime.of(2000 + rnd.nextInt(20), 1 + rnd.nextInt(12), 1 + rnd.nextInt(28), rnd.nextInt(24), rnd.nextInt(60), rnd.nextInt(60));
	}


	private static LocalDateTime createOldDate(Random rnd)
	{
		return LocalDateTime.of(1960 + rnd.nextInt(20), 1 + rnd.nextInt(12), 1 + rnd.nextInt(28), rnd.nextInt(24), rnd.nextInt(60), rnd.nextInt(60));
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
		return JOB_NAMES[rnd.nextInt(JOB_NAMES.length)];
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
		return ""+rnd.nextInt(10)+rnd.nextInt(10)+rnd.nextInt(10)+" "+rnd.nextInt(10)+rnd.nextInt(10);
	}


	private static String createAddress(Random rnd)
	{
		return ADDRESS_NAMES[rnd.nextInt(ADDRESS_NAMES.length)];
	}


	private static String createStreet(Random rnd)
	{
		return STREET_NAMES[rnd.nextInt(STREET_NAMES.length)];
	}


	private static String createEmail(Random rnd)
	{
		return createNickName(rnd) + "@" + MAIL_HOSTS[rnd.nextInt(MAIL_HOSTS.length)];
	}


	private static String createNickName(Random rnd)
	{
		return NICK_NAMES[rnd.nextInt(NICK_NAMES.length)] + rnd.nextInt(100);
	}


	private static String createPhoneNumber(Random rnd)
	{
		String tmp = "";
		for (int i = 0; i < 10; i++) tmp+=Character.toString((char)('0'+rnd.nextInt(10)));
		return tmp;
	}


	private static Document createGPS(Random rnd)
	{
		return new Document().put("lat", 90*rnd.nextDouble()).put("lng", 90*rnd.nextDouble());
	}
}
