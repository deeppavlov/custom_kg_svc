DICT = {}
for semantic_action in range(1, 50):
    DICT[semantic_action] = "Internal operation was made"

for semantic_action in range(50, 70):
    DICT[semantic_action] = "region Visual Clusters and Zoomable Space-"
DICT[50] += "Removed this item from visual cluster"
DICT[51] += "Added this item to visual cluster"
DICT[52] += "System removed this item from visual cluster"
DICT[53] += "Created a visual cluster"
DICT[54] += "Moved visual cluster in the space"
DICT[55] += "Deleted a visual cluster"
DICT[56] += "Unpinned from a project space"

for semantic_action in range(70, 90):
    DICT[semantic_action] = "region Entity CRUD Operations-"
DICT[70] += "Created this item"
DICT[71] += "Changed"
DICT[72] += "Deleted this item"
DICT[73] += "Renamed this item"
DICT[74] += "Moved in the space"
DICT[75] += "Changed a property"
DICT[76] += "System assigned to an application account"
DICT[77] += "Thumbnail extracted and saved"
DICT[78] += "Description updated"

for semantic_action in range(90, 150):
    DICT[semantic_action] = "region Project Operations-"
DICT[90] += "New project was detected"
DICT[91] += "Deleted a project space"
DICT[92] += "Joined project space"
DICT[93] += "Left project space"
DICT[94] += "Moved an entity within the project space"
DICT[95] += "Deleted an entity from the project space"
DICT[96] += "Added new entity to the project space"
DICT[97] += "System marked this project space as the last visited"

for semantic_action in range(150, 170):
    DICT[semantic_action] = "region User Management-"
DICT[150] += "Entered this project space"
DICT[151] += "Left this project space"
DICT[152] += "Moved within this project space"

for semantic_action in range(170, 200):
    DICT[semantic_action] = "region Linked Data Changes-"
DICT[170] += "Linked file was renamed or moved"
DICT[171] += "Linked file is no longer available"
DICT[172] += "Started copying file to local cache or linked app folder"
DICT[173] += "Finished copying file to local cache or linked app folder"
DICT[174] += "Started uploading of file to linked app remote data source"
DICT[175] += "Finished uploading of file to linked app remote data source"
DICT[180] += "Started downloading web page"
DICT[181] += "Finished downloading web page"
DICT[182] += "Failed to download web page"
DICT[185] += "Contents changed"

for semantic_action in range(200, 210):
    DICT[semantic_action] = "region Full-Text Search Indexing-"
DICT[200] += "System made this item ready for full-text search"
DICT[201] += "System failed to make this item ready for full-text search"

for semantic_action in range(210, 220):
    DICT[semantic_action] = "region List Management-"
DICT[210] += "Added to a list"
DICT[211] += "Added to a project space"
DICT[218] += "Removed from a project space"
DICT[219] += "Removed from a list"

for semantic_action in range(220, 230):
    DICT[semantic_action] = "region Linking and Meta Linking-"
DICT[220] += "Linked to Meta entity"
DICT[221] += "Linked to another entity"
DICT[222] += "Entities extracted and linked"
DICT[223] += "System fixed meta id and re-bound entity to its holding project"
DICT[224] += "Link to another entity removed"

for semantic_action in range(230, 240):
    DICT[semantic_action] = "region Keyphrases Extraction-"
DICT[230] += "Added a keyphrase"
DICT[231] += "Deleted a keyphrase"
DICT[232] += "All keyphrases removed"
DICT[233] += "System updated keyphrases"
DICT[234] += "System extracted keyphrases"
DICT[235] += "System recalculated keyphrases"

for semantic_action in range(240, 250):
    DICT[semantic_action] = "region Kinds-"
DICT[240] += "System identified item's kind"
DICT[241] += "Changed item's kind"

for semantic_action in range(250, 260):
    DICT[semantic_action] = "region Same Entities-"
DICT[250] += "Same entity was found"
DICT[251] += "Same person was found"

for semantic_action in range(300, 500):
    DICT[semantic_action] = "region Entity Semantic Activities-"
DICT[300] += "Opened"
DICT[301] += "Marked as read"
DICT[302] += "Marked as unread"
DICT[303] += "Edited"
DICT[304] += "System marked it as unread"
DICT[310] += "Cleared color status"
DICT[311] += "Marked as \"in progress\""
DICT[312] += "Marked as \"action required\""
DICT[313] += "Marked as \"done\""
DICT[314] += "Flagged as important"
DICT[315] += "Flagged as not important"
DICT[316] += "Resized"
DICT[317] += "Changed privacy status"
DICT[318] += "Flagged as top"
DICT[319] += "Flagged as normal"

def get_semantic_action_desc(number):
    return DICT[number]
