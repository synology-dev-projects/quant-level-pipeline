HISTORICAL WORKFLOW:

User 
  | (Runs manual_historical.py)
  v
[Config Module] -> Returns Secrets
  |
  v
[Extract Module]
  | Loop (Page 1...N):
  |   <-- Fetch JSON from TradingEdge API
  |   --> Yields Links
  | Loop (For every Link):
  |   <-- Scrape HTML from Post Link
  |   --> Appends {Data} to List
  |
  v
[Transform Module]
  | -> Convert List to DataFrame
  | -> Validate Schema (Crash if bad data)
  | -> Merge Duplicates (Combine ';')
  |
  v
[Load Module]
  | -> Connect to Oracle DB
  | -> INSERT data
  | -> COMMIT
  v
Done

INCREMENTAL WORKFLOW:

User 
  | (Runs daily_incremental.py)
  v
[Config Module] -> Returns Secrets
  |
  v
[Load Module]  <-- (Step 1a: Get Cutoff)
  | -> Query: "SELECT MAX(DATETIME)..."
  | -> Returns: cutoff_date (e.g., Yesterday)
  |
  v
[Extract Module]
  | Loop (Page 1...N):
  |   <-- Fetch JSON from TradingEdge API
  |   --> Check: Is post older than cutoff_date?
  |          [YES] -> STOP Loop immediately
  |          [NO]  -> Yield Link
  |
  | Loop (For every New Link):
  |   <-- Scrape HTML from Post Link
  |   --> Appends {Data} to List
  |
  v
[Transform Module]
  | -> Convert List to DataFrame
  | -> Validate Schema (Crash if bad data)
  | -> Merge Duplicates (Combine ';')
  |
  v
[Load Module]
  | -> Connect to Oracle DB
  | -> INSERT data (No Truncate)
  |      [Error?] -> ROLLBACK & EXIT
  |      [Success?] -> COMMIT
  v
Done