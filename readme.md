# Wikidata Locations Extractor

This node script takes a SQL database of Wikidata locations and extracts the data into a JSON file for further consumption.

## Usage
1. Get a geo dump from `https://dumps.wikimedia.org/` (I used `https://dumps.wikimedia.org/enwiki/20230301/enwiki-20230301-geo_tags.sql.gz`)
1. Make sure you have `sqlite3` and `gawk` installed on your system
2. Bring your sql file into this directory and name it `input.sql`
6. Run `npm install`
7. Run `node index.js`