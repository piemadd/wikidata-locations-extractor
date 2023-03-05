const fs = require("fs");
const sqlite3 = require('sqlite3').verbose();
const spawn = require("child_process").spawn;

const extractData = (() => {
  //opening database
  let db = new sqlite3.Database('./data/input.db', sqlite3.OPEN_READONLY, (err) => {
    if (err) {
      console.error(err.message);
    }

    console.log('Connected to the input database.');
  });

  //only required data for dump
  const sql = `SELECT gt_id, gt_page_id, gt_primary, gt_lat, gt_lon FROM geo_tags WHERE gt_lat IS NOT NULL AND gt_lon IS NOT NULL AND gt_page_id IS NOT NULL AND gt_id IS NOT NULL;`

  console.log('querying data')
  db.all(sql, [], (err, rows) => {
    if (err) {
      throw err;
    }

    let pages = {};

    rows.forEach((row, i, arr) => {
      if (pages[row.gt_page_id] === undefined) pages[row.gt_page_id] = [];

      pages[row.gt_page_id].push({
        lat: parseFloat(row.gt_lat).toFixed(5),
        lon: parseFloat(row.gt_lon).toFixed(5),
        id: parseInt(row.gt_id),
        primary: row.gt_primary
      })

      if (i % 100000 === 0) console.log(`${i}/${arr.length} rows processed`);
    });

    console.log('rows processed, sorting pages');

    let finalPages = {};

    Object.keys(pages).forEach((pageId, i, arr) => {
      const page = pages[pageId];
      const sorted = page.sort((a, b) => {
        if (a.primary === '1') return 1;
        if (b.primary === '1') return -1;

        return a.id - b.id;
      });

      const cleaned = sorted.map((item) => {
        return [Number(item.lat), Number(item.lon), item.id];
      });

      if (sorted[0].primary === '0') {
        finalPages[pageId] = cleaned;
      } else {
        finalPages[pageId] = [cleaned[0]];
      };

      if (i % 100000 === 0) console.log(`${i}/${arr.length} pages processed`);
    });

    console.log('pages sorted, writing to file');

    fs.writeFileSync('./data.json', JSON.stringify(finalPages));
  });
})

// check if data already ingested but not extracted
if (fs.existsSync('./data/input.db')) {
  console.log('data already ingested, starting extraction');
  extractData();
  return;
}

// clean up data folder
if (fs.existsSync('./data')) fs.rmSync('./data', { recursive: true });
fs.mkdirSync('./data');

// converting mysql dump to sqlite dump
const ingest = spawn('sh', ['-c', './convertDump.sh input.sql | sqlite3 ./data/input.db']);

ingest.stdout.on('data', (data) => {
  console.log(data.toString('utf8'));
});

ingest.stderr.on('data', (data) => {
  console.log(data.toString('utf8'));
});

ingest.on('close', (code) => {
  console.log(`child process exited with code ${code}`);
  console.log('data ingested, starting extraction');

  extractData();
});

//spawn("sqlite3 ./data/input.db < ./data/fixed.sql");

// ./mysql2sqlite dump_mysql.sql | sqlite3 mysqlite3.db

//  | sqlite3 ./data/input.db