const axios = require('axios');
const cheerio = require('cheerio');
const { Client } = require('pg');
const cron = require('node-cron');

const url = 'https://simponiternak.pertanian.go.id/harga-daerah.php';

const client = new Client({
    user: 'scrapping',          // Ganti dengan username PostgreSQL kamu
    host: '192.168.19.109',               // Ganti jika database ada di server lain
    database: 'scrapping_database',       // Ganti dengan nama database kamu
    password: 'MyBest12345',       // Ganti dengan password PostgreSQL kamu
    port: 5432,                      // Port default PostgreSQL
});


async function scrapeAndInsert() {
    // Fungsi untuk menyimpan data ke PostgreSQL
    const saveDataToDatabase = async (header, rows) => {
      try {
          await client.connect();
          
          const dateString = header[header.length - 1];
          const [day, month, year] = dateString.split('/');
          const date = `${year}-${month}-${day}`;
        
        
          const currentData = await client.query('select * from commodity_prices where date = $1', [date]);
        const lengthCurrentData = currentData.rowCount;
        
        if (lengthCurrentData != 0) {
            for (const row of rows) { // Mulai dari index 1 untuk menghindari header
              // console.log(row[8]);
              const query = 'UPDATE commodity_prices SET price = $1 WHERE commodity = $2 AND date = $3 RETURNING *;';
              const values = [parseFloat(row[8].replace(/,/g, '')), row[1], date]
              await client.query(query, values); // Ganti koma dengan string kosong dan ubah ke float
            }
            console.log("DATA UPDATED SUCCESSFULLY");
        } else {
                  // Loop melalui setiap baris data dan masukkan ke database
          for (const row of rows) { // Mulai dari index 1 untuk menghindari header
              // console.log(row[8]);
              const query = 'INSERT INTO commodity_prices (commodity, date, price, source_url) VALUES ($1, $2, $3, $4)';
              await client.query(query, [row[1], date, parseFloat(row[8].replace(/,/g, '')), url]); // Ganti koma dengan string kosong dan ubah ke float
          }
          
          console.log("DATA SAVED SUCCESSFULLY");
        }
      } catch (err) {
        console.error('Error inserting data: ', err);
      } finally {
        await client.end(); // Menutup koneksi setelah semua data disimpan
      }
    };
  
    axios.get(url).then(response => {
    const html = response.data;
    const $ = cheerio.load(html);
  
    let data = [];
  
    // Select each table row
    $('table tr').each((index, element) => {
      let row = [];
  
      // Select each cell (td) or header (th) in the row
      $(element).find('td, th').each((i, el) => {
        row.push($(el).text().trim());  // Trim to remove extra spaces or line breaks
      });
  
      // Only add the row if it has some data (ignore empty rows)
      if (row.length > 0) {
        data.push(row);
      }
    });
      
      const header = data[0];
      const rows = data.slice(1);
  
      // console.log(rows);
      saveDataToDatabase(header, rows);
    }).catch(error => {
    console.error(error);
  });  
}

// Jadwalkan scraping setiap jam
cron.schedule('0 * * * *', () => {
  console.log('Running task every hour...');
  scrapeAndInsert();
});