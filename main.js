const { MongoClient } = require('mongodb');
const fs = require('fs');
const readline = require('readline');
const zlib = require('zlib');
const https = require('https');
const { join } = require('path');

const client = new MongoClient('mongodb+srv://leato44eek:qwerty123@cluster0.azyw2zj.mongodb.net/?retryWrites=true&w=majority');
const dbName = 'Cluster0';
const fileUrl = 'https://popwatch-staging.s3.us-east-2.amazonaws.com/movies_1.gz';
const fileName = 'movies.json'

// запуск бд
async function startDB() {
  await client.connect();
  console.log('БД включена'); 
  const db = client.db(dbName);
  const collection = db.collection('movies');
}

//Вставлення в бд
async function insert(file){
  const db = client.db(dbName);
  const collection = db.collection('movies');
  const insertResult = await collection.insertMany(file);
  console.log('Вставлено фільми =>', insertResult);
}

//Шукання по індексі в бд
async function findIndexDB(index){
  const db = client.db(dbName);
  const collection = db.collection('movies');
const filteredDocs = await collection.find({ movie_id:index }).toArray();
console.log(`Знайдено ${index} елемент`, filteredDocs);
}

//Шукання в бд
async function findDB(){
  const db = client.db(dbName);
  const collection = db.collection('movies');
  const findResult = await collection.find({}).toArray();
  console.log('Знайдено документ =>', findResult);
}

//Парсер
const mass = [] // паршені дані тут
async function parserRLine(rl){
  for await (const line of rl) {
  const jsonLine = JSON.parse(line)
  mass.push(jsonLine)
  }
}

  //Перевірка на наявність 
async function checkaVailability(mass) {
  const db = client.db(dbName);
  const collection = db.collection('movies');

  for (const item of mass) {
  const movieId = item.movie_id; // Отримання значення movie_id з кожного елемента масиву mass
  const bulk = collection.initializeUnorderedBulkOp(); // Створення нового батчу
  bulk.find({ movie_id: movieId }).upsert().updateOne({ $set: item });

  const result = await bulk.execute();
  console.log(`Кількість фільмів оновлено: ${result.nModified}`);
  console.log(`Кількість фільмів вставлено: ${result.nUpserted}`);
  }
}
//Скачує, витягує файл з архіва , перевіряє на наявність і записує  його в бд
async function unzipAndWriteInMongoDB(startDB,parserRLine,checkaVailability) {
  try {
  //запуск БД
  await startDB();

  //Качає та витягує файл 
  https.get(fileUrl,  (res) => {
    const unzip = zlib.createGunzip();
    const writeStream = fs.createWriteStream(fileName);
    res.pipe(unzip).pipe(writeStream);

  //Читати те, що достали
  writeStream.on('finish', async () => {
    const rl = readline.createInterface({
      input: fs.createReadStream(fileName)
    });

  //Парсер
    await parserRLine(rl)

  //Перевірка на наявність 
    await checkaVailability(mass)
    })
  });
  }  catch (err) {
    console.error(err);
  }
}

//запуск
unzipAndWriteInMongoDB(startDB,parserRLine,checkaVailability)
