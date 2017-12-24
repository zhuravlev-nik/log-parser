const
  exec = require('child_process').exec,
  fs = require('fs'),
  readline = require('readline');

const
  logsFolder = './logs/',
  dateFormat = new RegExp(/^\d{4}-\d{2}-\d{2}\w{1}\d{2}:\d{2}:\d{2}\.\d{3}\w{1}$/);

let
  dateFrom,
  dateTo;

/* chechArgs {{{ */
const checkArgs = (next) => {
  let
    strDate1 = process.argv[2] || null,
    strDate2 = process.argv[3] || null;

  if(!strDate1 || !strDate2 || strDate1.search(dateFormat) !== 0 || strDate2.search(dateFormat) !== 0){
    return next(new Error('Wrong parameters\nnode script.js from(ISO date) to(ISO date)"'));
  }

  let
    date1 = new Date(strDate1),
    date2 = new Date(strDate2);

  if(date1.valueOf() === date2.valueOf()){
    date2.setDate(date2.getDate() + 1);
  }
  dateFrom = (date1 < date2)?date1:date2,
  dateTo = (date1 < date2)?date2:date1;
  return next();
}
/* }}} */

/* getFileList {{{ */
const getFileList = (folder) => {
  return new Promise((resolve, reject) => {
    resolve(fs.readdirSync(folder));
  })
}
/* }}} */

/* unpackArchive {{{ */
const unpackArchive = (archive) => {
  return new Promise((resolve, reject) => {
    let arch = exec("tar -xzvf "+archive+" -C "+logsFolder, function(err, stdout, stderr){
      if(err) reject(err);
      resolve(stdout.replace('\n',''));
    })
  })
}
/* }}} */

/* readLogFile {{{ */
const readLogFile = (file) => {
  return new Promise((resolve, reject) => {
    const stream = fs.createReadStream(file);
    const rl = readline.createInterface(stream);
    let result = {};
    let line_num = 0;
    rl.on('line', function(line) {
      line_num++;
      if(line){
        try{
          line = JSON.parse(line);
        }catch(err){
          console.error('Bad string '+file+' line '+line_num+' - '+line);
          return false;
        }
        if(!line.time || line.time.search(dateFormat) !== 0){
          console.error('Wrong date format '+file+' line '+line_num+' - '+JSON.stringify(line));
          return false;
        }
        let date = new Date(line.time);
        let campaign = line.campaign_id || null;
        let event_type = line.event_type || null;
        if(date >= dateFrom && date <= dateTo && campaign && event_type == "show"){
          if(!result[campaign])
            result[campaign] = 1;
          else
            result[campaign]++;
        }
      }
    });
    rl.on('close', function() {
      resolve(result)
    });
  })
}
/* }}} */

/* removeLogFile {{{ */
const removeLogFile = (file) => {
  return new Promise((resolve, reject) => {
    let arch = exec("rm "+file, function(err, stdout, stderr){
      if(err) reject(err);
      resolve();
    })
  })
}
/* }}} */

/* parseLogs {{{ */
const parseLogs = async (path, next) => {
  let result = {};
  let files;
  try{
    files = await getFileList(path);
    if(!files.length) return next(new Error('Files not found'));
  }catch(err){
    return next(new Error('Files not found'));
  }
  for(let i=0; i<files.length; i++) {
    let file = files[i];
    let logFile;
    try{
      logFile = await unpackArchive(path+file);
    }catch(err){
      if(err) console.error('Bad file '+file);
    }
    if(logFile){
      try{
        const _result = await readLogFile(path+logFile);
        for(let key in _result){
          if(!result[key])
            result[key] = _result[key];
          else
            result[key] += _result[key];
        }
      }catch(err){
        console.error('File read error');
      }
      await removeLogFile(path+logFile);
    }
  }
  return next(null, result);
}
/* }}} */


checkArgs(function(err){
  if(err){
    console.error(err.message);
    process.exit();
  };
  parseLogs(logsFolder, function(err, result){
    if(err) console.error(err.message);
    for(let i in result){
      console.log(i+':'+result[i]);
    }
  });
});
