// AACSelect.com — zero-dependency Node server: REST + live SSE push + JSON persistence
const http=require('http'),fs=require('fs'),path=require('path'),crypto=require('crypto');
const PORT=process.env.PORT||3000,PUB=path.join(__dirname,'public'),DBF=process.env.DB_FILE||path.join(__dirname,'data.json');
const WEEK=(+process.env.CYCLE_MIN||10080)*6e4,DAY=WEEK/7; // CYCLE_MIN=2 makes a whole cycle last 2 minutes (for testing)
let db={start:Date.now(),noms:{},votes:{},voted:{},ipv:{},nodded:{},ballots:0,frozen:null,last:null,champs:[],chat:[]};
try{db={...db,...JSON.parse(fs.readFileSync(DBF))}}catch{}
try{fs.mkdirSync(path.dirname(DBF),{recursive:true});fs.accessSync(path.dirname(DBF),fs.constants.W_OK);console.log('Saving data to '+DBF)}catch(e){console.error('WARNING: cannot write to '+DBF+' - votes will NOT be saved! '+e.message)}
if(process.env.RENDER&&!DBF.startsWith('/data'))console.warn('WARNING (Render): no persistent Disk in use - votes and chat reset on every deploy/restart. Add a Disk mounted at /data and set DB_FILE=/data/data.json');
let st;const save=()=>{clearTimeout(st);st=setTimeout(()=>fs.writeFile(DBF+'.tmp',JSON.stringify(db),()=>fs.rename(DBF+'.tmp',DBF,()=>{})),300)};
const BAD=/f+u+c+k|sh[i]t|b[i]tch|cunt|nigg|fag|retard|rape|rapist|nazi|hitler|whore|slut|asshole|dick|pussy|kike|spic|chink|tranny|kkk/;
const clean=s=>!BAD.test(String(s).toLowerCase().replace(/[@4]/g,'a').replace(/3/g,'e').replace(/[!1|]/g,'i').replace(/0/g,'o').replace(/[$5]/g,'s').replace(/[^a-z]/g,''));
const board=()=>Object.entries(db.noms).map(([key,c])=>({key,...c})).sort((a,b)=>b.count-a.count||b.at-a.at);
function tick(){const n=Date.now(),close=db.start+WEEK;
 if(!db.frozen&&n>=close){const s=board().slice(0,3).map(c=>({...c,votes:db.votes[c.key]||0})).sort((a,b)=>b.votes-a.votes||b.count-a.count);
  db.frozen={s,total:s.reduce((a,c)=>a+c.votes,0),announceAt:close+DAY};save()}
 if(db.frozen&&n>=db.frozen.announceAt){const w=db.frozen.s[0];
  if(w&&db.frozen.total>0)db.champs.unshift({name:w.name,social:w.social,votes:w.votes,at:db.frozen.announceAt});
  db.last={standings:db.frozen.s.map(({name,social,votes})=>({name,social,votes})),totalVotes:db.frozen.total,announceAt:db.frozen.announceAt};db.champs=db.champs.slice(0,24);Object.assign(db,{frozen:null,votes:{},voted:{},ipv:{},nodded:{},ballots:0,start:n});save()}}
const streams=new Set();
const pub=()=>{tick();const B=board();return{now:Date.now(),start:db.start,close:db.start+WEEK,frozen:!!db.frozen,announceAt:db.frozen?.announceAt||0,
 board:B.slice(0,50).map(({key,name,social,count})=>({key,name,social,count})),cands:B.slice(0,3).map(c=>c.key),ballots:db.ballots,
 nods:B.reduce((a,c)=>a+c.count,0),champs:db.champs,last:db.last,chat:db.chat.slice(-60),online:streams.size}};
const bc=()=>{const d=`data: ${JSON.stringify(pub())}\n\n`;streams.forEach(r=>r.write(d))};
const send=(res,c,o)=>{res.writeHead(c,{'Content-Type':'application/json'});res.end(JSON.stringify(o))};
const MIME={html:'text/html',js:'text/javascript',css:'text/css',png:'image/png',mp3:'audio/mpeg',mp4:'video/mp4',json:'application/json',svg:'image/svg+xml'};
function serve(req,res){let p=decodeURIComponent(req.url.split('?')[0]);if(p.endsWith('/'))p+='index.html';if(p==='/legal')p='/legal.html';
 const f=path.join(PUB,path.normalize(p));if(!f.startsWith(PUB))return res.writeHead(403).end();
 fs.stat(f,(e,s)=>{if(e||!s.isFile())return res.writeHead(404).end('Not found');
  const h={'Content-Type':MIME[path.extname(f).slice(1)]||'application/octet-stream','Accept-Ranges':'bytes','X-Content-Type-Options':'nosniff','Referrer-Policy':'strict-origin-when-cross-origin','Cache-Control':/html$/.test(f)?'no-cache':'public,max-age=86400'};
  const m=/bytes=(\d*)-(\d*)/.exec(req.headers.range||'');
  if(m){const a=+m[1]||0,b=m[2]?+m[2]:s.size-1;res.writeHead(206,{...h,'Content-Range':`bytes ${a}-${b}/${s.size}`,'Content-Length':b-a+1});fs.createReadStream(f,{start:a,end:b}).pipe(res)}
  else{res.writeHead(200,{...h,'Content-Length':s.size});fs.createReadStream(f).pipe(res)}})}
const hits={};
const server=http.createServer((req,res)=>{const url=req.url.split('?')[0];
 if(url==='/healthz')return send(res,200,{ok:1,up:process.uptime()|0});
 if(url==='/api/state')return send(res,200,pub());
 if(url==='/api/stream'){res.writeHead(200,{'Content-Type':'text/event-stream','Cache-Control':'no-cache',Connection:'keep-alive','X-Accel-Buffering':'no'});
  streams.add(res);req.on('close',()=>{streams.delete(res);bc()});return bc()}
 if(req.method==='POST'&&url.startsWith('/api/')){let b='';req.on('data',d=>{b+=d;if(b.length>4e3)req.destroy()});
  req.on('end',()=>{let j;try{j=JSON.parse(b)}catch{return send(res,400,{error:'Bad request'})}
   const ip=(req.headers['x-forwarded-for']||req.socket.remoteAddress||'').split(',')[0].trim(),n=Date.now();
   hits[ip]=(hits[ip]||[]).filter(x=>n-x<6e4);if(hits[ip].length>=14)return send(res,429,{error:'Slow down a little.'});hits[ip].push(n);
   const ipk=crypto.createHash('sha256').update(ip+(db.salt||(db.salt=crypto.randomBytes(8).toString('hex')))).digest('hex').slice(0,16);const S=(v,l)=>String(v||'').trim().slice(0,l);tick();const vid=S(j.vid,64);
   if(url==='/api/nominate'){const name=S(j.name,30),soc=S(j.social,80);
    if(name.length<2)return send(res,400,{error:'Creator name needs at least 2 characters.'});
    if(!/^(@[\w.-]{2,30}|(https?:\/\/)?([\w-]+\.)+[a-z]{2,}(\/\S*)?)$/i.test(soc))return send(res,400,{error:'Social must be an @handle or a link.'});
    if(!clean(name+soc))return send(res,400,{error:'Blocked by the Axiom Guard — keep it clean.'});
    const key=(name+'|'+soc).toLowerCase().replace(/\s+/g,'');if(vid.length<8||db.nodded[vid+key])return send(res,409,{error:'You already nodded this creator this cycle.'});
    db.nodded[vid+key]=1;const c=db.noms[key]||(db.noms[key]={name,social:soc,count:0});c.count++;c.at=n;save();bc();return send(res,200,{ok:1})}
   if(url==='/api/nod'){const c=db.noms[S(j.key,140)];if(!c||vid.length<8||db.nodded[vid+S(j.key,140)])return send(res,409,{error:'Already nodded.'});
    db.nodded[vid+S(j.key,140)]=1;c.count++;c.at=n;save();bc();return send(res,200,{ok:1})}
   if(url==='/api/vote'){const k=S(j.key,140);if(db.frozen)return send(res,400,{error:'Ballots are frozen.'});
    if(vid.length<8||!board().slice(0,3).some(x=>x.key===k))return send(res,400,{error:'Invalid ballot.'});
    if(db.voted[vid]||(db.ipv[ipk]||0)>=3)return send(res,409,{error:'Ballot already cast.'});
    db.voted[vid]=1;db.ipv[ipk]=(db.ipv[ipk]||0)+1;db.votes[k]=(db.votes[k]||0)+1;db.ballots++;save();bc();
    return send(res,200,{ok:1,receipt:crypto.createHash('sha1').update(vid+k+n).digest('hex').slice(0,8).toUpperCase()})}
   if(url==='/api/chat'){const name=S(j.name,20),text=S(j.text,240);if(name.length<2||!text)return send(res,400,{error:'Say something.'});
    if(!clean(name+text))return send(res,400,{error:'Blocked by the Axiom Guard — keep the frequency clean.'});
    db.chat.push({id:n+Math.random().toString(36).slice(2,6),name,text,at:n});db.chat=db.chat.slice(-100);save();bc();return send(res,200,{ok:1})}
   send(res,404,{error:'Unknown'})});return}
 serve(req,res)});
setInterval(()=>{tick();bc()},2e4);

process.on('uncaughtException',e=>console.error('uncaught',e));process.on('unhandledRejection',e=>console.error('unhandled',e));
const bye=()=>{try{fs.writeFileSync(DBF,JSON.stringify(db))}catch{}process.exit(0)};process.on('SIGTERM',bye);process.on('SIGINT',bye);

let port=+PORT;const url=()=>`http://localhost:${port}`;
server.on('listening',()=>{console.log(`\n  ================================================\n   AAC is LIVE  ->  ${url()}\n   Keep this window OPEN. Press Ctrl+C to stop.\n  ================================================\n`);
 if(process.env.AAC_OPEN==='1'){const{exec}=require('child_process');exec(process.platform==='win32'?`start "" ${url()}`:process.platform==='darwin'?`open ${url()}`:`xdg-open ${url()}`,()=>{})}});
const go=()=>server.listen(port);
server.on('error',e=>{if(e.code==='EADDRINUSE'&&port<+PORT+20){console.log(`Port ${port} is busy (another program or an old copy is using it) - trying ${port+1}...`);port++;setTimeout(go,100)}else{console.error('\nCould not start the server: '+e.message+'\n');process.exit(1)}});
go();
