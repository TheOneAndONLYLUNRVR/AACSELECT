// AACSelect (https://aacselect.onrender.com) — zero-dependency Node server: REST + live SSE push + JSON persistence + optional cloud bucket backup
const http=require('http'),https=require('https'),fs=require('fs'),path=require('path'),crypto=require('crypto'),zlib=require('zlib'),os=require('os');
let PUB=path.join(__dirname,'public');const PORT=process.env.PORT||3000,DBF=process.env.DB_FILE||path.join(__dirname,'data.json'),SITE='https://aacselect.onrender.com';
const WEEK=(+process.env.CYCLE_MIN||10080)*6e4,DAY=WEEK/7; // CYCLE_MIN=2 makes a whole cycle last 2 minutes (for testing)
const sleep=ms=>new Promise(r=>setTimeout(r,ms));
// Auto-unpack: if the website folder is missing but a release .zip sits next to server.js, extract it (so a repo with just the zip still works)
let DIAG='';
function unpack(){try{if(fs.existsSync(path.join(PUB,'index.html')))return;const z=fs.readdirSync(__dirname).filter(f=>/\.zip$/i.test(f)).sort((x,y)=>x.localeCompare(y,undefined,{numeric:true})).pop();if(!z){DIAG='no .zip file found next to server.js';return}const b=fs.readFileSync(path.join(__dirname,z));let e=b.length-22;while(e>=0&&b.readUInt32LE(e)!==0x06054b50)e--;if(e<0){DIAG='the zip could not be read (corrupt upload?)';return}
 const n=b.readUInt16LE(e+10),E=[];let p=b.readUInt32LE(e+16);for(let i=0;i<n;i++){const nl=b.readUInt16LE(p+28),xl=b.readUInt16LE(p+30),cl=b.readUInt16LE(p+32);E.push({m:b.readUInt16LE(p+10),cs:b.readUInt32LE(p+20),off:b.readUInt32LE(p+42),name:b.toString('utf8',p+46,p+46+nl)});p+=46+nl+xl+cl}
 const ix=E.find(x=>/(^|\/)public\/index\.html$/.test(x.name));if(!ix){DIAG='zip '+z+' has no public/index.html inside';return}const base=ix.name.slice(0,-'index.html'.length);let c=0;try{fs.mkdirSync(PUB,{recursive:true});fs.accessSync(PUB,fs.constants.W_OK)}catch{PUB=path.join(os.tmpdir(),'aac-public');DIAG='repo folder is read-only, using '+PUB}
 for(const x of E){if(!x.name.startsWith(base)||x.name.endsWith('/'))continue;const rel=x.name.slice(base.length);if(rel.includes('..'))continue;const nl=b.readUInt16LE(x.off+26),xl=b.readUInt16LE(x.off+28),d=b.subarray(x.off+30+nl+xl,x.off+30+nl+xl+x.cs),o=path.join(PUB,rel);fs.mkdirSync(path.dirname(o),{recursive:true});fs.writeFileSync(o,x.m===8?zlib.inflateRawSync(d):d);c++}
 console.log(`Unpacked ${c} website files from ${z}`)}catch(err){DIAG='unpack error: '+err.message;console.error('Could not unpack zip:',err.message)}}
unpack();if(!fs.existsSync(path.join(PUB,'index.html')))console.error('WARNING: website files missing - upload the "public" folder (or the site .zip) next to server.js');
const FRESH=()=>({start:Date.now(),noms:{},votes:{},voted:{},ipv:{},nodded:{},ballots:0,frozen:null,last:null,champs:[],chat:[],promos:[],plog:{},bans:[],seen:{},savedAt:0});
let db=FRESH();
try{db={...db,...JSON.parse(fs.readFileSync(DBF))}}catch{}
try{fs.mkdirSync(path.dirname(DBF),{recursive:true});fs.accessSync(path.dirname(DBF),fs.constants.W_OK);console.log('Saving data to '+DBF)}catch(e){console.error('WARNING: cannot write to '+DBF+' - '+e.message)}

// ---- CLOUD BUCKET (any S3-compatible storage: Cloudflare R2, Backblaze B2, AWS S3...). Keeps votes, nominations, chat, bans AND Spotlight files
//      safe through restarts/redeploys even on Render's FREE plan (which has no persistent disk).
const S3=process.env.S3_ENDPOINT&&process.env.S3_BUCKET&&process.env.S3_KEY_ID&&process.env.S3_SECRET?{ep:new URL(process.env.S3_ENDPOINT),b:process.env.S3_BUCKET,k:process.env.S3_KEY_ID,s:process.env.S3_SECRET,r:process.env.S3_REGION||'auto',p:(process.env.S3_PREFIX||'aacselect').replace(/^\/+|\/+$/g,'')}:null;
const HM=(k,d)=>crypto.createHmac('sha256',k).update(d).digest(),SHA=d=>crypto.createHash('sha256').update(d).digest('hex');
function sign(method,p,h,payload,key,secret,region,t){const d=t.slice(0,8),sk=Object.keys(h).map(k=>k.toLowerCase()).sort(),L=Object.fromEntries(Object.entries(h).map(([k,v])=>[k.toLowerCase(),String(v).trim()])),sh=sk.join(';'),sc=`${d}/${region}/s3/aws4_request`;
 const cr=[method,p,'',sk.map(k=>k+':'+L[k]+'\n').join(''),sh,payload].join('\n'),sig=HM(HM(HM(HM(HM('AWS4'+secret,d),region),'s3'),'aws4_request'),['AWS4-HMAC-SHA256',t,sc,SHA(cr)].join('\n')).toString('hex');
 return`AWS4-HMAC-SHA256 Credential=${key}/${sc}, SignedHeaders=${sh}, Signature=${sig}`}
// low-level request; body = Buffer|string|{file,size}; resolves with the raw response (caller consumes it)
function s3(method,key,body,extra={}){return new Promise((ok,no)=>{const t=new Date().toISOString().replace(/[-:]|\.\d{3}/g,''),p=('/'+S3.b+'/'+(S3.p?S3.p+'/':'')+key).split('/').map(encodeURIComponent).join('/'),buf=body!=null&&!body.file,payload=buf?SHA(body):body?.file?'UNSIGNED-PAYLOAD':SHA(''),
  h={host:S3.ep.host,'x-amz-content-sha256':payload,'x-amz-date':t},rq=(S3.ep.protocol==='http:'?http:https).request({method,hostname:S3.ep.hostname,port:S3.ep.port||undefined,path:p,timeout:120e3,headers:{...h,...extra,Authorization:sign(method,p,h,payload,S3.k,S3.s,S3.r,t),...(body!=null?{'Content-Length':buf?Buffer.byteLength(body):body.size}:{})}},ok);
 rq.on('error',no);rq.on('timeout',()=>rq.destroy(new Error('bucket timeout')));if(body?.file){const rs=fs.createReadStream(body.file);rs.on('error',e=>rq.destroy(e));rs.pipe(rq)}else rq.end(body??undefined)})}
const drain=r=>new Promise(ok=>{const c=[];r.on('data',d=>c.push(d));r.on('end',()=>ok(Buffer.concat(c)));r.on('error',()=>ok(Buffer.alloc(0)))});
async function s3put(key,body,type){const r=await s3('PUT',key,body,{'Content-Type':type||'application/octet-stream'}),b=await drain(r);if(r.statusCode>=300)throw Error('bucket PUT '+r.statusCode+' '+b.toString().slice(0,200))}
async function s3get(key){const r=await s3('GET',key),b=await drain(r);if(r.statusCode===404)return null;if(r.statusCode>=300)throw Error('bucket GET '+r.statusCode+' '+b.toString().slice(0,200));return b}
async function s3del(key){try{const r=await s3('DELETE',key);await drain(r)}catch(e){console.error('bucket delete failed:',e.message)}}

// ---- saving: local file (instant) + cloud bucket (coalesced, at most every few seconds)
let st,cloudOK=!S3,cBusy=0,cNext=null,lastDaily='';
const flush=()=>{db.savedAt=Date.now();const j=JSON.stringify(db);fs.writeFile(DBF+'.tmp',j,()=>fs.rename(DBF+'.tmp',DBF,()=>{}));if(S3&&cloudOK)cloud(j)};
const save=()=>{clearTimeout(st);st=setTimeout(flush,300)};
function cloud(j){cNext=j;if(cBusy)return;cBusy=1;(async()=>{while(cNext){const b=cNext;cNext=null;try{await s3put('state.json',b,'application/json');const d=new Date().toISOString().slice(0,10);if(d!==lastDaily){lastDaily=d;await s3put('backups/state-'+d+'.json',b,'application/json')}}catch(e){console.error('cloud save failed (will retry):',e.message);if(!cNext)cNext=b;await sleep(8e3)}await sleep(4e3)}cBusy=0})()}
async function restore(){if(!S3)return;for(let i=0;i<8;i++){try{const b=await s3get('state.json');if(b){const r=JSON.parse(b);if((r.savedAt||0)>=(db.savedAt||0)){db={...FRESH(),...r};console.log('Restored state from the cloud bucket (saved '+new Date(r.savedAt||0).toISOString()+')')}}else console.log('Cloud bucket is empty - starting fresh there');cloudOK=1;return}catch(e){console.error(`cloud restore attempt ${i+1} failed: ${e.message}`);await sleep(4e3)}}
 console.error('WARNING: could not read the cloud bucket - running on local data, cloud saving paused until it is reachable');
 const retry=setInterval(async()=>{try{const b=await s3get('state.json'),r=b&&JSON.parse(b);if(r&&(r.savedAt||0)>(db.savedAt||0))db={...FRESH(),...r};cloudOK=1;clearInterval(retry);console.log('Cloud bucket reachable again');save()}catch{}},6e4)}
if(process.env.RENDER&&!S3&&!DBF.startsWith('/data'))console.warn('WARNING (Render): no persistent Disk and no cloud bucket - everything resets on every restart. Set the S3_* env vars (see README) or add a Disk at /data.');

// ---- SPOTLIGHT: visitor videos + images
const MB=1048576,PROMO_DIR=process.env.PROMO_DIR||path.join(path.dirname(DBF),'promos'),MAXV=(+process.env.MAX_PROMO_MB||250)*MB,MAXI=(+process.env.MAX_IMAGE_MB||25)*MB,CAPB=(+process.env.PROMO_CAP_MB||(S3?8000:900))*MB,MAXN=+process.env.PROMO_MAX||60,CACHEB=(+process.env.CACHE_MB||1500)*MB;
try{fs.mkdirSync(PROMO_DIR,{recursive:true})}catch(e){console.error('WARNING: cannot create '+PROMO_DIR)}
const KINDS={'video/mp4':['video','mp4'],'video/quicktime':['video','mp4'],'video/webm':['video','webm'],'image/jpeg':['image','jpg'],'image/png':['image','png'],'image/webp':['image','webp'],'image/gif':['image','gif']};
const magic=(h,ext)=>ext==='mp4'?h.toString('latin1',4,8)==='ftyp':ext==='webm'?h.readUInt32BE(0)===0x1a45dfa3:ext==='jpg'?h[0]===0xff&&h[1]===0xd8&&h[2]===0xff:ext==='png'?h.readUInt32BE(0)===0x89504e47:ext==='gif'?h.toString('latin1',0,4)==='GIF8':ext==='webp'?h.toString('latin1',0,4)==='RIFF'&&h.toString('latin1',8,12)==='WEBP':false;
const fname=p=>p.id+'.'+(p.ext||'mp4'),ploc=p=>path.join(PROMO_DIR,fname(p)),pkey=p=>'promos/'+fname(p);
const SS=(v,l)=>String(v||'').trim().slice(0,l),SOC=/^(@[\w.-]{2,30}|(https?:\/\/)?([\w-]+\.)+[a-z]{2,}(\/\S*)?)$/i,rmP=p=>{try{fs.unlinkSync(ploc(p))}catch{}S3&&s3del(pkey(p))};
const ipOf=req=>(req.headers['cf-connecting-ip']||req.headers['x-forwarded-for']||req.socket.remoteAddress||'').split(',')[0].trim(),ipH=ip=>crypto.createHash('sha256').update(String(ip).trim().toLowerCase()+(db.salt||(db.salt=crypto.randomBytes(8).toString('hex')))).digest('hex').slice(0,16);
const used=new Map(); // local cache last-use times
function prune(){const n=Date.now();let ch=0;db.promos=db.promos.filter(p=>{if(n-p.at>14*864e5){rmP(p);ch=1;return false}return true});let tot=db.promos.reduce((a,p)=>a+p.size,0);while((tot>CAPB||db.promos.length>MAXN)&&db.promos.length){const p=db.promos.pop();rmP(p);tot-=p.size;ch=1}if(ch)save();
 if(!S3||!cloudOK)return;const up=db.promos.find(p=>!p.cloud&&fs.existsSync(ploc(p)));if(up&&!up.busy)pushPromo(up); // retry failed cloud uploads
 let loc=db.promos.filter(p=>p.cloud&&fs.existsSync(ploc(p))).sort((a,b)=>(used.get(a.id)||a.at)-(used.get(b.id)||b.at)),sz=loc.reduce((a,p)=>a+p.size,0);while(sz>CACHEB&&loc.length){const p=loc.shift();try{fs.unlinkSync(ploc(p))}catch{}sz-=p.size}}
async function pushPromo(p){if(!S3)return;p.busy=1;try{await s3put(pkey(p),{file:ploc(p),size:p.size},(p.kind==='image'?'image/':'video/')+(p.ext==='jpg'?'jpeg':p.ext));p.cloud=1;save()}catch(e){console.error('Spotlight cloud upload failed (will retry):',e.message)}delete p.busy}
function upload(req,res){const fail=(c,m,x)=>{res.setHeader('Connection','close');send(res,c,{error:m,...x});req.resume()};
 const q=new URL(req.url,'http://x').searchParams,title=SS(q.get('title'),60),name=SS(q.get('name'),30),soc=SS(q.get('social'),80),vid=SS(q.get('vid'),64),n=Date.now(),len=+req.headers['content-length']||0,ipk=ipH(ipOf(req)),bn=banOf(vid,ipk);
 if(bn)return fail(403,'You are banned.',{banned:banView(bn)});
 if(title.length<3)return fail(400,'Give your Spotlight a title (3+ characters).');if(name.length<2)return fail(400,'Creator name needs 2+ characters.');if(!SOC.test(soc))return fail(400,'Social must be an @handle or a link.');
 if(!clean(title,name,soc))return fail(400,'Blocked by the Axiom Guard - keep it clean.');if(vid.length<8)return fail(400,'Invalid session.');
 const K=KINDS[(req.headers['content-type']||'').split(';')[0].trim().toLowerCase()];if(!K)return fail(415,'Only videos (MP4, MOV, WebM) or images (JPG, PNG, WebP, GIF) are allowed.');
 const[kind,ext]=K,MAX=kind==='video'?MAXV:MAXI;if(!len||len>MAX)return fail(413,(kind==='video'?'Videos':'Images')+' must be under '+MAX/MB+' MB.');
 const L=k=>(db.plog[k]=(db.plog[k]||[]).filter(x=>n-x<864e5));if(L(vid).length>=3||L(ipk).length>=6)return fail(429,'Daily upload limit reached - try again tomorrow.');
 const id=crypto.randomBytes(8).toString('hex'),tmp=path.join(PROMO_DIR,id+'.part'),ws=fs.createWriteStream(tmp);let got=0,head=Buffer.alloc(0),bad=0;
 req.on('data',d=>{got+=d.length;if(head.length<12)head=Buffer.concat([head,d]).subarray(0,12);if(got>MAX&&!bad){bad=1;req.unpipe(ws);ws.destroy();fs.unlink(tmp,()=>{});send(res,413,{error:'File too large.'});req.resume()}});
 req.on('aborted',()=>{bad=1;ws.destroy();fs.unlink(tmp,()=>{})});req.pipe(ws);
 ws.on('error',()=>{fs.unlink(tmp,()=>{});if(!res.headersSent)send(res,500,{error:'Upload failed.'})});
 ws.on('finish',()=>{if(bad)return;if(head.length<12||!magic(head,ext)){fs.unlink(tmp,()=>{});return send(res,415,{error:'That file is not a real '+ext.toUpperCase()+'.'})}
  const p={id,title,name,social:soc,at:n,size:got,reports:0,kind,ext};fs.rename(tmp,ploc(p),err=>{if(err)return send(res,500,{error:'Could not save the file.'});L(vid).push(n);L(ipk).push(n);db.promos.unshift(p);seenAct(vid,ipk,req);prune();save();bc();send(res,200,{ok:1,id});pushPromo(p)})})}
// ---- AXIOM GUARD v2: whole-word + disguise aware (no more false positives like "Entity_B" or "grape")
const GD_LEET={'@':'a','4':'a','3':'e','!':'i','1':'i','|':'i','0':'o','$':'s','5':'s','7':'t'},GD_STRONG=/f+u+c+k|nigg+(?:er|a)|faggot|cocksuck|motherf/,GD_WORD=/^(?:shit|bullshit|bitch|cunt|dick|pussy|whore|slut|fag|faggot|rape|rapist|nazi|hitler|kike|spic|chink|tranny|kkk|asshole|retard|retarded|wetback|gook)(?:s|es|ed|ing|er|ers|y|head|heads)?$/;
function gdBad(s){s=String(s||'').replace(/^@/,'');if(/^https?:\/\//i.test(s)||s.includes('/'))s=s.replace(/^(?:https?:\/\/)?(?:[\w-]+\.)+[a-z]{2,}\/?/i,'');const n=s.toLowerCase().normalize('NFKD').replace(/[̀-ͯ]/g,'').replace(/[@43!1|0$57]/g,c=>GD_LEET[c]),t=n.split(/[^a-z]+/).filter(Boolean),m=[];let r='';
 for(const w of t){if(w.length===1)r+=w;else{if(r.length>1)m.push(r);r='';m.push(w)}}if(r.length>1)m.push(r);return m.some(w=>GD_STRONG.test(w)||GD_WORD.test(w))}
// ---- /AXIOM GUARD
const clean=(...a)=>a.every(x=>!gdBad(x));
const board=()=>Object.entries(db.noms).map(([key,c])=>({key,...c})).sort((a,b)=>b.count-a.count||b.at-a.at);
// Nominations stay on the Board for the whole cycle and are only cleared once a winner has been announced.
function tick(){const n=Date.now(),close=db.start+WEEK;
 if(!db.frozen&&n>=close){const s=board().slice(0,3).map(c=>({...c,votes:db.votes[c.key]||0})).sort((a,b)=>b.votes-a.votes||b.count-a.count);
  db.frozen={s,total:s.reduce((a,c)=>a+c.votes,0),announceAt:close+DAY};save()}
 if(db.frozen&&n>=db.frozen.announceAt){const w=db.frozen.s[0],won=!!(w&&db.frozen.total>0);
  if(won)db.champs.unshift({name:w.name,social:w.social,votes:w.votes,at:db.frozen.announceAt});
  db.last={standings:db.frozen.s.map(({name,social,votes})=>({name,social,votes})),totalVotes:db.frozen.total,announceAt:db.frozen.announceAt};db.champs=db.champs.slice(0,24);
  Object.assign(db,{frozen:null,votes:{},voted:{},ipv:{},nodded:{},ballots:0,start:n},won?{noms:{}}:{});save()}}
// a nomination for someone already on the Board counts as a nod for them (matched by handle or by name)
const normH=s=>{s=String(s).toLowerCase().trim();const m=/^@?([\w.-]{2,30})$/.exec(s)||/(?:tiktok\.com|youtube\.com|twitch\.tv|instagram\.com|x\.com|twitter\.com|kick\.com)\/@?([\w.-]{2,30})/.exec(s);return m?m[1]:s.replace(/^https?:\/\/(www\.)?/,'').replace(/\/+$/,'')},normN=s=>String(s).toLowerCase().normalize('NFKD').replace(/[^a-z0-9]/g,'');
const findNom=(key,name,soc)=>db.noms[key]?key:Object.keys(db.noms).find(k=>normH(db.noms[k].social)===normH(soc))||Object.keys(db.noms).find(k=>normN(db.noms[k].name)===normN(name)&&normN(name).length>=3);

// ---- MODERATION: visitors, kicks, bans
// Admin panel password. Only a salted scrypt hash is stored here (never the password itself). Setting ADMIN_KEY on Render replaces it.
const ADMIN_HASH='8cc0cca2903dd5837cfd8b405a1220e7:13fd1176221d57d98c4af8f7594b96c144c70add4c568fc7a3eefaccc953a12a',ADMIN_KEY=process.env.ADMIN_KEY||'',streams=new Map(); // res -> {vid,ipk,ua,since,banned}
const dev=ua=>{ua=String(ua||'');const o=/iPhone/.test(ua)?'iPhone':/iPad/.test(ua)?'iPad':/Android/.test(ua)?'Android':/CrOS/.test(ua)?'ChromeOS':/Windows/.test(ua)?'Windows':/Mac OS X/.test(ua)?'Mac':/Linux/.test(ua)?'Linux':'Unknown',b=/Edg\//.test(ua)?'Edge':/OPR\/|Opera/.test(ua)?'Opera':/SamsungBrowser/.test(ua)?'Samsung':/Firefox\//.test(ua)?'Firefox':/Chrome\//.test(ua)?'Chrome':/Safari\//.test(ua)?'Safari':'Browser';return o+' · '+b};
function seenOf(vid,ipk,req){if(vid.length<8)return null;const s=db.seen[vid]||(db.seen[vid]={first:Date.now(),acts:0,names:[]});s.ipk=ipk;s.last=Date.now();if(req)s.dev=dev(req.headers['user-agent']);return s}
const seenAct=(vid,ipk,req,name)=>{const s=seenOf(vid,ipk,req);if(!s)return;s.acts++;if(name&&!s.names.includes(name)){s.names.unshift(name);s.names=s.names.slice(0,5)}};
function trimSeen(){const n=Date.now(),E=Object.entries(db.seen);for(const[k,s]of E)if(n-(s.last||0)>45*864e5)delete db.seen[k];const R=Object.entries(db.seen).sort((a,b)=>(b[1].last||0)-(a[1].last||0));R.slice(3000).forEach(([k])=>delete db.seen[k])}
const active=b=>!b.until||b.until>Date.now();
function banOf(vid,ipk){db.bans=db.bans.filter(active);return db.bans.find(b=>(vid&&b.vids.includes(vid))||(ipk&&b.ipks.includes(ipk)))}
const banView=b=>({id:b.id,reason:b.reason||'',until:b.until||0,at:b.at});
const sse=(r,ev,o)=>{try{r.write(`event: ${ev}\ndata: ${JSON.stringify(o)}\n\n`)}catch{}};
function enforce(){for(const[r,s]of streams){const b=banOf(s.vid,s.ipk);if(b&&!s.banned){s.banned=1;sse(r,'ban',banView(b))}else if(!b&&s.banned){s.banned=0;sse(r,'unban',{});r.write(`data: ${JSON.stringify(pub())}\n\n`)}}}
const goodKeys=new Set(),okKey=k=>{if(typeof k!=='string'||!k||k.length>200)return false;const t=SHA(k);if(goodKeys.has(t))return true;let ok;if(ADMIN_KEY)ok=k.length===ADMIN_KEY.length&&crypto.timingSafeEqual(Buffer.from(k),Buffer.from(ADMIN_KEY));else{const[salt,h]=ADMIN_HASH.split(':');ok=crypto.timingSafeEqual(crypto.scryptSync(k,salt,32),Buffer.from(h,'hex'))}if(ok)goodKeys.add(t);return ok},fails={};
const isIP=t=>/^(\d{1,3}\.){3}\d{1,3}$/.test(t)||(/^[0-9a-f:]+$/i.test(t)&&t.split(':').length>2);
function resolve(t){t=SS(t,80);if(!t)return null;const lo=t.toLowerCase().replace(/^@/,'');
 if(isIP(t)){const ipk=ipH(t);return{label:'IP '+t.replace(/[\d]+$|[0-9a-f]+$/i,'*'),vids:Object.keys(db.seen).filter(v=>db.seen[v].ipk===ipk),ipks:[ipk],kind:'ip'}}
 if(db.seen[t])return{label:(db.seen[t].names[0]||'Visitor')+' · '+t.slice(0,6),vids:[t],ipks:[db.seen[t].ipk].filter(Boolean),kind:'id'};
 const pre=Object.keys(db.seen).filter(v=>v.toLowerCase().startsWith(lo));if(lo.length>=4&&pre.length===1){const v=pre[0];return{label:(db.seen[v].names[0]||'Visitor')+' · '+v.slice(0,6),vids:[v],ipks:[db.seen[v].ipk].filter(Boolean),kind:'id'}}
 const byName=Object.entries(db.seen).filter(([,s])=>s.names.some(n=>n.toLowerCase()===lo)).sort((a,b)=>(b[1].last||0)-(a[1].last||0));
 if(byName.length){const[v,s]=byName[0];return{label:s.names.find(n=>n.toLowerCase()===lo)+' · '+v.slice(0,6),vids:[v],ipks:[s.ipk].filter(Boolean),names:[lo],kind:'name'}}
 return{label:t,vids:[],ipks:[],names:[lo],kind:'pending'}} // not seen yet: banned the moment someone uses that name in chat
function overview(){const n=Date.now(),on=new Map();for(const s of streams.values()){const o=on.get(s.vid)||{vid:s.vid,tabs:0,since:s.since,banned:!!s.banned};o.tabs++;o.since=Math.min(o.since,s.since);on.set(s.vid,o)}
 const V=v=>{const s=db.seen[v]||{};return{name:s.names?.[0]||'',names:s.names||[],dev:s.dev||'',ipk:(s.ipk||'').slice(0,6),acts:s.acts||0,first:s.first||0,last:s.last||0}};
 return{now:n,keySet:true,cloud:S3?(cloudOK?'on':'error'):'off',online:[...on.values()].map(o=>({...o,...V(o.vid)})).sort((a,b)=>a.since-b.since),
  recent:Object.entries(db.seen).sort((a,b)=>(b[1].last||0)-(a[1].last||0)).slice(0,80).map(([vid])=>({vid,...V(vid),online:on.has(vid)})),
  bans:db.bans.filter(active).map(b=>({...b,vids:b.vids.map(v=>v.slice(0,6)),ipks:b.ipks.length,names:b.names||[]})),
  stats:{online:on.size,visitors24:Object.values(db.seen).filter(s=>n-(s.last||0)<864e5).length,known:Object.keys(db.seen).length,board:Object.keys(db.noms).length,promos:db.promos.length,ballots:db.ballots,chat:db.chat.length,uptime:process.uptime()|0},
  board:board().slice(0,50).map(({key,name,social,count})=>({key,name,social,count})),promos:db.promos.map(({id,title,name,social,at,size,kind,ext,reports,hidden})=>({id,title,name,social,at,size,kind:kind||'video',ext:ext||'mp4',reports:reports||0,hidden:!!hidden}))}}
function admin(url,j,res,req){const ip=ipOf(req),n=Date.now();fails[ip]=(fails[ip]||[]).filter(x=>n-x<6e5);
 
 if(fails[ip].length>=6)return send(res,429,{error:'Too many wrong passwords. Locked for 10 minutes.'});
 if(!okKey(req.headers['x-admin-key']||j.key)){fails[ip].push(n);return send(res,403,{error:'Wrong password.'})}
 if(url==='/api/admin/overview'||url==='/api/admin/check')return send(res,200,overview());
 if(url==='/api/admin/kick'){const v=SS(j.vid,64);let c=0;for(const[r,s]of streams)if(s.vid===v){sse(r,'kick',{reason:SS(j.reason,120)});setTimeout(()=>{try{r.end()}catch{}},250);c++}return c?send(res,200,{ok:1,tabs:c}):send(res,404,{error:'That visitor is not online any more.'})}
 if(url==='/api/admin/ban'){const R=resolve(j.target);if(!R)return send(res,400,{error:'Type a visitor ID, chat name or IP address.'});const mins=Math.max(0,+j.minutes||0);
  const b={id:crypto.randomBytes(5).toString('hex'),label:R.label,kind:R.kind,vids:R.vids,ipks:j.withIp===false&&R.kind!=='ip'?[]:R.ipks,names:R.names||[],reason:SS(j.reason,140),at:n,until:mins?n+mins*6e4:0};
  db.bans.unshift(b);save();enforce();bc();return send(res,200,{ok:1,ban:{...b,vids:b.vids.map(v=>v.slice(0,6)),ipks:b.ipks.length}})}
 if(url==='/api/admin/unban'){const i=db.bans.findIndex(b=>b.id===SS(j.id,20));if(i<0)return send(res,404,{error:'Ban not found (maybe it already expired).'});db.bans.splice(i,1);save();enforce();bc();return send(res,200,{ok:1})}
 if(url==='/api/admin/nominee/remove'){const k=SS(j.key,140);if(!db.noms[k])return send(res,404,{error:'Not on the Board.'});delete db.noms[k];save();bc();return send(res,200,{ok:1})}
 if(url==='/api/admin/promo/remove'){const i=db.promos.findIndex(x=>x.id===SS(j.id,32));if(i<0)return send(res,404,{error:'Not found'});rmP(db.promos[i]);db.promos.splice(i,1);save();bc();return send(res,200,{ok:1})}
 if(url==='/api/admin/chat/clear'){db.chat=[];save();bc();return send(res,200,{ok:1})}
 send(res,404,{error:'Unknown'})}

const pub=()=>{tick();const B=board();return{now:Date.now(),start:db.start,close:db.start+WEEK,frozen:!!db.frozen,announceAt:db.frozen?.announceAt||0,
 board:B.slice(0,50).map(({key,name,social,count})=>({key,name,social,count})),cands:B.slice(0,3).map(c=>c.key),ballots:db.ballots,
 nods:B.reduce((a,c)=>a+c.count,0),champs:db.champs,last:db.last,promos:db.promos.filter(p=>!p.hidden).slice(0,MAXN).map(({id,title,name,social,at,kind,ext})=>({id,title,name,social,at,kind:kind||'video',ext:ext||'mp4'})),
 chat:db.chat.slice(-60).map(({id,name,text,at})=>({id,name,text,at})),online:new Set([...streams.values()].filter(s=>!s.banned).map(s=>s.vid)).size,maxVideoMB:MAXV/MB,maxImageMB:MAXI/MB}};
const bc=()=>{const d=`data: ${JSON.stringify(pub())}\n\n`;for(const[r,s]of streams)if(!s.banned)r.write(d)};
const send=(res,c,o)=>{if(res.headersSent)return;res.writeHead(c,{'Content-Type':'application/json','Cache-Control':'no-store'});res.end(JSON.stringify(o))};
const MIME={html:'text/html',js:'text/javascript',css:'text/css',png:'image/png',jpg:'image/jpeg',webp:'image/webp',gif:'image/gif',mp3:'audio/mpeg',mp4:'video/mp4',webm:'video/webm',json:'application/json',svg:'image/svg+xml'};
const pulls=new Map();
function cachePull(p){if(pulls.has(p.id))return;const tmp=ploc(p)+'.dl';pulls.set(p.id,1);s3('GET',pkey(p)).then(r=>{if(r.statusCode!==200){r.resume();throw Error('status '+r.statusCode)}const ws=fs.createWriteStream(tmp);r.pipe(ws);ws.on('finish',()=>fs.rename(tmp,ploc(p),()=>pulls.delete(p.id)));ws.on('error',()=>{fs.unlink(tmp,()=>{});pulls.delete(p.id)})}).catch(e=>{pulls.delete(p.id);console.error('cache fill failed:',e.message)})}
function promoFile(req,res,p){const f=ploc(p),h={'Content-Type':MIME[p.ext||'mp4'],'Accept-Ranges':'bytes','Cache-Control':'public,max-age=86400','X-Content-Type-Options':'nosniff'};used.set(p.id,Date.now());
 if(fs.existsSync(f))return range(req,res,f,p.size,h);if(!S3)return res.writeHead(404).end('Not found');
 // not cached here (fresh instance): stream it straight from the bucket, and fill the local cache in the background
 const m=/bytes=(\d*)-(\d*)/.exec(req.headers.range||'');s3('GET',pkey(p),null,m?{Range:req.headers.range}:{}).then(r=>{if(r.statusCode>=300&&r.statusCode!==206){r.resume();return res.writeHead(404).end('Not found')}
  res.writeHead(r.statusCode,{...h,...(r.headers['content-length']?{'Content-Length':r.headers['content-length']}:{}),...(r.headers['content-range']?{'Content-Range':r.headers['content-range']}:{})});r.pipe(res);req.on('close',()=>r.destroy())}).catch(()=>res.writeHead(502).end('Storage unavailable'));cachePull(p)}
function range(req,res,f,size,h){const m=/bytes=(\d*)-(\d*)/.exec(req.headers.range||'');
 if(m){let a=m[1]===''?Math.max(0,size-(+m[2]||0)):+m[1],b=m[1]!==''&&m[2]?Math.min(+m[2],size-1):size-1;if(a>=size||a>b){res.writeHead(416,{'Content-Range':`bytes */${size}`});return res.end()}res.writeHead(206,{...h,'Content-Range':`bytes ${a}-${b}/${size}`,'Content-Length':b-a+1});fs.createReadStream(f,{start:a,end:b}).pipe(res)}
 else{res.writeHead(200,{...h,'Content-Length':size});fs.createReadStream(f).pipe(res)}}
function serve(req,res){let p;try{p=decodeURIComponent(req.url.split('?')[0])}catch{return res.writeHead(400).end()}if(p.endsWith('/'))p+='index.html';if(p==='/legal')p='/legal.html';
 const pm=/^\/promo\/([a-f0-9]{16})\.(\w+)$/.exec(p);if(pm){const pr=db.promos.find(x=>x.id===pm[1]&&!x.hidden);return pr?promoFile(req,res,pr):res.writeHead(404).end('Not found')}
 const f=path.join(PUB,path.normalize(p));if(!f.startsWith(PUB))return res.writeHead(403).end();
 fs.stat(f,(e,s)=>{if(e||!s.isFile())return fs.existsSync(path.join(PUB,'index.html'))?res.writeHead(404).end('Not found'):res.writeHead(503,{'Content-Type':'text/html'}).end('<body style="font:18px system-ui;background:#06030f;color:#e9e6ff;padding:40px"><h1>Server is running, website files are missing</h1><p>Upload the <b>public</b> folder (or the site .zip) next to server.js in your GitHub repo, then redeploy.</p><p style="color:#9d93c9;font-size:14px">Diagnostic: '+String(DIAG||'none').replace(/[<&]/g,' ')+' | files next to server.js: '+fs.readdirSync(__dirname).join(', ')+'</p></body>');
  const h={'Content-Type':MIME[path.extname(f).slice(1)]||'application/octet-stream','Accept-Ranges':'bytes','X-Content-Type-Options':'nosniff','Referrer-Policy':'strict-origin-when-cross-origin','Cache-Control':/\.html$/.test(f)?'no-cache':/\.(js|css)$/.test(f)?'public,max-age=300':'public,max-age=86400'};
  if(/\.(html|js|css|json|svg)$/.test(f)&&/\bgzip\b/.test(req.headers['accept-encoding']||'')){const k=f+s.mtimeMs;let z=GZ.get(k);if(!z){z=zlib.gzipSync(fs.readFileSync(f));GZ.set(k,z)}res.writeHead(200,{...h,'Content-Encoding':'gzip',Vary:'Accept-Encoding','Content-Length':z.length});return res.end(z)}
  range(req,res,f,s.size,h)})}
const hits={},GZ=new Map(),EXTRA=(process.env.ALLOWED_ORIGINS||'').split(',').map(x=>x.trim()).filter(Boolean),okOrigin=o=>!!o&&(EXTRA.includes(o)||/^https?:\/\/(localhost(:\d+)?|127\.0\.0\.1(:\d+)?|([a-z0-9-]+\.)*(aacselect\.com|workers\.dev|pages\.dev|onrender\.com))$/.test(o));
const server=http.createServer((req,res)=>{const url=req.url.split('?')[0];
 if(url.startsWith('/api/')){const o=req.headers.origin;if(okOrigin(o))res.setHeader('Access-Control-Allow-Origin',o);res.setHeader('Vary','Origin');res.setHeader('Access-Control-Allow-Headers','Content-Type, X-Admin-Key');res.setHeader('Access-Control-Allow-Methods','GET,POST,OPTIONS');if(req.method==='OPTIONS'){res.writeHead(204);return res.end()}}
 if(url==='/healthz')return send(res,200,{ok:1,up:process.uptime()|0,cloud:S3?(cloudOK?'on':'error'):'off'});
 if(url==='/api/state')return send(res,200,pub());
 if(url==='/api/me'){const q=new URL(req.url,'http://x').searchParams,b=banOf(SS(q.get('vid'),64),ipH(ipOf(req)));return send(res,200,{banned:b?banView(b):null})}
 if(url==='/api/stream'){const q=new URL(req.url,'http://x').searchParams,vid=SS(q.get('vid'),64)||'anon-'+crypto.randomBytes(4).toString('hex'),ipk=ipH(ipOf(req)),b=banOf(vid,ipk);
  res.writeHead(200,{'Content-Type':'text/event-stream','Cache-Control':'no-cache',Connection:'keep-alive','X-Accel-Buffering':'no'});res.write('retry: 4000\n\n');
  const fresh=!db.seen[vid];streams.set(res,{vid,ipk,since:Date.now(),banned:b?1:0});seenOf(vid,ipk,req);if(fresh)save();req.on('close',()=>{streams.delete(res);bc()});
  if(b)return sse(res,'ban',banView(b));return bc()}
 if(url==='/api/admin/overview'&&req.method==='GET')return admin(url,{},res,req);
 if(req.method==='POST'&&(url==='/api/promo'||url==='/api/upload'))return upload(req,res);
 if(req.method==='POST'&&url.startsWith('/api/')){let b='';req.on('data',d=>{b+=d;if(b.length>4e3)req.destroy()});
  req.on('end',()=>{let j;try{j=JSON.parse(b)}catch{return send(res,400,{error:'Bad request'})}
   if(url.startsWith('/api/admin/'))return admin(url,j,res,req);
   const ip=ipOf(req),n=Date.now();
   hits[ip]=(hits[ip]||[]).filter(x=>n-x<6e4);if(hits[ip].length>=14)return send(res,429,{error:'Slow down a little.'});hits[ip].push(n);
   const ipk=ipH(ip),S=SS;tick();const vid=S(j.vid,64),bn=banOf(vid,ipk);if(bn)return send(res,403,{error:'You are banned.',banned:banView(bn)});
   if(url==='/api/nominate'){const name=S(j.name,30),soc=S(j.social,80);
    if(name.length<2)return send(res,400,{error:'Creator name needs at least 2 characters.'});
    if(!SOC.test(soc))return send(res,400,{error:'Social must be an @handle or a link.'});
    if(!clean(name,soc))return send(res,400,{error:'Blocked by the Axiom Guard — keep it clean.'});
    if(vid.length<8)return send(res,400,{error:'Invalid session - reload the page.'});
    const key=findNom((name+'|'+soc).toLowerCase().replace(/\s+/g,''),name,soc)||(name+'|'+soc).toLowerCase().replace(/\s+/g,''),had=!!db.noms[key];
    if(db.nodded[vid+key])return send(res,409,{error:`You already gave ${had?db.noms[key].name:name} your nod this cycle.`,key});
    db.nodded[vid+key]=1;const c=db.noms[key]||(db.noms[key]={name,social:soc,count:0});c.count++;c.at=n;seenAct(vid,ipk,req);save();bc();return send(res,200,{ok:1,key,name:c.name,count:c.count,nod:had})}
   if(url==='/api/nod'){const k=S(j.key,140),c=db.noms[k];if(!c||vid.length<8)return send(res,404,{error:'That creator is no longer on the Board.'});if(db.nodded[vid+k])return send(res,409,{error:'Already nodded.'});
    db.nodded[vid+k]=1;c.count++;c.at=n;seenAct(vid,ipk,req);save();bc();return send(res,200,{ok:1,key:k,count:c.count})}
   if(url==='/api/vote'){const k=S(j.key,140);if(db.frozen)return send(res,400,{error:'Ballots are frozen.'});
    if(vid.length<8||!board().slice(0,3).some(x=>x.key===k))return send(res,400,{error:'Invalid ballot.'});
    if(db.voted[vid]||(db.ipv[ipk]||0)>=3)return send(res,409,{error:'Ballot already cast.'});
    db.voted[vid]=1;db.ipv[ipk]=(db.ipv[ipk]||0)+1;db.votes[k]=(db.votes[k]||0)+1;db.ballots++;seenAct(vid,ipk,req);save();bc();
    return send(res,200,{ok:1,receipt:crypto.createHash('sha1').update(vid+k+n).digest('hex').slice(0,8).toUpperCase()})}
   if(url==='/api/chat'){const name=S(j.name,20),text=S(j.text,240);if(name.length<2||!text)return send(res,400,{error:'Say something.'});
    const nb=db.bans.filter(active).find(b=>(b.names||[]).includes(name.toLowerCase()));if(nb&&vid.length>=8){nb.vids.includes(vid)||nb.vids.push(vid);nb.ipks.includes(ipk)||nb.ipks.push(ipk);save();enforce();return send(res,403,{error:'You are banned.',banned:banView(nb)})}
    if(!clean(name,text))return send(res,400,{error:'Blocked by the Axiom Guard — keep the frequency clean.'});
    db.chat.push({id:n+Math.random().toString(36).slice(2,6),name,text,at:n,vid});db.chat=db.chat.slice(-100);seenAct(vid,ipk,req,name);save();bc();return send(res,200,{ok:1})}
   if(url==='/api/promo/report'){const p=db.promos.find(x=>x.id===S(j.id,32));if(!p||vid.length<8)return send(res,404,{error:'Not found'});p.rep=p.rep||{};if(p.rep[vid])return send(res,409,{error:'You already reported this.'});p.rep[vid]=1;p.reports=(p.reports||0)+1;if(p.reports>=3)p.hidden=1;save();bc();return send(res,200,{ok:1})}
   if(url==='/api/promo/delete'){if(!okKey(S(j.key,200)))return send(res,403,{error:'Not allowed.'});const i=db.promos.findIndex(x=>x.id===S(j.id,32));if(i<0)return send(res,404,{error:'Not found'});rmP(db.promos[i]);db.promos.splice(i,1);save();bc();return send(res,200,{ok:1})}
   send(res,404,{error:'Unknown'})});return}
 serve(req,res)});
setInterval(()=>{tick();prune();trimSeen();const before=db.bans.length;enforce();if(db.bans.length!==before)save();bc();const n=Date.now();for(const k in hits){hits[k]=hits[k].filter(x=>n-x<6e4);if(!hits[k].length)delete hits[k]}for(const s of streams.values())if(db.seen[s.vid])db.seen[s.vid].last=n},2e4);

// ---- STAY AWAKE: Render's free plan sleeps after 15 min without visitors; the server pings its own public address so it never dozes off.
const SELF=(process.env.KEEPALIVE_URL||process.env.RENDER_EXTERNAL_URL||(process.env.RENDER?SITE:'')).replace(/\/+$/,'');
if(SELF&&process.env.KEEPALIVE!=='0'){const ping=()=>fetch(SELF+'/healthz?keepalive=1',{signal:AbortSignal.timeout(2e4)}).then(r=>r.ok||console.warn('keep-alive ping got '+r.status)).catch(e=>console.warn('keep-alive ping failed: '+e.message)).finally(()=>setTimeout(ping,(8+Math.random()*4)*6e4));setTimeout(ping,6e4);console.log('Keep-alive on: pinging '+SELF+'/healthz every ~10 minutes')}

process.on('uncaughtException',e=>console.error('uncaught',e));process.on('unhandledRejection',e=>console.error('unhandled',e));
const bye=async()=>{try{db.savedAt=Date.now();const j=JSON.stringify(db);fs.writeFileSync(DBF,j);if(S3&&cloudOK)await Promise.race([s3put('state.json',j,'application/json'),sleep(15e3)])}catch(e){console.error('final save failed:',e.message)}process.exit(0)};process.on('SIGTERM',bye);process.on('SIGINT',bye);

let port=+PORT;const url=()=>`http://localhost:${port}`;
server.on('listening',()=>{console.log(`\n  ================================================\n   AAC is LIVE  ->  ${url()}\n   Keep this window OPEN. Press Ctrl+C to stop.\n  ================================================\n`);
 console.log('  Website files: '+(fs.existsSync(path.join(PUB,'index.html'))?'OK':'MISSING (API still works)')+'  |  Health check: /healthz  |  Data file: '+DBF+'  |  Cloud bucket: '+(S3?(cloudOK?'ON':'UNREACHABLE'):'off')+'  |  Admin panel: ON ('+(ADMIN_KEY?'password from ADMIN_KEY':'built-in password')+')'+'\n');
 if(process.env.AAC_OPEN==='1'){const{exec}=require('child_process');exec(process.platform==='win32'?`start "" ${url()}`:process.platform==='darwin'?`open ${url()}`:`xdg-open ${url()}`,()=>{})}});
const go=()=>server.listen(port);
server.on('error',e=>{if(e.code==='EADDRINUSE'&&port<+PORT+20){console.log(`Port ${port} is busy (another program or an old copy is using it) - trying ${port+1}...`);port++;setTimeout(go,100)}else{console.error('\nCould not start the server: '+e.message+'\n');process.exit(1)}});
restore().then(()=>{for(const k of Object.keys(FRESH()))if(db[k]==null)db[k]=FRESH()[k];db.bans=(db.bans||[]).map(b=>({vids:[],ipks:[],names:[],...b}));go()});
