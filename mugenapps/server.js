// MugenChat server (Node 18+)
// Storage: MongoDB Atlas when MONGODB_URI is set (npm install needed), otherwise a local data.json file.
'use strict';
const http=require('http'),fs=require('fs'),path=require('path'),crypto=require('crypto');
const PORT=process.env.PORT||3000,DIR=process.env.DATA_DIR||__dirname,DB=path.join(DIR,'data.json'),UP=path.join(DIR,'uploads');
const MONGO=process.env.MONGODB_URI||'',MDBNAME=process.env.MONGODB_DB||'mugenchat';
fs.mkdirSync(UP,{recursive:true});

/* ================= storage ================= */
let db={users:[],msgs:[],groups:[],reqs:[],sched:[],meta:{},sessions:{}},mongo=null;
const COLS=['users','msgs','groups','reqs','sched','meta','sessions'];
const docsOf=c=>c==='meta'?Object.entries(db.meta).map(([id,v])=>({...v,id})):c==='sessions'?Object.entries(db.sessions).map(([id,uid])=>({id,uid})):db[c];
const usr=id=>db.users.find(x=>x.id===id),grp=id=>db.groups.find(g=>g.id===id);
const fr=(a,b)=>(a.friends||[]).includes(b.id),blk=(a,b)=>(a.blocked||[]).includes(b.id);
const link=(a,b)=>[[a,b],[b,a]].forEach(([x,y])=>{x.friends=x.friends||[];if(!x.friends.includes(y.id))x.friends.push(y.id)});

function normalize(){
  for(const c of ['users','msgs','groups','reqs','sched'])db[c]=db[c]||[];
  db.meta=db.meta||{};db.sessions=db.sessions||{};
  db.msgs.forEach(m=>{m.readBy=m.readBy||(m.read?[m.to]:[]);m.delBy=m.delBy||m.readBy;m.react=m.react||{}});
  db.users.forEach(u=>{u.friends=u.friends||[];u.blocked=u.blocked||[];if(!u.username){const b0=(u.email.split('@')[0].toLowerCase().replace(/[^a-z0-9._]/g,'')||'user').slice(0,16).padEnd(3,'0');let n=b0,i=1;while(db.users.some(x=>x!==u&&x.username===n))n=b0+(i++);u.username=n}});
  db.msgs.forEach(m=>{if(!grp(m.to)){const a=usr(m.from),c=usr(m.to);if(a&&c&&!fr(a,c))link(a,c)}});
}
async function load(){
  if(MONGO){
    let MC;try{MC=require('mongodb').MongoClient}catch{console.error('\nMongoDB driver is missing. Run:  npm install\n');process.exit(1)}
    const cl=new MC(MONGO,{serverSelectionTimeoutMS:20000});
    try{await cl.connect()}catch(e){console.error('\nCould not connect to MongoDB Atlas: '+e.message+'\n - check MONGODB_URI (special characters in the password must be URL-encoded)\n - Atlas > Network Access: allow 0.0.0.0/0 (Render uses changing IPs)\n');process.exit(1)}
    const d=cl.db(MDBNAME);mongo={d,files:d.collection('files'),snap:{}};
    COLS.forEach(c=>mongo.snap[c]=new Map());
    const empty=(await d.collection('users').estimatedDocumentCount())===0;
    if(empty&&fs.existsSync(DB)){try{db={...db,...JSON.parse(fs.readFileSync(DB))};console.log('Imported your old data.json into MongoDB')}catch{}normalize();save()}
    else{
      const rows=c=>d.collection(c).find({}).toArray();
      for(const c of ['users','msgs','groups','reqs','sched'])db[c]=(await rows(c)).map(r=>{delete r._id;return r});
      db.meta={};for(const r of await rows('meta')){const k=r._id;delete r._id;delete r.id;db.meta[k]=r}
      db.sessions={};for(const r of await rows('sessions'))db.sessions[r._id]=r.uid;
      normalize();
      for(const c of COLS)for(const x of docsOf(c))mongo.snap[c].set(x.id,JSON.stringify(x));
    }
    console.log('MongoDB Atlas connected: database "'+MDBNAME+'"');
  }else{
    try{db=JSON.parse(fs.readFileSync(DB))}catch{try{db=JSON.parse(fs.readFileSync(DB+'.bak'))}catch{}}
    normalize();
  }
}
let tm,lb=0,flushing=false,again=false;
async function flush(){
  if(!mongo){try{const t=DB+'.tmp';fs.writeFileSync(t,JSON.stringify(db));if(Date.now()-lb>6e5&&fs.existsSync(DB)){fs.copyFileSync(DB,DB+'.bak');lb=Date.now()}fs.renameSync(t,DB)}catch(e){console.error('save failed',e.message)}return}
  if(flushing){again=true;return}
  flushing=true;
  try{
    for(const c of COLS){
      const ops=[],pend=[],cur=new Set();
      for(const x of docsOf(c)){const s=JSON.stringify(x);cur.add(x.id);if(mongo.snap[c].get(x.id)!==s){ops.push({replaceOne:{filter:{_id:x.id},replacement:{...x,_id:x.id},upsert:true}});pend.push([x.id,s])}}
      for(const id of mongo.snap[c].keys())if(!cur.has(id)){ops.push({deleteOne:{filter:{_id:id}}});pend.push([id,null])}
      if(ops.length){await mongo.d.collection(c).bulkWrite(ops,{ordered:false});pend.forEach(([id,s])=>s===null?mongo.snap[c].delete(id):mongo.snap[c].set(id,s))}
    }
  }catch(e){console.error('MongoDB save failed (will retry):',e.message);again=true}
  finally{flushing=false;if(again){again=false;setTimeout(flush,2000)}}
}
const save=()=>{clearTimeout(tm);tm=setTimeout(flush,mongo?700:200)};
const shutdown=async()=>{clearTimeout(tm);while(flushing)await new Promise(r=>setTimeout(r,50));await flush();while(flushing)await new Promise(r=>setTimeout(r,50));process.exit(0)};
['SIGTERM','SIGINT'].forEach(g=>process.on(g,shutdown));

/* ---- files (disk cache + MongoDB when enabled) ---- */
async function putFile(name,buf){fs.writeFileSync(path.join(UP,name),buf);if(mongo)await mongo.files.replaceOne({_id:name},{_id:name,data:buf,size:buf.length,t:Date.now()},{upsert:true})}
async function getFile(name){
  const fp=path.join(UP,name);if(fs.existsSync(fp))return fs.readFileSync(fp);
  if(mongo){const r=await mongo.files.findOne({_id:name});if(r){const d=r.data,b=Buffer.isBuffer(d)?d:Buffer.from(d.buffer.subarray(0,d.position??d.buffer.length));try{fs.writeFileSync(fp,b)}catch{}return b}}
  return null;
}
function delFile(name){try{fs.unlinkSync(path.join(UP,name))}catch{}if(mongo)mongo.files.deleteOne({_id:name}).catch(()=>{})}
function rmMedia(m){for(const u of [m.media&&m.media.url,m.audio]){if(typeof u!=='string'||!u.startsWith('/u/'))continue;if(db.msgs.some(x=>x!==m&&((x.media&&x.media.url===u)||x.audio===u)))continue;delFile(path.basename(u))}}

/* ================= helpers ================= */
const hash=(p,s)=>crypto.scryptSync(p,s,32).toString('hex');
const clients=new Map(),online=id=>(clients.get(id)||new Set()).size>0;
const rooms=new Map(); // group call rooms: gid -> {video,m:Set,t}
const pub=u=>({id:u.id,name:u.name,username:u.username,about:u.about||'',avatar:u.avatar,seen:u.seen,online:online(u.id),ring:u.ring}),self=u=>({...pub(u),email:u.email,hideSeen:!!u.hideSeen,noRead:!!u.noRead,ct:u.ct||{}});
const unlink=(a,b)=>[[a,b],[b,a]].forEach(([x,y])=>x.friends=(x.friends||[]).filter(i=>i!==y.id));
const rel=(a,b)=>a.id===b.id?'self':fr(a,b)?'friend':blk(a,b)?'blocked':db.reqs.some(r=>r.from===a.id&&r.to===b.id)?'sent':db.reqs.some(r=>r.from===b.id&&r.to===a.id)?'got':'none';
const pubV=(u,me)=>{if(!u)return{};const r=rel(me,u),p=pub(u);if((r!=='friend'&&r!=='self')||(u.hideSeen&&r!=='self')){p.online=false;p.seen=0}return{...p,rel:r}};
const members=m=>{const g=grp(m.to);return g?g.members:[m.from,m.to]},others=m=>members(m).filter(x=>x!==m.from);
const view=m=>{
  const o=others(m),rd=x=>m.readBy.includes(x)&&!(usr(x)||{}).noRead;
  const v={...m,readBy:undefined,delBy:undefined,hid:undefined,star:undefined,voBy:undefined,g:!!grp(m.to),fn:m.sys?'':(usr(m.from)||{name:'Deleted account'}).name,st:o.every(rd)?2:o.every(x=>m.delBy.includes(x))?1:0};
  if(m.vo){v.vb=m.voBy||[];if(m.media)v.media={kind:m.media.kind,name:'',vo:1}}
  return v;
};
const key=(m,u)=>grp(m.to)?m.to:(m.from===u?m.to:m.from);
const ckey=(id,meId)=>grp(id)?id:[id,meId].sort().join('|');
const metaOf=k=>db.meta[k]||(db.meta[k]={});
const roomInfo=id=>{const r=rooms.get(id);return r&&r.m.size?{video:r.video,n:r.m.size}:undefined};
const info=(id,full,me)=>{
  const g=grp(id),mt=db.meta[ckey(id,me.id)]||{};let pin;
  if(mt.pin){const x=db.msgs.find(q=>q.id===mt.pin);if(x&&!x.del&&!(x.hid||[]).includes(me.id))pin={id:x.id,text:(x.text||'').slice(0,80),kind:x.media?x.media.kind:x.audio?'audio':x.poll?'poll':'',fn:(usr(x.from)||{}).name}}
  const ex={ttl:mt.ttl||0,pin};
  if(g){const adm=(g.admins||[g.admin]).includes(me.id);return{id,name:g.name,group:true,avatar:g.avatar,desc:g.desc||'',admins:g.admins||[g.admin],onlyAdmins:!!g.onlyAdmins,lockInfo:!!g.lockInfo,inv:adm?g.inv:undefined,gtheme:mt.theme,call:roomInfo(id),members:full?g.members.map(i=>pubV(usr(i),me)):undefined,...ex}}
  return{...pubV(usr(id),me),...ex};
};
const push=(id,ev,d)=>(clients.get(id)||[]).forEach(r=>r.write(`event: ${ev}\ndata: ${JSON.stringify(d)}\n\n`));
const body=req=>new Promise(r=>{let b='';req.on('data',c=>{b+=c;if(b.length>14e6)req.destroy()});req.on('end',()=>{try{r(JSON.parse(b||'{}'))}catch{r({})}})});
const out=(res,c,o)=>{res.writeHead(c,{'Content-Type':'application/json'});res.end(JSON.stringify(o))};
const gm=(g,text)=>{const m={id:crypto.randomUUID(),from:'',to:g.id,text,sys:1,time:Date.now(),react:{},readBy:[],delBy:[]};db.msgs.push(m);g.members.forEach(x=>push(x,'msg',view(m)))};
const newSession=id=>{const t=crypto.randomBytes(24).toString('hex');db.sessions[t]=id;save();return t};
const chatMembers=(to,me)=>{const g=grp(to);return g?g.members:[me.id,to]};
const isAdm=(g,id)=>(g.admins||[g.admin]).includes(id);
const hits=new Map();
const tooMany=(k,max,ms)=>(hits.get(k)||[]).filter(t=>Date.now()-t<ms).length>=max;
const hit=k=>{const a=(hits.get(k)||[]).filter(t=>Date.now()-t<6e5);a.push(Date.now());hits.set(k,a)};
setInterval(()=>{for(const[k,a]of hits)if(!a.some(t=>Date.now()-t<6e5))hits.delete(k)},60000).unref();

const DU=/^data:([a-z]+\/[a-z0-9.+-]+)(?:;[^;,]+)*?;base64,(.+)$/s;
const EXT={'image/jpeg':'jpg','image/png':'png','image/webp':'webp','image/gif':'gif','video/mp4':'mp4','video/webm':'webm','video/quicktime':'mov','application/pdf':'pdf','application/zip':'zip','text/plain':'txt','text/csv':'csv','application/msword':'doc','application/vnd.openxmlformats-officedocument.wordprocessingml.document':'docx','application/vnd.ms-excel':'xls','application/vnd.openxmlformats-officedocument.spreadsheetml.sheet':'xlsx','application/vnd.openxmlformats-officedocument.presentationml.presentation':'pptx','audio/mpeg':'mp3'};
const AEXT={'audio/webm':'weba','audio/ogg':'ogg','audio/mp4':'m4a','audio/x-m4a':'m4a','audio/mpeg':'mp3','audio/wav':'wav','audio/aac':'aac'};
const MT={html:'text/html; charset=utf-8',json:'application/json',js:'text/javascript',png:'image/png',jpg:'image/jpeg',webp:'image/webp',gif:'image/gif',mp4:'video/mp4',webm:'video/webm',mov:'video/quicktime',weba:'audio/webm',ogg:'audio/ogg',m4a:'audio/mp4',mp3:'audio/mpeg',wav:'audio/wav',aac:'audio/aac'};
const allow=new Map(),al=(a,c)=>{if(!allow.has(a))allow.set(a,new Set());allow.get(a).add(c)};
const voTok=new Map(); // one-time view tokens for view-once media
const voFile=name=>db.msgs.some(m=>m.vo&&m.media&&m.media.url==='/u/'+name);

/* ---- personalisation validation ---- */
const hx=v=>/^#[0-9a-f]{6}$/i.test(v)?v:'';
const pick=(v,a,d)=>a.includes(v)?v:d;
const num=(v,a,b,d)=>{v=+v;return Number.isFinite(v)?Math.max(a,Math.min(b,v)):d};
const FONTS=['','Poppins','Caveat','Pacifico','Merriweather','Space Mono','Dancing Script','Bangers','Orbitron','Lobster','Roboto Slab','Indie Flower'];
const FRAMES=['','double','dashed','dotted','gold','neon','crown','hearts','stars','flame','sparkle','leaf'];
const FX=['','float','bounce','shake','heartbeat','wobble','spin','swing'];
const ANIM=['','pop','slide','zoom','bounce','flip','fade','shake','rocket','blur'];
const ctSan=t=>{t=t||{};const o={};
  o.wp=pick(String(t.wp||''),['','c','i',...Array.from({length:14},(_,i)=>'g'+i)],'');
  o.wc=hx(t.wc);o.wpi=typeof t.wpi==='string'&&/^\/u\/[\w.-]+$/.test(t.wpi)?t.wpi:'';
  o.dim=num(t.dim,0,80,0);o.blur=num(t.blur,0,20,0);
  o.c1=hx(t.c1);o.c2=hx(t.c2);o.bc=hx(t.bc);o.tc=hx(t.tc);
  o.font=pick(String(t.font||''),FONTS,'');o.fs=pick(String(t.fs||''),['','s','m','l','xl'],'');
  o.bub=pick(String(t.bub||''),['','round','sharp','leaf','pill','tail'],'');
  o.bs=pick(String(t.bs||''),['','grad','solid','neon','glass'],'');
  o.sa=pick(String(t.sa||''),ANIM,'');o.ra=pick(String(t.ra||''),ANIM,'');
  o.ty=pick(String(t.ty||''),['','none','glow','sparkle','ripple'],'');
  o.ti=pick(String(t.ti||''),['','dots','wave','bars','pen'],'');
  o.snd=pick(String(t.snd||''),['','pop','whoosh','ding','none'],'');
  return o};
const ringSan=r=>({on:!!r.on,c1:hx(r.c1)||'#7c3aed',c2:hx(r.c2)||'#06b6d4',mode:pick(r.mode,['solid','pulse','spin','rainbow','breathe'],'solid'),w:num(r.w,2,12,3),gl:num(r.gl,0,40,14),sp:num(r.sp,.6,5,1.8),frame:pick(r.frame,FRAMES,''),fx:pick(r.fx,FX,'')});

/* ================= sending ================= */
async function doSend(me,b){
  const g=grp(b.to);
  if(g?!g.members.includes(me.id):(!usr(b.to)||b.to===me.id))return{e:'Invalid chat'};
  if(!g&&!fr(me,usr(b.to)))return{e:'You can only message friends',c:403};
  if(g&&g.onlyAdmins&&!isAdm(g,me.id))return{e:'Only admins can send messages in this group',c:403};
  const text=String(b.text||'').trim().slice(0,2000);
  let audio;
  if(typeof b.audio==='string'&&b.audio.startsWith('data:audio')&&b.audio.length<4e6){
    const mm=DU.exec(b.audio),ex=mm&&AEXT[mm[1]];if(!ex)return{e:'Unsupported audio'};
    const name=crypto.randomUUID()+'.'+ex;await putFile(name,Buffer.from(mm[2],'base64'));audio='/u/'+name;
  }else if(typeof b.audio==='string'&&/^\/u\/[\w-]+\.(weba|ogg|m4a|mp3|wav|aac)$/.test(b.audio))audio=b.audio; // forwarded voice note
  const media=b.media&&typeof b.media.url==='string'&&/^\/u\/[\w.-]+$/.test(b.media.url)?{url:b.media.url,kind:['video','file'].includes(b.media.kind)?b.media.kind:'image',name:String(b.media.name||'').slice(0,80)}:undefined;
  let poll;
  if(b.poll&&typeof b.poll==='object'){const q=String(b.poll.q||'').trim().slice(0,200),opts=(Array.isArray(b.poll.opts)?b.poll.opts:[]).map(x=>String(x||'').trim().slice(0,80)).filter(Boolean).slice(0,8);
    if(!q||opts.length<2)return{e:'A poll needs a question and at least 2 options'};poll={q,multi:!!b.poll.multi,opts:opts.map(t=>({t,v:[]}))}}
  if(!text&&!audio&&!media&&!poll)return{e:'Empty message'};
  let reply;const q=b.reply&&db.msgs.find(x=>x.id===b.reply&&members(x).includes(me.id)&&!(x.hid||[]).includes(me.id));
  if(q)reply={id:q.id,from:q.from,fn:(usr(q.from)||{}).name,text:q.del?'':(q.text||'').slice(0,80),kind:q.media?q.media.kind:q.audio?'audio':q.poll?'poll':''};
  const m={id:crypto.randomUUID(),from:me.id,to:b.to,text,audio,media,poll,reply,fwd:b.fwd?1:undefined,time:Date.now(),react:{},readBy:[],delBy:[]};
  if(b.vo&&media&&media.kind!=='file')m.vo=1;
  const bn=+b.burn;if([3,5,10,30,60,300].includes(bn))m.burn=bn;
  const tl=(db.meta[ckey(b.to,me.id)]||{}).ttl;if(tl)m.exp=Date.now()+tl;
  others(m).forEach(x=>online(x)&&m.delBy.push(x));db.msgs.push(m);save();
  const v=view(m);members(m).forEach(x=>push(x,'msg',v));
  return{m};
}

/* ================= server ================= */
async function handle(req,res){
  const u=new URL(req.url,'http://x'),p=u.pathname,ip=(req.headers['x-forwarded-for']||'').split(',')[0].trim()||req.socket.remoteAddress;
  if(!p.startsWith('/api/')){
    if(p.startsWith('/u/')){
      const f=path.basename(p),vo=voFile(f);
      if(vo){const t=voTok.get(u.searchParams.get('vt')||'');if(!t||t.f!==f||t.x<Date.now()){res.writeHead(403);return res.end()}}
      const buf=await getFile(f);if(!buf){res.writeHead(404);return res.end()}
      const ex=f.split('.').pop();
      return sendBuf(req,res,buf,MT[ex]||'application/octet-stream',{'X-Content-Type-Options':'nosniff','Cache-Control':vo?'no-store':'private, max-age=86400',...(!MT[ex]?{'Content-Disposition':'attachment'}:{})});
    }
    const f=p==='/'?'index.html':path.basename(p),fp=path.join(__dirname,'public',f);
    if(!fs.existsSync(fp)){res.writeHead(404);return res.end()}
    const ex=f.split('.').pop();res.writeHead(200,{'Content-Type':MT[ex]||'application/octet-stream','X-Content-Type-Options':'nosniff',...(ex==='html'||ex==='js'?{'Cache-Control':'no-cache'}:{})});return res.end(fs.readFileSync(fp));
  }
  const b=req.method==='POST'?await body(req):{};
  if(p==='/api/register'){
    if(tooMany('r:'+ip,10,3600000))return out(res,429,{error:'Too many sign-ups from this network. Try later.'});
    const email=String(b.email||'').trim().toLowerCase(),name=String(b.name||'').trim().slice(0,40),pw=String(b.password||'');
    if(!/^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(email))return out(res,400,{error:'Enter a valid email'});
    if(!name)return out(res,400,{error:'Enter your name'});
    const un=String(b.username||'').trim().toLowerCase().replace(/^@/,'');
    if(!/^[a-z0-9._]{3,20}$/.test(un))return out(res,400,{error:'Username: 3-20 letters, numbers, . or _'});
    if(db.users.find(x=>x.username===un))return out(res,400,{error:'This username is taken'});
    if(pw.length<6)return out(res,400,{error:'Password must be at least 6 characters'});
    if(db.users.find(x=>x.email===email))return out(res,400,{error:'This email is already registered'});
    const salt=crypto.randomBytes(16).toString('hex'),user={id:crypto.randomUUID(),name,username:un,email,friends:[],blocked:[],salt,pw:hash(pw,salt)};
    db.users.push(user);hit('r:'+ip);return out(res,200,{token:newSession(user.id),user:self(user)});
  }
  if(p==='/api/login'){
    const idf=String(b.email||'').trim().toLowerCase(),lk='l:'+ip+':'+idf;
    if(tooMany(lk,8,600000))return out(res,429,{error:'Too many wrong attempts. Wait 10 minutes and try again.'});
    const user=db.users.find(x=>x.email===idf||x.username===idf.replace(/^@/,''));
    if(!user||hash(String(b.password||''),user.salt)!==user.pw){hit(lk);return out(res,401,{error:'Wrong email or password'})}
    return out(res,200,{token:newSession(user.id),user:self(user)});
  }
  const tok=(req.headers.authorization||'').replace('Bearer ','')||u.searchParams.get('token');
  const me=usr(db.sessions[tok]);
  if(!me)return out(res,401,{error:'Please log in'});
  if(p==='/api/me')return out(res,200,self(me));
  if(p==='/api/logout'){delete db.sessions[tok];save();return out(res,200,{})}
  if(p==='/api/sessions')return out(res,200,{n:Object.values(db.sessions).filter(i=>i===me.id).length});
  if(p==='/api/logoutall'){Object.keys(db.sessions).forEach(t=>db.sessions[t]===me.id&&t!==tok&&delete db.sessions[t]);save();return out(res,200,{})}
  if(p==='/api/profile'){
    if(typeof b.username==='string'){const un=b.username.trim().toLowerCase().replace(/^@/,'');if(un!==me.username){if(!/^[a-z0-9._]{3,20}$/.test(un))return out(res,400,{error:'Username: 3-20 letters, numbers, . or _'});if(db.users.some(x=>x.username===un))return out(res,400,{error:'This username is taken'});me.username=un}}
    if(b.ring&&typeof b.ring==='object')me.ring=ringSan(b.ring);
    if(typeof b.hideSeen==='boolean')me.hideSeen=b.hideSeen;
    if(typeof b.noRead==='boolean')me.noRead=b.noRead;
    if(typeof b.name==='string'&&b.name.trim())me.name=b.name.trim().slice(0,40);
    if(typeof b.about==='string')me.about=b.about.slice(0,100);
    if(typeof b.avatar==='string'&&b.avatar.startsWith('data:image')&&b.avatar.length<4e5)me.avatar=b.avatar;
    save();db.users.forEach(x=>fr(x,me)&&push(x.id,'presence',{}));return out(res,200,self(me));
  }
  if(p==='/api/ct'){ // per-chat look: wallpaper, colours, fonts, animations
    const id=String(b.chat||'');if(id!=='*'&&!grp(id)&&!usr(id))return out(res,400,{error:'Invalid chat'});
    me.ct=me.ct||{};
    if(b.t===null)delete me.ct[id];else me.ct[id]=ctSan(b.t);
    if(b.both&&usr(id)&&fr(me,usr(id))){const T2=usr(id);T2.ct=T2.ct||{};if(b.t===null)delete T2.ct[me.id];else T2.ct[me.id]=me.ct[id];push(T2.id,'ct',{})}
    save();return out(res,200,{ct:me.ct});
  }
  if(p==='/api/users'){
    const q=(u.searchParams.get('q')||'').toLowerCase().replace(/^@/,'');
    return out(res,200,(me.friends||[]).map(usr).filter(x=>x&&(x.username.includes(q)||x.name.toLowerCase().includes(q))).slice(0,50).map(x=>pubV(x,me)));
  }
  if(p==='/api/upload'){
    const mm=DU.exec(b.data||''),ex=mm&&EXT[mm[1]];
    if(!ex)return out(res,400,{error:'This file type is not supported'});
    const name=crypto.randomUUID()+'.'+ex;await putFile(name,Buffer.from(mm[2],'base64'));
    return out(res,200,{url:'/u/'+name,kind:mm[1].startsWith('image')?'image':mm[1].startsWith('video')?'video':'file'});
  }
  if(p==='/api/group'){
    const ms=[...new Set([me.id,...(b.members||[]).filter(i=>(me.friends||[]).includes(i))])],name=String(b.name||'').trim().slice(0,40);
    if(!name||ms.length<2)return out(res,400,{error:'Enter a group name and pick members'});
    const g={id:crypto.randomUUID(),name,admin:me.id,admins:[me.id],members:ms,created:Date.now()};
    db.groups.push(g);save();ms.forEach(x=>push(x,'grp',{}));return out(res,200,{id:g.id});
  }
  if(p==='/api/join'){ // join a group with an invite code
    const g=db.groups.find(x=>x.inv&&x.inv===String(b.code||''));
    if(!g)return out(res,404,{error:'This invite link is not valid any more'});
    if(!g.members.includes(me.id)){if(g.members.length>=30)return out(res,400,{error:'This group is full (30 members)'});g.members.push(me.id);gm(g,me.name+' joined using the invite link');save();g.members.forEach(x=>push(x,'grp',{}))}
    return out(res,200,{id:g.id});
  }
  if(p==='/api/chats'){
    const m=new Map();
    db.groups.filter(g=>g.members.includes(me.id)&&!(me.cleared&&me.cleared[g.id])).forEach(g=>m.set(g.id,{last:{from:'',text:'Group created',time:g.created},unread:0}));
    for(const x of db.msgs){if(!members(x).includes(me.id)||(x.hid||[]).includes(me.id))continue;const k=key(x,me.id),c=m.get(k)||{unread:0};
      c.last=view(x);if(x.from!==me.id&&!x.sys&&!x.readBy.includes(me.id))c.unread++;m.set(k,c)}
    const mu=id=>(me.muted||[]).includes(id)||(me.mu&&me.mu[id]>Date.now());
    return out(res,200,[...m].map(([id,c])=>({chat:info(id,0,me),last:c.last,unread:c.unread,pin:(me.pins||[]).includes(id),muted:!!mu(id),arch:(me.arch||[]).includes(id)})).filter(c=>c.chat&&c.chat.id).sort((a,b)=>b.pin-a.pin||b.last.time-a.last.time));
  }
  if(p==='/api/messages'){
    const w=u.searchParams.get('with'),g=grp(w);
    if(g&&!g.members.includes(me.id))return out(res,403,{error:'Not a member'});
    const list=db.msgs.filter(x=>!(x.hid||[]).includes(me.id)&&(g?x.to===w:(!grp(x.to)&&((x.from===me.id&&x.to===w)||(x.from===w&&x.to===me.id)))));
    let ch=false;
    list.forEach(x=>{if(x.from!==me.id&&!x.readBy.includes(me.id)){x.readBy.push(me.id);if(!x.delBy.includes(me.id))x.delBy.push(me.id);if(x.burn&&!x.exp)x.exp=Date.now()+x.burn*1000;ch=true;push(x.from,'upd',{})}});
    if(ch)save();
    return out(res,200,{chat:info(w,true,me),messages:list.map(x=>({...view(x),starred:(x.star||[]).includes(me.id)}))});
  }
  if(p==='/api/send'){
    if(tooMany('s:'+me.id,60,60000))return out(res,429,{error:'Slow down a little'});hit('s:'+me.id);
    const at=+b.at;
    if(at&&at>Date.now()+10000){
      if(at>Date.now()+30*864e5)return out(res,400,{error:'You can schedule up to 30 days ahead'});
      const g=grp(b.to);if(g?!g.members.includes(me.id):(!usr(b.to)||!fr(me,usr(b.to))))return out(res,400,{error:'Invalid chat'});
      const t=String(b.text||'').trim().slice(0,2000);if(!t&&!b.media)return out(res,400,{error:'Empty message'});
      if(db.sched.filter(s=>s.from===me.id).length>=50)return out(res,400,{error:'You have too many scheduled messages'});
      const s={id:crypto.randomUUID(),from:me.id,at,b:{to:b.to,text:t,media:b.media,reply:b.reply,burn:b.burn}};db.sched.push(s);save();return out(res,200,{sched:s.id,at});
    }
    const r=await doSend(me,b);return r.e?out(res,r.c||400,{error:r.e}):out(res,200,view(r.m));
  }
  if(p==='/api/sched')return out(res,200,db.sched.filter(s=>s.from===me.id).map(s=>({id:s.id,at:s.at,to:s.b.to,text:s.b.text,chat:(grp(s.b.to)||usr(s.b.to)||{}).name})));
  if(p==='/api/sched/cancel'){db.sched=db.sched.filter(s=>!(s.id===b.id&&s.from===me.id));save();return out(res,200,{})}
  if(p==='/api/react'){
    const m=db.msgs.find(x=>x.id===b.id);if(!m||!members(m).includes(me.id))return out(res,400,{error:'Invalid'});
    const e=String(b.e||'').slice(0,8);if(m.react[me.id]===e)delete m.react[me.id];else m.react[me.id]=e;
    save();members(m).forEach(x=>push(x,'upd',{}));return out(res,200,{});
  }
  if(p==='/api/vote'){
    const m=db.msgs.find(x=>x.id===b.id&&x.poll&&members(x).includes(me.id)),i=+b.i;
    if(!m||!m.poll.opts[i])return out(res,400,{error:'Invalid poll'});
    const o=m.poll.opts[i],had=o.v.includes(me.id);
    if(!m.poll.multi)m.poll.opts.forEach(x=>x.v=x.v.filter(id=>id!==me.id));
    if(!had&&!o.v.includes(me.id))o.v.push(me.id);else if(had&&m.poll.multi)o.v=o.v.filter(id=>id!==me.id);
    save();members(m).forEach(x=>push(x,'upd',{}));return out(res,200,{});
  }
  if(p==='/api/vo'){ // open a view-once photo/video (works only one time per person)
    const m=db.msgs.find(x=>x.id===b.id&&x.vo&&x.media&&members(x).includes(me.id));
    if(!m||m.from===me.id||m.media.gone)return out(res,400,{error:'Not available'});
    m.voBy=m.voBy||[];if(m.voBy.includes(me.id))return out(res,410,{error:'You already opened this'});
    m.voBy.push(me.id);const t=crypto.randomBytes(12).toString('hex'),url=m.media.url;voTok.set(t,{f:path.basename(url),x:Date.now()+3e5});
    if(others(m).every(x=>m.voBy.includes(x)))setTimeout(()=>{if(!db.msgs.includes(m)||!m.media)return;rmMedia(m);m.media={kind:m.media.kind,url:'/u/gone',gone:1};save()},3.2e5);
    save();members(m).forEach(x=>push(x,'upd',{}));return out(res,200,{url:url+'?vt='+t,kind:m.media.kind});
  }
  if(p==='/api/msginfo'){
    const m=db.msgs.find(x=>x.id===u.searchParams.get('id')&&x.from===me.id);if(!m)return out(res,400,{error:'Not available'});
    const o=others(m),nm=i=>(usr(i)||{name:'?'}).name,rd=o.filter(x=>m.readBy.includes(x)&&!(usr(x)||{}).noRead);
    return out(res,200,{read:rd.map(nm),delivered:o.filter(x=>!rd.includes(x)&&m.delBy.includes(x)).map(nm),sent:o.filter(x=>!rd.includes(x)&&!m.delBy.includes(x)).map(nm),time:m.time,exp:m.exp});
  }
  if(p==='/api/search'){
    const q=(u.searchParams.get('q')||'').toLowerCase().trim();if(q.length<2)return out(res,200,[]);
    return out(res,200,db.msgs.filter(m=>!m.del&&!m.sys&&m.text&&members(m).includes(me.id)&&!(m.hid||[]).includes(me.id)&&m.text.toLowerCase().includes(q)).slice(-60).reverse().map(m=>({m:view(m),chat:info(key(m,me.id),0,me)})));
  }
  if(p==='/api/ttl'||p==='/api/pin'){
    const to=String(b.to||''),g=grp(to);
    if(g?!g.members.includes(me.id):(!usr(to)||!fr(me,usr(to))))return out(res,400,{error:'Invalid chat'});
    if(g&&g.lockInfo&&!isAdm(g,me.id))return out(res,403,{error:'Only admins can change this'});
    const mt=metaOf(ckey(to,me.id));
    if(p==='/api/ttl'){const t=pick(+b.ttl,[0,864e5,6048e5,7776e6],0);if(t)mt.ttl=t;else delete mt.ttl;if(g)gm(g,me.name+(t?' turned on disappearing messages ('+(t===864e5?'24 hours':t===6048e5?'7 days':'90 days')+')':' turned off disappearing messages'))}
    else{const m=db.msgs.find(x=>x.id===b.id&&members(x).includes(me.id));if(b.id&&!m)return out(res,400,{error:'Invalid message'});if(b.id)mt.pin=b.id;else delete mt.pin}
    save();chatMembers(to,me).forEach(x=>push(x,g?'grp':'upd',{}));return out(res,200,{});
  }
  const T=usr(b.id||b.user);
  if(p==='/api/find'){const q=(u.searchParams.get('q')||'').toLowerCase().replace(/^@/,'').trim();if(q.length<2)return out(res,200,[]);
    return out(res,200,db.users.filter(x=>x.id!==me.id&&!blk(x,me)&&(x.username.includes(q)||x.name.toLowerCase().includes(q))).sort((a,c)=>(c.username===q)-(a.username===q)).slice(0,20).map(x=>pubV(x,me)));}
  if(p==='/api/req'){ // friend request to one person, or to many at once (ids:[...])
    const ids=Array.isArray(b.ids)?b.ids.slice(0,30):[b.id||b.user];let n=0;
    for(const i of ids){const T2=usr(i);if(!T2||T2===me||blk(T2,me)||blk(me,T2))continue;
      if(!fr(me,T2)){if(db.reqs.some(r=>r.from===T2.id&&r.to===me.id)){link(me,T2);db.reqs=db.reqs.filter(r=>!(r.from===T2.id&&r.to===me.id))}else if(!db.reqs.some(r=>r.from===me.id&&r.to===T2.id))db.reqs.push({id:crypto.randomUUID(),from:me.id,to:T2.id,time:Date.now()})}
      n++;push(T2.id,'req',{})}
    if(!n)return out(res,400,{error:'User not found'});save();push(me.id,'req',{});return out(res,200,{n});}
  if(p==='/api/reqs')return out(res,200,{in:db.reqs.filter(r=>r.to===me.id).map(r=>({id:r.id,user:pubV(usr(r.from),me)})),out:db.reqs.filter(r=>r.from===me.id).map(r=>({id:r.id,user:pubV(usr(r.to),me)}))});
  if(p==='/api/accept'){const r=db.reqs.find(x=>x.id===b.id&&x.to===me.id),f=r&&usr(r.from);if(!f)return out(res,400,{error:'Request not found'});link(me,f);db.reqs=db.reqs.filter(x=>x!==r);save();push(f.id,'req',{});push(me.id,'req',{});return out(res,200,{});}
  if(p==='/api/decline'){const gone=db.reqs.filter(r=>(r.from===me.id||r.to===me.id)&&(r.id===b.id||(b.user&&(r.from===b.user||r.to===b.user))));db.reqs=db.reqs.filter(r=>!gone.includes(r));save();gone.forEach(r=>{push(r.from,'req',{});push(r.to,'req',{})});return out(res,200,{});}
  if(p==='/api/friends')return out(res,200,{friends:(me.friends||[]).map(usr).filter(Boolean).map(x=>pubV(x,me)),blocked:(me.blocked||[]).map(usr).filter(Boolean).map(x=>pubV(x,me))});
  if(p==='/api/unfriend'||p==='/api/block'||p==='/api/unblock'){if(!T||T===me)return out(res,400,{error:'User not found'});
    if(p!=='/api/unblock')unlink(me,T);
    if(p==='/api/block'){db.reqs=db.reqs.filter(r=>!((r.from===me.id&&r.to===T.id)||(r.from===T.id&&r.to===me.id)));me.blocked=me.blocked||[];if(!me.blocked.includes(T.id))me.blocked.push(T.id)}
    if(p==='/api/unblock')me.blocked=(me.blocked||[]).filter(i=>i!==T.id);
    save();push(T.id,'req',{});push(me.id,'req',{});return out(res,200,{});}
  if(p==='/api/clearchats'){const ids=new Set(b.ids||[]);db.msgs.forEach(m=>{if(members(m).includes(me.id)&&ids.has(key(m,me.id))){m.hid=m.hid||[];if(!m.hid.includes(me.id))m.hid.push(me.id)}});me.cleared=me.cleared||{};ids.forEach(i=>me.cleared[i]=Date.now());save();return out(res,200,{});}
  if(p==='/api/password'){const np=String(b.new||'');if(hash(String(b.old||''),me.salt)!==me.pw)return out(res,401,{error:'Current password is wrong'});if(np.length<6)return out(res,400,{error:'New password must be at least 6 characters'});
    me.salt=crypto.randomBytes(16).toString('hex');me.pw=hash(np,me.salt);Object.keys(db.sessions).forEach(t=>db.sessions[t]===me.id&&t!==tok&&delete db.sessions[t]);save();return out(res,200,{});}
  if(p==='/api/deleteaccount'){if(hash(String(b.password||''),me.salt)!==me.pw)return out(res,401,{error:'Wrong password'});
    const id=me.id;db.msgs.filter(m=>!grp(m.to)&&(m.from===id||m.to===id)).forEach(rmMedia);db.msgs=db.msgs.filter(m=>grp(m.to)||(m.from!==id&&m.to!==id));db.users=db.users.filter(x=>x!==me);db.sched=db.sched.filter(s=>s.from!==id);
    db.users.forEach(x=>{x.friends=(x.friends||[]).filter(i=>i!==id);x.blocked=(x.blocked||[]).filter(i=>i!==id)});db.reqs=db.reqs.filter(r=>r.from!==id&&r.to!==id);
    db.groups.forEach(g=>{g.members=g.members.filter(i=>i!==id);g.admins=(g.admins||[]).filter(i=>i!==id);if(!g.admins.length&&g.members.length)g.admins=[g.members[0]]});const gs=new Set(db.groups.filter(g=>g.members.length<2).map(g=>g.id));db.groups=db.groups.filter(g=>!gs.has(g.id));db.msgs=db.msgs.filter(m=>!gs.has(m.to));
    Object.keys(db.sessions).forEach(t=>db.sessions[t]===id&&delete db.sessions[t]);save();db.users.forEach(x=>{push(x.id,'req',{});push(x.id,'grp',{})});return out(res,200,{});}
  const G0=grp(b.id);
  if(p==='/api/chatflag'){
    const f=pick(b.flag,['pins','arch'],'muted'),st=new Set(me[f]||[]);(b.ids||[]).forEach(i=>b.on?st.add(i):st.delete(i));
    if(f==='muted'){me.mu=me.mu||{};const until=+b.until;(b.ids||[]).forEach(i=>{if(b.on&&until>Date.now()){me.mu[i]=until;st.delete(i)}else delete me.mu[i]})}
    me[f]=[...st];save();return out(res,200,{});}
  if(p==='/api/star'){const m=db.msgs.find(x=>x.id===b.id);if(!m||!members(m).includes(me.id))return out(res,400,{error:'Invalid'});m.star=m.star||[];const i=m.star.indexOf(me.id);i<0?m.star.push(me.id):m.star.splice(i,1);save();return out(res,200,{starred:i<0});}
  if(p==='/api/starred')return out(res,200,db.msgs.filter(m=>(m.star||[]).includes(me.id)&&!m.del&&!(m.hid||[]).includes(me.id)&&members(m).includes(me.id)).slice(-100).reverse().map(m=>({m:view(m),chat:info(key(m,me.id),0,me)})));
  if(p.startsWith('/api/group/')){
    if(!G0||!G0.members.includes(me.id))return out(res,400,{error:'Group not found'});
    const adm=isAdm(G0,me.id),ping=()=>{save();[...new Set([...G0.members,me.id,b.user].filter(Boolean))].forEach(x=>push(x,'grp',{}))};
    if(p==='/api/group/edit'){
      if(G0.lockInfo&&!adm)return out(res,403,{error:'Only admins can edit this group'});
      if(typeof b.name==='string'&&b.name.trim()&&b.name.trim()!==G0.name){G0.name=b.name.trim().slice(0,40);gm(G0,me.name+' renamed the group to "'+G0.name+'"')}
      if(typeof b.desc==='string')G0.desc=b.desc.slice(0,200);
      if(typeof b.avatar==='string'&&b.avatar.startsWith('data:image')&&b.avatar.length<4e5){G0.avatar=b.avatar;gm(G0,me.name+' changed the group photo')}
      ping();return out(res,200,{});}
    if(p==='/api/group/set'){ // admin-only: group rules and shared look
      if(!adm)return out(res,403,{error:'Only admins can do this'});
      if(typeof b.onlyAdmins==='boolean'){G0.onlyAdmins=b.onlyAdmins;gm(G0,me.name+(b.onlyAdmins?' allowed only admins to send messages':' allowed everyone to send messages'))}
      if(typeof b.lockInfo==='boolean'){G0.lockInfo=b.lockInfo;gm(G0,me.name+(b.lockInfo?' allowed only admins to edit group info':' allowed everyone to edit group info'))}
      if('theme' in b){const mt=metaOf(G0.id);if(b.theme===null)delete mt.theme;else{mt.theme=ctSan(b.theme);gm(G0,me.name+' changed the group theme')}}
      ping();return out(res,200,{});}
    if(p==='/api/group/invite'){if(!adm)return out(res,403,{error:'Only admins can do this'});if(b.off)delete G0.inv;else if(!G0.inv||b.reset)G0.inv=crypto.randomBytes(6).toString('base64url');ping();return out(res,200,{inv:G0.inv});}
    if(p==='/api/group/add'){const ids=[...new Set(b.members||[])].filter(i=>usr(i)&&!G0.members.includes(i)&&fr(me,{id:i}));if(!ids.length)return out(res,400,{error:'Pick friends who are not in the group'});
      if(G0.members.length+ids.length>30)return out(res,400,{error:'A group can have up to 30 members'});
      G0.members.push(...ids);ids.forEach(i=>gm(G0,me.name+' added '+usr(i).name));ping();return out(res,200,{});}
    if(p==='/api/group/remove'){if(!adm)return out(res,403,{error:'Only admins can remove members'});if(!G0.members.includes(b.user)||b.user===me.id)return out(res,400,{error:'Invalid member'});
      const nm=usr(b.user).name;G0.members=G0.members.filter(i=>i!==b.user);G0.admins=(G0.admins||[]).filter(i=>i!==b.user);leaveRoom(G0.id,b.user);gm(G0,me.name+' removed '+nm);ping();return out(res,200,{});}
    if(p==='/api/group/admin'){if(!adm)return out(res,403,{error:'Only admins can do this'});if(!G0.members.includes(b.user))return out(res,400,{error:'Invalid member'});
      G0.admins=G0.admins||[G0.admin];if(b.on){if(!G0.admins.includes(b.user))G0.admins.push(b.user);gm(G0,usr(b.user).name+' is now an admin')}else if(G0.admins.length>1)G0.admins=G0.admins.filter(i=>i!==b.user);ping();return out(res,200,{});}
    if(p==='/api/group/leave'){G0.members=G0.members.filter(i=>i!==me.id);G0.admins=(G0.admins||[]).filter(i=>i!==me.id);leaveRoom(G0.id,me.id);
      if(!G0.members.length){db.groups=db.groups.filter(g=>g!==G0);db.msgs.filter(m=>m.to===G0.id).forEach(rmMedia);db.msgs=db.msgs.filter(m=>m.to!==G0.id);delete db.meta[G0.id]}else{if(!G0.admins.length)G0.admins=[G0.members[0]];gm(G0,me.name+' left')}
      b.user=me.id;ping();return out(res,200,{});}
  }
  if(p==='/api/gcall'){ // group voice/video call rooms (everyone connects to everyone)
    if(!G0||!G0.members.includes(me.id))return out(res,400,{error:'Group not found'});
    if(b.act==='leave'){leaveRoom(G0.id,me.id);return out(res,200,{})}
    let r=rooms.get(G0.id);const fresh=!r;
    if(!r){r={video:!!b.video,m:new Set(),t:Date.now()};rooms.set(G0.id,r)}
    if(!r.m.has(me.id)&&r.m.size>=6)return out(res,400,{error:'A group call can have up to 6 people'});
    const peers=[...r.m].filter(i=>i!==me.id&&usr(i)).map(i=>pub(usr(i)));r.m.add(me.id);
    G0.members.forEach(x=>{if(x===me.id)return;push(x,'gcall',fresh?{act:'ring',gid:G0.id,gname:G0.name,from:me.id,fname:me.name,video:r.video}:{act:'upd',gid:G0.id})});
    return out(res,200,{peers,video:r.video});
  }
  if(p==='/api/edit'){
    const m=db.msgs.find(x=>x.id===b.id),t=String(b.text||'').trim().slice(0,2000);
    if(!m||m.from!==me.id||m.del||!t||m.poll)return out(res,400,{error:'Cannot edit this message'});
    m.text=t;m.edited=true;save();members(m).forEach(x=>push(x,'upd',{}));return out(res,200,{});
  }
  if(p==='/api/delete'){
    const m=db.msgs.find(x=>x.id===b.id);if(!m||!members(m).includes(me.id))return out(res,400,{error:'Invalid'});
    if(b.all&&m.from===me.id){const old={media:m.media,audio:m.audio};m.del=true;m.text='';delete m.audio;delete m.media;delete m.reply;delete m.poll;delete m.vo;m.react={};rmMedia(old)}else(m.hid=m.hid||[]).push(me.id);
    save();members(m).forEach(x=>push(x,'upd',{}));return out(res,200,{});
  }
  if(p==='/api/typing'){
    const g=grp(b.to);(g?g.members:[b.to]).filter(x=>x!==me.id&&(g||fr(me,{id:x}))).forEach(x=>push(x,'typing',{chat:g?b.to:me.id,name:me.name,k:b.k==='rec'?'rec':''}));return out(res,200,{});
  }
  if(p==='/api/signal'){
    const to=usr(b.to),okf=i=>fr(me,{id:i})||(allow.get(me.id)||new Set()).has(i)||db.groups.some(g=>g.members.includes(me.id)&&g.members.includes(i));
    if(!to||!okf(to.id))return out(res,403,{error:'Not allowed'});
    if(b.data&&b.data.t==='offer'&&Array.isArray(b.data.with))b.data.with.slice(0,5).forEach(i=>{if(usr(i)&&okf(i)){al(to.id,i);al(i,to.id)}});
    push(b.to,'sig',{from:me.id,name:me.name,data:b.data});return out(res,200,{})}
  if(p==='/api/stream'){
    res.writeHead(200,{'Content-Type':'text/event-stream','Cache-Control':'no-cache',Connection:'keep-alive','X-Accel-Buffering':'no'});res.write(': ok\n\n');
    const was=online(me.id);
    if(!clients.has(me.id))clients.set(me.id,new Set());clients.get(me.id).add(res);
    db.msgs.forEach(m=>{if(m.from!==me.id&&members(m).includes(me.id)&&!m.delBy.includes(me.id)){m.delBy.push(me.id);push(m.from,'upd',{})}});save();
    if(!was)db.users.forEach(x=>fr(x,me)&&push(x.id,'presence',{}));
    const ka=setInterval(()=>res.write(': ping\n\n'),25000);
    return req.on('close',()=>{clearInterval(ka);clients.get(me.id).delete(res);me.seen=Date.now();save();
      if(!online(me.id)){rooms.forEach((r,gid)=>leaveRoom(gid,me.id));db.users.forEach(x=>fr(x,me)&&push(x.id,'presence',{}))}});
  }
  out(res,404,{error:'Not found'});
}
function leaveRoom(gid,uid){
  const r=rooms.get(gid);if(!r||!r.m.delete(uid))return;const g=grp(gid);
  if(!r.m.size){rooms.delete(gid);if(g)g.members.forEach(x=>push(x,'gcall',{act:'end',gid}))}else if(g)g.members.forEach(x=>push(x,'gcall',{act:'upd',gid}));
}
function sendBuf(req,res,buf,type,extra){
  const h={'Content-Type':type,'Accept-Ranges':'bytes',...extra},r=/^bytes=(\d*)-(\d*)$/.exec(req.headers.range||'');
  if(r&&(r[1]||r[2])){const s=r[1]===''?Math.max(0,buf.length-(+r[2])):+r[1],e=r[1]===''||r[2]===''?buf.length-1:Math.min(+r[2],buf.length-1);
    if(s>e||s>=buf.length){res.writeHead(416,{'Content-Range':'bytes */'+buf.length});return res.end()}
    res.writeHead(206,{...h,'Content-Range':`bytes ${s}-${e}/${buf.length}`,'Content-Length':e-s+1});return res.end(buf.subarray(s,e+1))}
  res.writeHead(200,{...h,'Content-Length':buf.length});res.end(buf);
}

/* ================= background jobs ================= */
function startJobs(){
  setInterval(()=>{ // disappearing + burn-after-read messages
    const now=Date.now(),gone=db.msgs.filter(m=>m.exp&&m.exp<=now);if(!gone.length)return;
    const s=new Set(gone),to=new Set();db.msgs=db.msgs.filter(m=>!s.has(m));
    gone.forEach(m=>{rmMedia(m);members(m).forEach(x=>to.add(x))});save();to.forEach(x=>push(x,'upd',{}));
  },4000).unref();
  setInterval(async()=>{ // scheduled messages
    const now=Date.now(),due=db.sched.filter(s=>s.at<=now);if(!due.length)return;
    db.sched=db.sched.filter(s=>!due.includes(s));save();
    for(const s of due){const u=usr(s.from);if(u){try{await doSend(u,{...s.b})}catch(e){console.error('scheduled send failed',e.message)}}}
  },3000).unref();
}

load().then(()=>{
  startJobs();
  http.createServer((req,res)=>handle(req,res).catch(e=>{console.error(e);try{out(res,500,{error:'Server error'})}catch{}})).listen(PORT,()=>console.log('MugenChat is running: http://localhost:'+PORT+(mongo?'  (data in MongoDB Atlas)':'  (data in local data.json)')));
});
