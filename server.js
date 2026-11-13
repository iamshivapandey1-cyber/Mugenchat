// MugenChat server - only Node.js needed, no npm packages
const http=require('http'),fs=require('fs'),path=require('path'),crypto=require('crypto');
const PORT=process.env.PORT||3000,DIR=process.env.DATA_DIR||__dirname,DB=path.join(DIR,'data.json'),UP=path.join(DIR,'uploads');
fs.mkdirSync(UP,{recursive:true});
let db={users:[],msgs:[],groups:[],sessions:{}};
try{db=JSON.parse(fs.readFileSync(DB))}catch{try{db=JSON.parse(fs.readFileSync(DB+'.bak'))}catch{}}
db.groups=db.groups||[];db.reqs=db.reqs||[];db.sessions=db.sessions||{};db.msgs.forEach(m=>{m.readBy=m.readBy||(m.read?[m.to]:[]);m.delBy=m.delBy||m.readBy;m.react=m.react||{}});
let tm,lb=0;const flush=()=>{try{const t=DB+'.tmp';fs.writeFileSync(t,JSON.stringify(db));if(Date.now()-lb>6e5&&fs.existsSync(DB)){fs.copyFileSync(DB,DB+'.bak');lb=Date.now()}fs.renameSync(t,DB)}catch(e){console.error('save failed',e.message)}};
const save=()=>{clearTimeout(tm);tm=setTimeout(flush,200)};['SIGTERM','SIGINT'].forEach(g=>process.on(g,()=>{flush();process.exit(0)}));
const hash=(p,s)=>crypto.scryptSync(p,s,32).toString('hex');
const clients=new Map(),online=id=>(clients.get(id)||new Set()).size>0;
const usr=id=>db.users.find(x=>x.id===id),grp=id=>db.groups.find(g=>g.id===id);
const pub=u=>({id:u.id,name:u.name,username:u.username,about:u.about||'',avatar:u.avatar,seen:u.seen,online:online(u.id)}),self=u=>({...pub(u),email:u.email});
const fr=(a,b)=>(a.friends||[]).includes(b.id),blk=(a,b)=>(a.blocked||[]).includes(b.id);
const link=(a,b)=>[[a,b],[b,a]].forEach(([x,y])=>{x.friends=x.friends||[];if(!x.friends.includes(y.id))x.friends.push(y.id)});
const unlink=(a,b)=>[[a,b],[b,a]].forEach(([x,y])=>x.friends=(x.friends||[]).filter(i=>i!==y.id));
const rel=(a,b)=>a.id===b.id?'self':fr(a,b)?'friend':blk(a,b)?'blocked':db.reqs.some(r=>r.from===a.id&&r.to===b.id)?'sent':db.reqs.some(r=>r.from===b.id&&r.to===a.id)?'got':'none';
const pubV=(u,me)=>{if(!u)return{};const r=rel(me,u),p=pub(u);if(r!=='friend'&&r!=='self'){p.online=false;p.seen=0}return{...p,rel:r}};
const members=m=>{const g=grp(m.to);return g?g.members:[m.from,m.to]},others=m=>members(m).filter(x=>x!==m.from);
const view=m=>({...m,readBy:undefined,delBy:undefined,hid:undefined,g:!!grp(m.to),fn:(usr(m.from)||{name:'Deleted account'}).name,st:others(m).every(x=>m.readBy.includes(x))?2:others(m).every(x=>m.delBy.includes(x))?1:0});
const key=(m,u)=>grp(m.to)?m.to:(m.from===u?m.to:m.from);
const info=(id,full,me)=>{const g=grp(id);return g?{id,name:g.name,group:true,members:full?g.members.map(i=>pubV(usr(i),me)):undefined}:pubV(usr(id),me)};
const push=(id,ev,d)=>(clients.get(id)||[]).forEach(r=>r.write(`event: ${ev}\ndata: ${JSON.stringify(d)}\n\n`));
const body=req=>new Promise(r=>{let b='';req.on('data',c=>{b+=c;if(b.length>14e6)req.destroy()});req.on('end',()=>{try{r(JSON.parse(b||'{}'))}catch{r({})}})});
const out=(res,c,o)=>{res.writeHead(c,{'Content-Type':'application/json'});res.end(JSON.stringify(o))};
const newSession=id=>{const t=crypto.randomBytes(24).toString('hex');db.sessions[t]=id;save();return t};
const MT={html:'text/html; charset=utf-8',json:'application/json',js:'text/javascript',png:'image/png',jpg:'image/jpeg',webp:'image/webp',gif:'image/gif',mp4:'video/mp4',webm:'video/webm',mov:'video/quicktime'};
db.users.forEach(u=>{u.friends=u.friends||[];u.blocked=u.blocked||[];if(!u.username){const b0=(u.email.split('@')[0].toLowerCase().replace(/[^a-z0-9._]/g,'')||'user').slice(0,16).padEnd(3,'0');let n=b0,i=1;while(db.users.some(x=>x!==u&&x.username===n))n=b0+(i++);u.username=n}});
db.msgs.forEach(m=>{if(!grp(m.to)){const a=usr(m.from),c=usr(m.to);if(a&&c&&!fr(a,c))link(a,c)}});
http.createServer(async(req,res)=>{
 const u=new URL(req.url,'http://x'),p=u.pathname;
 if(!p.startsWith('/api/')){const f=p==='/'?'index.html':path.basename(p),fp=path.join(p.startsWith('/u/')?UP:path.join(__dirname,'public'),f);
  if(!fs.existsSync(fp)){res.writeHead(404);return res.end()}
  res.writeHead(200,{'Content-Type':MT[f.split('.').pop()]||'text/plain'});return res.end(fs.readFileSync(fp))}
 const b=req.method==='POST'?await body(req):{};
 if(p==='/api/register'){
  const email=String(b.email||'').trim().toLowerCase(),name=String(b.name||'').trim().slice(0,40),pw=String(b.password||'');
  if(!/^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(email))return out(res,400,{error:'Enter a valid email'});
  if(!name)return out(res,400,{error:'Enter your name'});
  const un=String(b.username||'').trim().toLowerCase().replace(/^@/,'');
  if(!/^[a-z0-9._]{3,20}$/.test(un))return out(res,400,{error:'Username: 3-20 letters, numbers, . or _'});
  if(db.users.find(x=>x.username===un))return out(res,400,{error:'This username is taken'});
  if(pw.length<6)return out(res,400,{error:'Password must be at least 6 characters'});
  if(db.users.find(x=>x.email===email))return out(res,400,{error:'This email is already registered'});
  const salt=crypto.randomBytes(16).toString('hex'),user={id:crypto.randomUUID(),name,username:un,email,friends:[],blocked:[],salt,pw:hash(pw,salt)};
  db.users.push(user);return out(res,200,{token:newSession(user.id),user:self(user)});
 }
 if(p==='/api/login'){
  const idf=String(b.email||'').trim().toLowerCase(),user=db.users.find(x=>x.email===idf||x.username===idf.replace(/^@/,''));
  if(!user||hash(String(b.password||''),user.salt)!==user.pw)return out(res,401,{error:'Wrong email or password'});
  return out(res,200,{token:newSession(user.id),user:self(user)});
 }
 const tok=(req.headers.authorization||'').replace('Bearer ','')||u.searchParams.get('token');
 const me=usr(db.sessions[tok]);
 if(!me)return out(res,401,{error:'Please log in'});
 if(p==='/api/me')return out(res,200,self(me));
 if(p==='/api/logout'){delete db.sessions[tok];save();return out(res,200,{});}
 if(p==='/api/profile'){
  if(typeof b.username==='string'){const un=b.username.trim().toLowerCase().replace(/^@/,'');if(un!==me.username){if(!/^[a-z0-9._]{3,20}$/.test(un))return out(res,400,{error:'Username: 3-20 letters, numbers, . or _'});if(db.users.some(x=>x.username===un))return out(res,400,{error:'This username is taken'});me.username=un}}
  if(typeof b.name==='string'&&b.name.trim())me.name=b.name.trim().slice(0,40);
  if(typeof b.about==='string')me.about=b.about.slice(0,100);
  if(typeof b.avatar==='string'&&b.avatar.startsWith('data:image')&&b.avatar.length<4e5)me.avatar=b.avatar;
  save();return out(res,200,self(me));
 }
 if(p==='/api/users'){
  const q=(u.searchParams.get('q')||'').toLowerCase().replace(/^@/,'');
  return out(res,200,(me.friends||[]).map(usr).filter(x=>x&&(x.username.includes(q)||x.name.toLowerCase().includes(q))).slice(0,50).map(x=>pubV(x,me)));
 }
 if(p==='/api/upload'){
  const mm=/^data:(image\/(jpeg|png|webp|gif)|video\/(mp4|webm|quicktime));base64,(.+)$/s.exec(b.data||'');
  if(!mm)return out(res,400,{error:'Unsupported file'});
  const name=crypto.randomUUID()+'.'+{jpeg:'jpg',png:'png',webp:'webp',gif:'gif',mp4:'mp4',webm:'webm',quicktime:'mov'}[mm[2]||mm[3]];
  fs.writeFileSync(path.join(UP,name),Buffer.from(mm[4],'base64'));return out(res,200,{url:'/u/'+name,kind:mm[2]?'image':'video'});
 }
 if(p==='/api/group'){
  const ms=[...new Set([me.id,...(b.members||[]).filter(i=>(me.friends||[]).includes(i))])],name=String(b.name||'').trim().slice(0,40);
  if(!name||ms.length<2)return out(res,400,{error:'Enter a group name and pick members'});
  const g={id:crypto.randomUUID(),name,admin:me.id,members:ms,created:Date.now()};
  db.groups.push(g);save();ms.forEach(x=>push(x,'grp',{}));return out(res,200,{id:g.id});
 }
 if(p==='/api/chats'){
  const m=new Map();
  db.groups.filter(g=>g.members.includes(me.id)&&!(me.cleared&&me.cleared[g.id])).forEach(g=>m.set(g.id,{last:{from:'',text:'Group created',time:g.created},unread:0}));
  for(const x of db.msgs){if(!members(x).includes(me.id)||(x.hid||[]).includes(me.id))continue;const k=key(x,me.id),c=m.get(k)||{unread:0};
   c.last=view(x);if(x.from!==me.id&&!x.readBy.includes(me.id))c.unread++;m.set(k,c)}
  return out(res,200,[...m].map(([id,c])=>({chat:info(id,0,me),last:c.last,unread:c.unread})).sort((a,b)=>b.last.time-a.last.time));
 }
 if(p==='/api/messages'){
  const w=u.searchParams.get('with'),g=grp(w);
  if(g&&!g.members.includes(me.id))return out(res,403,{error:'Not a member'});
  const list=db.msgs.filter(x=>!(x.hid||[]).includes(me.id)&&(g?x.to===w:(!grp(x.to)&&((x.from===me.id&&x.to===w)||(x.from===w&&x.to===me.id)))));
  let ch=false;
  list.forEach(x=>{if(x.from!==me.id&&!x.readBy.includes(me.id)){x.readBy.push(me.id);if(!x.delBy.includes(me.id))x.delBy.push(me.id);ch=true;push(x.from,'upd',{})}});
  if(ch)save();
  return out(res,200,{chat:info(w,true,me),messages:list.map(view)});
 }
 if(p==='/api/send'){
  const g=grp(b.to);
  if(g?!g.members.includes(me.id):(!usr(b.to)||b.to===me.id))return out(res,400,{error:'Invalid chat'});
  if(!g&&!fr(me,usr(b.to)))return out(res,403,{error:'You can only message friends'});
  const text=String(b.text||'').trim().slice(0,2000),audio=typeof b.audio==='string'&&b.audio.startsWith('data:audio')&&b.audio.length<3e6?b.audio:undefined;
  const media=b.media&&typeof b.media.url==='string'&&b.media.url.startsWith('/u/')?{url:b.media.url,kind:b.media.kind==='video'?'video':'image'}:undefined;
  if(!text&&!audio&&!media)return out(res,400,{error:'Empty message'});
  let reply;const q=b.reply&&db.msgs.find(x=>x.id===b.reply&&members(x).includes(me.id)&&!(x.hid||[]).includes(me.id));
  if(q)reply={id:q.id,from:q.from,fn:(usr(q.from)||{}).name,text:q.del?'':(q.text||'').slice(0,80),kind:q.media?q.media.kind:q.audio?'audio':''};
  const m={id:crypto.randomUUID(),from:me.id,to:b.to,text,audio,media,reply,time:Date.now(),react:{},readBy:[],delBy:[]};
  others(m).forEach(x=>online(x)&&m.delBy.push(x));db.msgs.push(m);save();
  const v=view(m);members(m).forEach(x=>push(x,'msg',v));return out(res,200,v);
 }
 if(p==='/api/react'){
  const m=db.msgs.find(x=>x.id===b.id);if(!m||!members(m).includes(me.id))return out(res,400,{error:'Invalid'});
  const e=String(b.e||'').slice(0,8);if(m.react[me.id]===e)delete m.react[me.id];else m.react[me.id]=e;
  save();members(m).forEach(x=>push(x,'upd',{}));return out(res,200,{});
 }
 const T=usr(b.id||b.user);
 if(p==='/api/find'){const q=(u.searchParams.get('q')||'').toLowerCase().replace(/^@/,'').trim();if(q.length<2)return out(res,200,[]);
  return out(res,200,db.users.filter(x=>x.id!==me.id&&!blk(x,me)&&(x.username.includes(q)||x.name.toLowerCase().includes(q))).sort((a,c)=>(c.username===q)-(a.username===q)).slice(0,20).map(x=>pubV(x,me)));}
 if(p==='/api/req'){if(!T||T===me||blk(T,me))return out(res,400,{error:'User not found'});if(blk(me,T))return out(res,400,{error:'Unblock this user first'});
  if(!fr(me,T)){if(db.reqs.some(r=>r.from===T.id&&r.to===me.id)){link(me,T);db.reqs=db.reqs.filter(r=>!(r.from===T.id&&r.to===me.id))}else if(!db.reqs.some(r=>r.from===me.id&&r.to===T.id))db.reqs.push({id:crypto.randomUUID(),from:me.id,to:T.id,time:Date.now()})}
  save();push(T.id,'req',{});push(me.id,'req',{});return out(res,200,{});}
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
  const id=me.id;db.msgs=db.msgs.filter(m=>grp(m.to)||(m.from!==id&&m.to!==id));db.users=db.users.filter(x=>x!==me);
  db.users.forEach(x=>{x.friends=(x.friends||[]).filter(i=>i!==id);x.blocked=(x.blocked||[]).filter(i=>i!==id)});db.reqs=db.reqs.filter(r=>r.from!==id&&r.to!==id);
  db.groups.forEach(g=>g.members=g.members.filter(i=>i!==id));const gs=new Set(db.groups.filter(g=>g.members.length<2).map(g=>g.id));db.groups=db.groups.filter(g=>!gs.has(g.id));db.msgs=db.msgs.filter(m=>!gs.has(m.to));
  Object.keys(db.sessions).forEach(t=>db.sessions[t]===id&&delete db.sessions[t]);save();db.users.forEach(x=>{push(x.id,'req',{});push(x.id,'grp',{})});return out(res,200,{});}
 if(p==='/api/edit'){
  const m=db.msgs.find(x=>x.id===b.id),t=String(b.text||'').trim().slice(0,2000);
  if(!m||m.from!==me.id||m.del||!t)return out(res,400,{error:'Cannot edit this message'});
  m.text=t;m.edited=true;save();members(m).forEach(x=>push(x,'upd',{}));return out(res,200,{});
 }
 if(p==='/api/delete'){
  const m=db.msgs.find(x=>x.id===b.id);if(!m||!members(m).includes(me.id))return out(res,400,{error:'Invalid'});
  if(b.all&&m.from===me.id){m.del=true;m.text='';delete m.audio;delete m.media;delete m.reply;m.react={}}else(m.hid=m.hid||[]).push(me.id);
  save();members(m).forEach(x=>push(x,'upd',{}));return out(res,200,{});
 }
 if(p==='/api/typing'){
  const g=grp(b.to);(g?g.members:[b.to]).filter(x=>x!==me.id&&(g||fr(me,{id:x}))).forEach(x=>push(x,'typing',{chat:g?b.to:me.id,name:me.name}));return out(res,200,{});
 }
 if(p==='/api/signal'){if(!fr(me,{id:b.to}))return out(res,403,{error:'Not friends'});push(b.to,'sig',{from:me.id,name:me.name,data:b.data});return out(res,200,{})}
 if(p==='/api/stream'){
  res.writeHead(200,{'Content-Type':'text/event-stream','Cache-Control':'no-cache',Connection:'keep-alive'});res.write(': ok\n\n');
  if(!clients.has(me.id))clients.set(me.id,new Set());clients.get(me.id).add(res);
  db.msgs.forEach(m=>{if(m.from!==me.id&&members(m).includes(me.id)&&!m.delBy.includes(me.id)){m.delBy.push(me.id);push(m.from,'upd',{})}});save();
  const ka=setInterval(()=>res.write(': ping\n\n'),25000);
  return req.on('close',()=>{clearInterval(ka);clients.get(me.id).delete(res);me.seen=Date.now();save()});
 }
 out(res,404,{error:'Not found'});
}).listen(PORT,()=>console.log('MugenChat is running: http://localhost:'+PORT));
