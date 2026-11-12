// MugenChat server - only Node.js needed, no npm packages
const http=require('http'),fs=require('fs'),path=require('path'),crypto=require('crypto');
const PORT=process.env.PORT||3000,DB=path.join(__dirname,'data.json'),UP=path.join(__dirname,'uploads');
fs.mkdirSync(UP,{recursive:true});
let db={users:[],msgs:[],groups:[],sessions:{}};
try{db=JSON.parse(fs.readFileSync(DB))}catch{}
db.groups=db.groups||[];db.msgs.forEach(m=>{m.readBy=m.readBy||(m.read?[m.to]:[]);m.delBy=m.delBy||m.readBy;m.react=m.react||{}});
let tm;const save=()=>{clearTimeout(tm);tm=setTimeout(()=>fs.writeFileSync(DB,JSON.stringify(db)),200)};
const hash=(p,s)=>crypto.scryptSync(p,s,32).toString('hex');
const clients=new Map(),online=id=>(clients.get(id)||new Set()).size>0;
const usr=id=>db.users.find(x=>x.id===id),grp=id=>db.groups.find(g=>g.id===id);
const pub=u=>({id:u.id,name:u.name,email:u.email,about:u.about||'',avatar:u.avatar,seen:u.seen,online:online(u.id)});
const members=m=>{const g=grp(m.to);return g?g.members:[m.from,m.to]},others=m=>members(m).filter(x=>x!==m.from);
const view=m=>({...m,readBy:undefined,delBy:undefined,hid:undefined,g:!!grp(m.to),fn:(usr(m.from)||{}).name,st:others(m).every(x=>m.readBy.includes(x))?2:others(m).every(x=>m.delBy.includes(x))?1:0});
const key=(m,u)=>grp(m.to)?m.to:(m.from===u?m.to:m.from);
const info=(id,full)=>{const g=grp(id);return g?{id,name:g.name,group:true,members:full?g.members.map(i=>pub(usr(i))):undefined}:pub(usr(id)||{})};
const push=(id,ev,d)=>(clients.get(id)||[]).forEach(r=>r.write(`event: ${ev}\ndata: ${JSON.stringify(d)}\n\n`));
const body=req=>new Promise(r=>{let b='';req.on('data',c=>{b+=c;if(b.length>14e6)req.destroy()});req.on('end',()=>{try{r(JSON.parse(b||'{}'))}catch{r({})}})});
const out=(res,c,o)=>{res.writeHead(c,{'Content-Type':'application/json'});res.end(JSON.stringify(o))};
const newSession=id=>{const t=crypto.randomBytes(24).toString('hex');db.sessions[t]=id;save();return t};
const MT={html:'text/html; charset=utf-8',json:'application/json',js:'text/javascript',png:'image/png',jpg:'image/jpeg',webp:'image/webp',gif:'image/gif',mp4:'video/mp4',webm:'video/webm',mov:'video/quicktime'};
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
  if(pw.length<6)return out(res,400,{error:'Password must be at least 6 characters'});
  if(db.users.find(x=>x.email===email))return out(res,400,{error:'This email is already registered'});
  const salt=crypto.randomBytes(16).toString('hex'),user={id:crypto.randomUUID(),name,email,salt,pw:hash(pw,salt)};
  db.users.push(user);return out(res,200,{token:newSession(user.id),user:pub(user)});
 }
 if(p==='/api/login'){
  const user=db.users.find(x=>x.email===String(b.email||'').trim().toLowerCase());
  if(!user||hash(String(b.password||''),user.salt)!==user.pw)return out(res,401,{error:'Wrong email or password'});
  return out(res,200,{token:newSession(user.id),user:pub(user)});
 }
 const tok=(req.headers.authorization||'').replace('Bearer ','')||u.searchParams.get('token');
 const me=usr(db.sessions[tok]);
 if(!me)return out(res,401,{error:'Please log in'});
 if(p==='/api/me')return out(res,200,pub(me));
 if(p==='/api/logout'){delete db.sessions[tok];save();return out(res,200,{});}
 if(p==='/api/profile'){
  if(typeof b.name==='string'&&b.name.trim())me.name=b.name.trim().slice(0,40);
  if(typeof b.about==='string')me.about=b.about.slice(0,100);
  if(typeof b.avatar==='string'&&b.avatar.startsWith('data:image')&&b.avatar.length<4e5)me.avatar=b.avatar;
  save();return out(res,200,pub(me));
 }
 if(p==='/api/users'){
  const q=(u.searchParams.get('q')||'').toLowerCase();
  return out(res,200,db.users.filter(x=>x.id!==me.id&&(x.email.includes(q)||x.name.toLowerCase().includes(q))).slice(0,20).map(pub));
 }
 if(p==='/api/upload'){
  const mm=/^data:(image\/(jpeg|png|webp|gif)|video\/(mp4|webm|quicktime));base64,(.+)$/s.exec(b.data||'');
  if(!mm)return out(res,400,{error:'Unsupported file'});
  const name=crypto.randomUUID()+'.'+{jpeg:'jpg',png:'png',webp:'webp',gif:'gif',mp4:'mp4',webm:'webm',quicktime:'mov'}[mm[2]||mm[3]];
  fs.writeFileSync(path.join(UP,name),Buffer.from(mm[4],'base64'));return out(res,200,{url:'/u/'+name,kind:mm[2]?'image':'video'});
 }
 if(p==='/api/group'){
  const ms=[...new Set([me.id,...(b.members||[]).filter(usr)])],name=String(b.name||'').trim().slice(0,40);
  if(!name||ms.length<2)return out(res,400,{error:'Enter a group name and pick members'});
  const g={id:crypto.randomUUID(),name,admin:me.id,members:ms,created:Date.now()};
  db.groups.push(g);save();ms.forEach(x=>push(x,'grp',{}));return out(res,200,{id:g.id});
 }
 if(p==='/api/chats'){
  const m=new Map();
  db.groups.filter(g=>g.members.includes(me.id)).forEach(g=>m.set(g.id,{last:{from:'',text:'Group created',time:g.created},unread:0}));
  for(const x of db.msgs){if(!members(x).includes(me.id)||(x.hid||[]).includes(me.id))continue;const k=key(x,me.id),c=m.get(k)||{unread:0};
   c.last=view(x);if(x.from!==me.id&&!x.readBy.includes(me.id))c.unread++;m.set(k,c)}
  return out(res,200,[...m].map(([id,c])=>({chat:info(id),last:c.last,unread:c.unread})).sort((a,b)=>b.last.time-a.last.time));
 }
 if(p==='/api/messages'){
  const w=u.searchParams.get('with'),g=grp(w);
  if(g&&!g.members.includes(me.id))return out(res,403,{error:'Not a member'});
  const list=db.msgs.filter(x=>!(x.hid||[]).includes(me.id)&&(g?x.to===w:(!grp(x.to)&&((x.from===me.id&&x.to===w)||(x.from===w&&x.to===me.id)))));
  let ch=false;
  list.forEach(x=>{if(x.from!==me.id&&!x.readBy.includes(me.id)){x.readBy.push(me.id);if(!x.delBy.includes(me.id))x.delBy.push(me.id);ch=true;push(x.from,'upd',{})}});
  if(ch)save();
  return out(res,200,{chat:info(w,true),messages:list.map(view)});
 }
 if(p==='/api/send'){
  const g=grp(b.to);
  if(g?!g.members.includes(me.id):(!usr(b.to)||b.to===me.id))return out(res,400,{error:'Invalid chat'});
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
  const g=grp(b.to);(g?g.members:[b.to]).filter(x=>x!==me.id).forEach(x=>push(x,'typing',{chat:g?b.to:me.id,name:me.name}));return out(res,200,{});
 }
 if(p==='/api/signal'){push(b.to,'sig',{from:me.id,name:me.name,data:b.data});return out(res,200,{})}
 if(p==='/api/stream'){
  res.writeHead(200,{'Content-Type':'text/event-stream','Cache-Control':'no-cache',Connection:'keep-alive'});res.write(': ok\n\n');
  if(!clients.has(me.id))clients.set(me.id,new Set());clients.get(me.id).add(res);
  db.msgs.forEach(m=>{if(m.from!==me.id&&members(m).includes(me.id)&&!m.delBy.includes(me.id)){m.delBy.push(me.id);push(m.from,'upd',{})}});save();
  const ka=setInterval(()=>res.write(': ping\n\n'),25000);
  return req.on('close',()=>{clearInterval(ka);clients.get(me.id).delete(res);me.seen=Date.now();save()});
 }
 out(res,404,{error:'Not found'});
}).listen(PORT,()=>console.log('MugenChat is running: http://localhost:'+PORT));
