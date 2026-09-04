pub const html = "<!doctype html>
<html lang='en'>
<head>
  <meta charset='utf-8'>
  <meta name='viewport' content='width=device-width, initial-scale=1'>
  <title>Gleam Stage / Demand Lab</title>
  <style>
    :root { --ink:#dfe7e5; --muted:#768681; --bg:#0b100f; --panel:#111917; --line:#25332f; --acid:#b8f34a; --cyan:#45d7c2; --hot:#ff8159; }
    * { box-sizing:border-box; }
    body { margin:0; color:var(--ink); background:var(--bg); font-family:ui-monospace,SFMono-Regular,Menlo,monospace; min-height:100vh; }
    body:before { content:''; position:fixed; inset:0; pointer-events:none; opacity:.22; background:linear-gradient(rgba(255,255,255,.025) 1px,transparent 1px),linear-gradient(90deg,rgba(255,255,255,.025) 1px,transparent 1px); background-size:32px 32px; }
    main { width:min(1120px,calc(100% - 32px)); margin:0 auto; padding:42px 0 70px; position:relative; }
    header { display:flex; align-items:flex-end; justify-content:space-between; gap:24px; margin-bottom:32px; border-bottom:1px solid var(--line); padding-bottom:22px; }
    .eyebrow { color:var(--acid); letter-spacing:.16em; text-transform:uppercase; font-size:12px; }
    h1 { font-family:Georgia,serif; font-size:clamp(38px,7vw,78px); line-height:.9; font-weight:400; margin:10px 0 0; letter-spacing:-.055em; }
    .status { color:var(--cyan); border:1px solid var(--cyan); padding:8px 12px; font-size:12px; text-transform:uppercase; }
    .grid { display:grid; grid-template-columns:repeat(4,1fr); gap:1px; background:var(--line); border:1px solid var(--line); }
    .metric { background:var(--panel); padding:22px; min-height:132px; }
    .metric label { color:var(--muted); text-transform:uppercase; font-size:11px; letter-spacing:.12em; }
    .metric strong { display:block; font-size:clamp(28px,4vw,48px); margin-top:20px; font-weight:500; }
    .metric.buffer strong { color:var(--hot); }
    .flow { margin-top:28px; border:1px solid var(--line); background:var(--panel); padding:26px; }
    .flow-head { display:flex; justify-content:space-between; margin-bottom:30px; color:var(--muted); font-size:12px; text-transform:uppercase; }
    .track { display:grid; grid-template-columns:1fr minmax(80px,.6fr) 1fr minmax(80px,.6fr) 1fr; align-items:center; gap:12px; }
    .node { border:1px solid var(--line); padding:18px; text-align:center; background:#0d1412; transition:.2s; min-width:0; }
    .node.active { border-color:var(--acid); box-shadow:0 0 24px rgba(184,243,74,.14); transform:translateY(-2px); }
    .node b { display:block; color:var(--acid); margin-bottom:5px; }
    .node small { display:block; color:var(--muted); font-size:10px; margin-top:7px; overflow-wrap:anywhere; }
    .node .pid { color:var(--cyan); padding:5px; background:#08100e; }
    .node .work { margin-top:12px; padding-top:10px; border-top:1px solid var(--line); color:var(--ink); }
    .rail { height:58px; position:relative; overflow:hidden; border-left:1px solid var(--line); border-right:1px solid var(--line); }
    .rail:after { content:''; position:absolute; top:28px; left:0; right:0; border-top:1px dashed var(--muted); }
    .rail em { position:absolute; z-index:3; font-style:normal; font-size:9px; letter-spacing:.08em; color:var(--muted); background:var(--panel); padding:2px 4px; }
    .rail em:first-of-type { top:3px; left:7px; color:var(--cyan); }
    .rail em:last-of-type { bottom:2px; right:7px; color:var(--acid); }
    .message { position:absolute; z-index:4; top:20px; min-width:22px; height:18px; border:1px solid var(--cyan); color:var(--bg); background:var(--cyan); padding:2px 4px; font-size:9px; text-align:center; opacity:0; }
    .rail.forward .message { animation:move-right .55s ease-in-out; }
    .rail.reverse .message { border-color:var(--acid); background:var(--acid); animation:move-left .55s ease-in-out; }
    @keyframes move-right { 0%{left:-10px;opacity:0} 15%,85%{opacity:1} 100%{left:calc(100% - 10px);opacity:0} }
    @keyframes move-left { 0%{right:-10px;opacity:0} 15%,85%{opacity:1} 100%{right:calc(100% - 10px);opacity:0} }
    .controls { display:grid; grid-template-columns:1fr 1fr; gap:18px; margin-top:18px; }
    .control { border:1px solid var(--line); background:var(--panel); padding:22px; }
    .control h2 { font:400 18px Georgia,serif; margin:0 0 16px; }
    button { appearance:none; border:1px solid var(--line); color:var(--ink); background:#16201d; padding:12px 15px; font:inherit; cursor:pointer; margin:4px 4px 4px 0; transition:.15s; }
    button:hover { border-color:var(--acid); color:var(--acid); transform:translateY(-1px); }
    button.hot:hover { border-color:var(--hot); color:var(--hot); }
    button.auto { display:block; width:100%; margin-top:14px; border-color:#355148; }
    button.auto.running { color:var(--bg); border-color:var(--acid); background:var(--acid); }
    .hint { color:var(--muted); font-size:10px; line-height:1.5; margin:12px 0 0; }
    .events { margin-top:18px; border:1px solid var(--line); padding:22px; min-height:118px; }
    .events label { color:var(--muted); text-transform:uppercase; font-size:11px; }
    #event-list { display:flex; flex-wrap:wrap; gap:7px; margin-top:16px; }
    .event { border:1px solid #315048; color:var(--cyan); padding:6px 8px; font-size:12px; animation:arrive .25s ease-out; }
    .timeline { margin-top:18px; border:1px solid var(--line); background:var(--panel); }
    .timeline h2 { font:400 18px Georgia,serif; margin:0; padding:20px 22px; border-bottom:1px solid var(--line); }
    .activity { display:grid; grid-template-columns:82px 78px 1fr auto; gap:14px; align-items:center; padding:12px 22px; border-bottom:1px solid rgba(37,51,47,.65); font-size:11px; }
    .activity time { color:var(--muted); }
    .activity .kind { color:var(--acid); }
    .activity .route { color:var(--muted); overflow-wrap:anywhere; }
    .activity .route b { color:var(--ink); font-weight:400; }
    .activity .amount { color:var(--cyan); font-size:13px; }
    .empty { color:var(--muted); padding:22px; font-size:11px; }
    @keyframes arrive { from{opacity:0;transform:translateY(8px)} }
    footer { color:var(--muted); font-size:11px; margin-top:18px; display:flex; justify-content:space-between; }
    @media(max-width:760px){.grid{grid-template-columns:1fr 1fr}.controls{grid-template-columns:1fr}.track{grid-template-columns:1fr}.rail{height:42px}.node{padding:12px 5px}.activity{grid-template-columns:65px 65px 1fr;padding:10px 12px}.activity .amount{display:none}header{align-items:flex-start;flex-direction:column}}
  </style>
</head>
<body>
<main>
  <header><div><div class='eyebrow'>BEAM / backpressure observatory</div><h1>Demand Lab</h1></div><div class='status' id='status'>actor online</div></header>
  <section class='grid'>
    <div class='metric'><label>events pushed</label><strong id='pushed'>0</strong></div>
    <div class='metric'><label>events delivered</label><strong id='received'>0</strong></div>
    <div class='metric'><label>open demand</label><strong id='demand'>0</strong></div>
    <div class='metric buffer'><label>buffered</label><strong id='buffered'>0</strong></div>
  </section>
  <section class='flow'><div class='flow-head'><span>3 live BEAM processes</span><span>messages travel between independent mailboxes</span></div><div class='track'><div class='node' id='producer-process'><b>PRODUCER PROCESS</b><span id='producer-name'>counter-producer</span><small class='pid' id='producer-pid'>loading PID</small><small>owns event generation</small><small class='work'>generated: <span id='producer-work'>0</span></small></div><div class='rail' id='producer-rail'><em>PUSH →</em><em></em><i class='message'>0</i></div><div class='node' id='stage-process'><b>STAGE PROCESS</b>DemandDispatcher<small class='pid' id='stage-pid'>loading PID</small><small>owns demand + FIFO buffer</small><small class='work'>waiting: <span id='stage-work'>0</span></small></div><div class='rail' id='consumer-rail'><em>DELIVER →</em><em>← DEMAND</em><i class='message'>0</i></div><div class='node' id='consumer-process'><b>CONSUMER PROCESS</b><span id='consumer-name'>dashboard-consumer</span><small class='pid' id='consumer-pid'>loading PID</small><small id='subscription-name'>subscription: dashboard-consumer</small><small class='work'>consumed: <span id='consumer-work'>0</span></small></div></div></section>
  <section class='controls'>
    <div class='control'><h2>01 / Generate events</h2><button class='hot' onclick='act(&quot;push&quot;,5)'>Push 5</button><button class='hot' onclick='act(&quot;push&quot;,100)'>Push 100</button><button class='hot' onclick='act(&quot;push&quot;,5000)'>Burst 5,000</button><button class='auto' onclick='startDemo()'>Run observable demo</button><p class='hint'>Queues 100 events, then starts a slow consumer.</p></div>
    <div class='control'><h2>02 / Release demand</h2><button onclick='act(&quot;ask&quot;,5)'>Ask 5</button><button onclick='act(&quot;ask&quot;,25)'>Ask 25</button><button onclick='act(&quot;ask&quot;,500)'>Ask 500</button><button class='auto' id='slow-toggle' onclick='toggleSlow()'>Start slow consumer · 5 / 1200ms</button><p class='hint'>A slow consumer asks only when it is ready, so the stage buffer drains visibly.</p></div>
  </section>
  <section class='events'><label>latest delivered events</label><div id='event-list'></div></section>
  <section class='timeline'><h2>Protocol activity</h2><div id='timeline'><div class='empty'>Push events or ask for demand to begin.</div></div></section>
  <footer><span>gleam_stage · pure core + generic runtime + OTP adapter</span><span id='clock'>poll 300ms</span></footer>
</main>
<script>
const fmt = new Intl.NumberFormat();
let renderedRevision = -1;
let refreshing = false;
let acting = false;
let slowTimer = null;
let latestSnapshot = null;
let lastAnimatedSequence = 0;
let animating = false;
const animationQueue = [];
const observedAt = new Map();
function queueActivity(activity){animationQueue.push(activity);runNextAnimation()}
function runNextAnimation(){
  if(animating || !animationQueue.length) return;
  animating=true;
  animateActivity(animationQueue.shift());
}
function animateActivity(activity){
  const config={PUSH:['producer-rail','forward','producer-process','stage-process'],DEMAND:['consumer-rail','reverse','consumer-process','stage-process'],DELIVER:['consumer-rail','forward','stage-process','consumer-process']}[activity.kind];
  if(!config){animating=false;runNextAnimation();return}
  const [railId,direction,fromId,toId]=config;
  const rail=document.getElementById(railId);
  rail.querySelector('.message').textContent=activity.amount;
  rail.classList.remove('forward','reverse');
  void rail.offsetWidth;
  rail.classList.add(direction);
  document.getElementById(fromId).classList.add('active');
  document.getElementById(toId).classList.add('active');
  setTimeout(()=>{rail.classList.remove(direction);document.getElementById(fromId).classList.remove('active');document.getElementById(toId).classList.remove('active');animating=false;runNextAnimation()},600);
}
function displayPid(pid){return pid.startsWith('//erl(')?pid.slice(6,-1):pid}
function render(s){
  if(s.revision < renderedRevision) return;
  renderedRevision = s.revision;
  latestSnapshot = s;
  for(const k of ['pushed','received','buffered']) document.getElementById(k).textContent=fmt.format(s[k]);
  document.getElementById('demand').textContent=fmt.format(s.outstanding_demand);
  document.getElementById('status').textContent=s.active?'actor online':'subscription cancelled';
  document.getElementById('producer-name').textContent=s.producer;
  document.getElementById('consumer-name').textContent=s.consumer;
  document.getElementById('subscription-name').textContent=`subscription: ${s.subscription}`;
  document.getElementById('producer-pid').textContent=displayPid(s.producer_pid);
  document.getElementById('stage-pid').textContent=displayPid(s.stage_pid);
  document.getElementById('consumer-pid').textContent=displayPid(s.consumer_pid);
  document.getElementById('producer-work').textContent=fmt.format(s.pushed);
  document.getElementById('stage-work').textContent=fmt.format(s.buffered);
  document.getElementById('consumer-work').textContent=fmt.format(s.received);
  document.getElementById('event-list').innerHTML=s.last_events.map(x=>`<span class='event'>#${x}</span>`).join('');
  for(const a of s.activities) if(!observedAt.has(a.sequence)) observedAt.set(a.sequence,new Date().toLocaleTimeString());
  document.getElementById('timeline').innerHTML=s.activities.length?s.activities.map(a=>`<div class='activity'><time>${observedAt.get(a.sequence)}</time><span class='kind'>${a.kind}</span><span class='route'><b>${a.source}</b> → ${a.target}</span><span class='amount'>${fmt.format(a.amount)}</span></div>`).join(''):`<div class='empty'>Push events or ask for demand to begin.</div>`;
  const newest=s.activities[0];
  if(lastAnimatedSequence) s.activities.filter(a=>a.sequence>lastAnimatedSequence).reverse().forEach(queueActivity);
  if(newest) lastAnimatedSequence=Math.max(lastAnimatedSequence,newest.sequence);
  if(slowTimer && s.buffered === 0 && s.outstanding_demand === 0) stopSlow();
}
async function refresh(){
  if(refreshing) return;
  refreshing = true;
  try{
    const response = await fetch('/api/state',{cache:'no-store'});
    if(!response.ok) throw new Error('state request failed');
    render(await response.json());
  }catch(e){document.getElementById('status').textContent='disconnected'}
  finally{refreshing=false}
}
async function act(kind,n){
  if(acting) return false;
  acting = true;
  try{
    const response = await fetch(`/api/${kind}/${n}`,{method:'POST',cache:'no-store'});
    if(!response.ok) throw new Error(await response.text());
    render(await response.json());
    setTimeout(refresh,25);
    return true;
  }catch(e){document.getElementById('status').textContent=e.message}
  finally{acting=false}
  return false;
}
function stopSlow(){
  if(slowTimer) clearInterval(slowTimer);
  slowTimer = null;
  const button = document.getElementById('slow-toggle');
  button.classList.remove('running');
  button.textContent='Start slow consumer · 5 / 1200ms';
}
function startSlow(){
  if(slowTimer) return;
  if(!latestSnapshot || latestSnapshot.buffered === 0){document.getElementById('status').textContent='push events first';return}
  const button = document.getElementById('slow-toggle');
  button.classList.add('running');
  button.textContent='Stop slow consumer';
  slowTimer=setInterval(()=>{if(latestSnapshot?.buffered>0 && !acting) act('ask',5)},1200);
  act('ask',5);
}
function toggleSlow(){slowTimer?stopSlow():startSlow()}
async function startDemo(){
  stopSlow();
  const pushed=await act('push',100);
  if(pushed){await refresh();startSlow()}
}
setInterval(refresh,300);refresh();
</script>
</body>
</html>"
