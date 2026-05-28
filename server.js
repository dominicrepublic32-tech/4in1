// ═══════════════════════════════════════════════
// EL ROI UPTREND — 4-in-1 Uptrend Bot
// Mirror of downtrend — finds ascending troughs
// ═══════════════════════════════════════════════
'use strict';

const express   = require('express');
const http      = require('http');
const WebSocket = require('ws');
const fetch     = require('node-fetch');
const path      = require('path');
const fs        = require('fs');
const crypto    = require('crypto');

const app     = express();
const server  = http.createServer(app);
const dashWss = new WebSocket.Server({ server, path: '/dashboard' });

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

const PORT          = process.env.PORT || 3000;
const APP_ID_DEMO   = 1089;
const APP_ID_LIVE   = '33ozSet2PJUWhxpWsjnsA';
const REDIRECT_URI  = 'https://g54in1.onrender.com/callback';

// ── PKCE HELPERS ─────────────────────────────────
function base64url(buf) {
  return buf.toString('base64').replace(/\+/g,'-').replace(/\//g,'_').replace(/=/g,'');
}
function generateCodeVerifier() { return base64url(crypto.randomBytes(32)); }
function generateCodeChallenge(v) { return base64url(crypto.createHash('sha256').update(v).digest()); }

const oauthPending = new Map();

// ── PERSISTENT STORAGE ───────────────────────────
const DATA_FILE = path.join(__dirname, 'data_uptrend.json');
function loadData() {
  try { if(fs.existsSync(DATA_FILE)) return JSON.parse(fs.readFileSync(DATA_FILE,'utf8')); }
  catch(e){ console.log('Load error:',e.message); }
  return { bots:{} };
}
function saveData() {
  try {
    const d={bots:{}};
    bots.forEach(b=>{ d.bots[b.id]={tradeLog:b.tradeLog,cfg:b.cfg}; });
    fs.writeFileSync(DATA_FILE,JSON.stringify(d,null,2));
  } catch(e){ console.log('Save error:',e.message); }
}
const savedData = loadData();

const MKT_NAMES = {
  '1HZ100V':'Volatility 100 (1s)','R_100':'Volatility 100',
  '1HZ75V':'Volatility 75 (1s)','R_75':'Volatility 75',
  '1HZ50V':'Volatility 50 (1s)','R_50':'Volatility 50',
  '1HZ25V':'Volatility 25 (1s)','1HZ10V':'Volatility 10 (1s)',
  'frxEURUSD':'EUR/USD','frxGBPUSD':'GBP/USD','frxXAUUSD':'Gold/USD',
  'cryBTCUSD':'BTC/USD','cryETHUSD':'ETH/USD','stpRNG':'Step Index',
  'BOOM1000':'Boom 1000','BOOM500':'Boom 500','CRASH1000':'Crash 1000','CRASH500':'Crash 500',
};

// ── BOT FACTORY ──────────────────────────────────
function createBot(id) {
  const saved = savedData.bots?.[id] || {};
  return {
    id,
    cfg: {
      accountType:'demo', apiToken:'',
      market:'1HZ100V', command:'NOTOUCH',
      stake:1.00, durationMins:5, barrierOffset:'-2.1',
      multiplier:10, takeProfit:4.00, stopLoss:2.00,
      scanTFs:['M1','M5'], minTFConfirm:2, smallTol:10, bigTol:15,
      smallConfirm:1, bigConfirm:2, proximityPct:90,
      maxTrades:0, maxConsecLosses:2, cooldownSecs:1800,
      teleToken:'', teleChatId:'',
      htfClosePct:20, htfPassPct:30,
      ...(saved.cfg||{}),
    },
    liveAccessToken:null, liveAccountId:null, liveLoggedIn:false,
    derivWs:null, botActive:false, userStarted:false,
    reconnectTimer:null, scanInterval:null,
    currentPrice:0,
    candles:{ M1:[],M5:[],M15:[],M30:[],H1:[],H4:[] },
    trendStatus:{ M1:null,M5:null,M15:null },
    confirmedTrend:false,   // true when 2+ TFs show UPTREND
    activeStructures:[],
    ignoredLevels:new Set(),
    doNotTradeZones:[],
    htfZones:[],            // resistance zones — built from swing highs
    htfZonePaused:false,
    htfPauseReason:'',
    activeHtfZoneId:null,
    autoHtfStructures:[],
    activeTrades:[],        // array of active trades with timers
    inTrade:false,          // only for multiplier
    currentContractId:null,
    entryTargets:[],
    currentActiveLevel:null, currentStructType:null,
    tradeCount:0, wins:0, losses:0, sessionPnl:0,
    tradeLog: saved.tradeLog || [],
    consecutiveLosses:0, lossCountdownPaused:false,
    lossCountdownTimer:null, lossCountdownRemaining:0, lossCountdownTotal:0,
    timeOffPaused:false, timeOffTimer:null, timeOffRemaining:0, timeOffTotal:0,
    tickerMsg:`— BOT ${id} READY —`, statusText:'IDLE',
  };
}

const bots = [createBot(1),createBot(2),createBot(3),createBot(4)];

// ── BROADCAST ─────────────────────────────────────
function broadcast(data) {
  const json=JSON.stringify(data);
  dashWss.clients.forEach(c=>{ if(c.readyState===WebSocket.OPEN) c.send(json); });
}

function broadcastBotState(b) {
  broadcast({
    type:'bot_state', id:b.id,
    botActive:b.botActive, currentPrice:b.currentPrice,
    trendStatus:b.trendStatus, confirmedTrend:b.confirmedTrend,
    activeStructures:b.activeStructures.map(s=>({
      peaks:s.peaks,baseDiff:s.baseDiff,type:s.type,tf:s.tf,
      projectedLevels:s.projectedLevels,tradedLevels:[...s.tradedLevels],id:s.id
    })),
    ignoredLevels:[...b.ignoredLevels],
    doNotTradeZones:b.doNotTradeZones,
    htfZones:b.htfZones,
    htfZonePaused:b.htfZonePaused,
    htfPauseReason:b.htfPauseReason||'',
    activeHtfZoneId:b.activeHtfZoneId||null,
    autoHtfStructures:b.autoHtfStructures||[],
    activeTrades:b.activeTrades,
    currentActiveLevel:b.currentActiveLevel,
    currentStructType:b.currentStructType,
    tradeCount:b.tradeCount,wins:b.wins,losses:b.losses,sessionPnl:b.sessionPnl,
    consecutiveLosses:b.consecutiveLosses,
    lossCountdownPaused:b.lossCountdownPaused,
    lossCountdownRemaining:b.lossCountdownRemaining,
    lossCountdownTotal:b.lossCountdownTotal,
    timeOffPaused:b.timeOffPaused,
    timeOffRemaining:b.timeOffRemaining,
    timeOffTotal:b.timeOffTotal,
    tickerMsg:b.tickerMsg, statusText:b.statusText,
    cfg:b.cfg,
    liveLoggedIn:b.liveLoggedIn,
    liveAccountId:b.liveAccountId,
    tradeLog:b.tradeLog.slice(0,100),
  });
}

function log(b,msg) {
  const t=new Date().toISOString().replace('T',' ').slice(0,19);
  const full=`[${t}][Bot${b.id}] ${msg}`;
  console.log(full);
  broadcast({type:'log',id:b.id,msg:full});
}
function setTicker(b,msg){ b.tickerMsg=msg; broadcast({type:'ticker',id:b.id,msg}); }
function setStatus(b,s,t){ b.statusText=t; broadcast({type:'status',id:b.id,status:s,text:t}); }

// ── TELEGRAM ──────────────────────────────────────
async function telegram(b,msg) {
  if(!b.cfg.teleToken||!b.cfg.teleChatId) return;
  try {
    await fetch(`https://api.telegram.org/bot${b.cfg.teleToken}/sendMessage`,{
      method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({chat_id:b.cfg.teleChatId,text:`📈 EL ROI UPTREND [Bot${b.id}]\n${msg}`,parse_mode:'HTML'})
    });
  } catch(e){ log(b,'Telegram error: '+e.message); }
}

// ── TREND (UPTREND BOT) ───────────────────────────
// Confirms UPTREND: higher highs + higher lows = uptrend
function analyzeTrend(b,tf) {
  const data=b.candles[tf];
  if(!data||data.length<10) return;
  const recent=data.slice(-20);
  const highs=recent.map(c=>c.high), lows=recent.map(c=>c.low);
  let lh=0,ll=0,hh=0,hl=0;
  for(let i=1;i<highs.length;i++){
    if(highs[i]<highs[i-1])lh++;else hh++;
    if(lows[i]<lows[i-1])ll++;else hl++;
  }
  const total=highs.length-1;
  const ds=(lh+ll)/(total*2), us=(hh+hl)/(total*2);
  b.trendStatus[tf]=us>=0.6?'up':ds>=0.6?'down':'neutral';
  // confirmedTrend = 2+ TFs showing UPTREND (opposite of downtrend bot)
  const uc=Object.values(b.trendStatus).filter(t=>t==='up').length;
  b.confirmedTrend=uc>=b.cfg.minTFConfirm;
  broadcast({type:'trend',id:b.id,trendStatus:b.trendStatus,confirmedTrend:b.confirmedTrend});
}

// ── UPTREND STRUCTURE DETECTION ───────────────────
// Mirror of downtrend: finds ascending troughs (higher lows)
// instead of descending peaks (lower highs)
function findStructuresInDataUptrend(b,data) {
  if(data.length<10) return {smallStruct:null,bigStruct:null};
  const LR=2, troughs=[];

  // Find swing lows (troughs) — opposite of finding peaks
  for(let i=LR;i<data.length-LR;i++){
    let bottom=true;
    for(let j=i-LR;j<=i+LR;j++){
      if(j!==i&&data[j].low<=data[i].low){bottom=false;break;}
    }
    if(bottom) troughs.push({price:data[i].low, index:i});
  }
  if(troughs.length<2) return {smallStruct:null,bigStruct:null};

  function findBestGroup(minSpan,maxSpan){
    let best=null;
    for(let s=0;s<troughs.length-1;s++){
      const sp0=troughs[s+1].index-troughs[s].index;
      if(sp0<minSpan||sp0>maxSpan) continue;
      // Ascending: each trough must be HIGHER than the previous (opposite of downtrend)
      if(troughs[s+1].price<=troughs[s].price) continue;
      const bd=troughs[s+1].price-troughs[s].price; // baseDiff is positive going up
      if(bd<=0) continue;
      const grp=[troughs[s],troughs[s+1]];
      for(let j=s+2;j<troughs.length;j++){
        const prev=grp[grp.length-1];
        const sp=troughs[j].index-prev.index;
        if(sp<minSpan||sp>maxSpan) continue;
        if(troughs[j].price<=prev.price) continue; // must keep going up
        const diff=troughs[j].price-prev.price;
        if(Math.abs(diff-bd)/bd<=0.10) grp.push(troughs[j]);
      }
      if(grp.length>=2){
        const tol=maxSpan===5?b.cfg.smallTol:b.cfg.bigTol;
        const cs=data.length-1-grp[grp.length-1].index;
        if(cs>tol) continue;
        const lp=grp[grp.length-1].price;
        // Check structure not broken: no candle closed BELOW last trough (for uptrend)
        let broken=false;
        for(let k=grp[grp.length-1].index+1;k<data.length;k++){
          if(Math.min(data[k].open,data[k].close)<lp-0.05){broken=true;break;}
        }
        if(broken) continue;
        if(!best||grp.length>best.peaks.length) best={peaks:grp,baseDiff:bd};
      }
    }
    return best;
  }
  return {smallStruct:findBestGroup(2,5),bigStruct:findBestGroup(5,15)};
}

// ── ZONE HELPERS ─────────────────────────────────
function isLevelInDoNotTradeZone(b,level) {
  return b.doNotTradeZones.some(z=>{
    const lo=Math.min(z.a,z.b),hi=Math.max(z.a,z.b);
    return level>=lo&&level<=hi;
  });
}

// HTF Zone — for uptrend bot: resistance zones built from swing HIGHS
// Price in zone = price is inside the resistance area
function isPriceInHTFZone(b,price) {
  return b.htfZones.find(z=>{
    if(z.cancelled) return false;
    const hi=Math.max(z.a,z.b), lo=Math.min(z.a,z.b);
    return price<=hi && price>=lo;
  });
}

// ── AUTO HTF DETECTION (UPTREND) ─────────────────
// Finds swing HIGHS (resistance) on M30/H1/H4
// Zone A = top of zone (where price first enters from below)
// Zone B = bottom of zone (price must break above A to confirm resistance break)
// Mirror of downtrend HTF which uses swing lows
function detectAutoHTFZones(b) {
  const HTF_TFS  = ['M30','H1','H4'];
  const closePct = (b.cfg.htfClosePct||20) / 100;
  const passPct  = (b.cfg.htfPassPct||30)  / 100;
  const ALMOST_EQUAL_TOL = 0.005;
  const LR = 3;
  const RECENCY = { M30:48, H1:48, H4:30 };

  const autoZones=[], autoStructures=[];

  for(const tf of HTF_TFS){
    const data=b.candles[tf];
    if(!data||data.length<LR*2+2) continue;
    const lookback=RECENCY[tf]||50;
    const recent=data.slice(-lookback);

    // Find swing HIGHS (peaks) — opposite of downtrend which finds troughs
    const peaks=[];
    for(let i=LR;i<recent.length-LR;i++){
      let isPeak=true;
      for(let j=i-LR;j<=i+LR;j++){
        if(j!==i&&recent[j].high>=recent[i].high){isPeak=false;break;}
      }
      if(isPeak) peaks.push({
        high:  recent[i].high,
        low:   recent[i].low,
        range: recent[i].high - recent[i].low,
        idx:   i,
      });
    }
    if(peaks.length<1) continue;

    // Most recent peak must not be too old
    const sh1Recency=recent.length-1-peaks[peaks.length-1].idx;
    if(sh1Recency>15) continue;

    const sh1=peaks[peaks.length-1]; // most recent swing high

    // ── PATTERN 1 & 2 — single swing high, price may approach ────────────
    // Zone A = sh1.high + (closePct × range) ← price enters from below here
    // Zone B = sh1.high - (passPct × range)  ← bottom of resistance zone
    const range12=sh1.range>0?sh1.range:sh1.high*0.002;
    const zA_12=parseFloat((sh1.high+closePct*range12).toFixed(2));
    const zB_12=parseFloat((sh1.high-passPct*range12).toFixed(2));
    const id12=`auto_12_${tf}_${sh1.high.toFixed(4)}`;
    const existCancel12=b.htfZones.find(z=>z.id===id12&&z.cancelled);
    // Zone broken = price already closed ABOVE zone A (resistance broken)
    const zone12Broken=recent.some(c=>c.close>zA_12);
    if(!existCancel12&&!zone12Broken){
      autoZones.push({a:zA_12,b:zB_12,source:'auto',id:id12,
        label:`${tf} SH1+2 (${sh1.high.toFixed(2)})`,cancelled:false});
      autoStructures.push({tf,type:'12',sh1:sh1.high,sh1low:sh1.low,
        zoneA:zA_12,zoneB:zB_12,id:id12});
    }

    if(peaks.length<2) continue;

    // ── STRUCTURES FROM PAIRS OF SWING HIGHS ──────────────────────────────
    for(let t=peaks.length-1;t>=1;t--){
      const shA=peaks[t];   // more recent
      const shB=peaks[t-1]; // older

      const diff=shA.high-shB.high; // positive = ascending highs
      const absDiff=Math.abs(diff);
      const avgHigh=(shA.high+shB.high)/2;
      const isAlmostEqual=absDiff/avgHigh<ALMOST_EQUAL_TOL;

      let zoneA,zoneB,structType,nextLevel;

      if(isAlmostEqual){
        // Equal/almost equal — zone around sh1 using its own range
        const r=shA.range>0?shA.range:shA.high*0.002;
        nextLevel=shA.high;
        zoneA=parseFloat((nextLevel+closePct*r).toFixed(2));
        zoneB=parseFloat((nextLevel-passPct*r).toFixed(2));
        structType='equal';
      } else if(diff>0){
        // Ascending highs — NEXT resistance expected higher
        const baseDiff=diff;
        nextLevel=parseFloat((shA.high+baseDiff).toFixed(2));
        zoneA=parseFloat((nextLevel+closePct*baseDiff).toFixed(2));
        zoneB=parseFloat((nextLevel-passPct*baseDiff).toFixed(2));
        structType='ascending';
      } else {
        // Descending highs — NEXT resistance expected lower
        const baseDiff=absDiff;
        nextLevel=parseFloat((shA.high-baseDiff).toFixed(2));
        zoneA=parseFloat((nextLevel+closePct*baseDiff).toFixed(2));
        zoneB=parseFloat((nextLevel-passPct*baseDiff).toFixed(2));
        structType='descending';
      }

      const idFull=`auto_${structType}_${tf}_${shA.high.toFixed(4)}_${shB.high.toFixed(4)}`;
      const existCancel=b.htfZones.find(z=>z.id===idFull&&z.cancelled);
      // Zone broken = price closed above zone A
      const zoneBroken=recent.some(c=>c.close>zoneA);
      if(!existCancel&&!zoneBroken){
        autoZones.push({a:zoneA,b:zoneB,source:'auto',id:idFull,
          label:`${tf} ${structType} NEXT:${nextLevel.toFixed(2)}`,cancelled:false});
        autoStructures.push({tf,type:structType,
          sh1:shA.high,sh2:shB.high,next:nextLevel,
          zoneA,zoneB,id:idFull});
      }
    }
  }

  const keepZones=b.htfZones.filter(z=>z.source==='manual'||z.cancelled);
  b.htfZones=[...autoZones,...keepZones];
  b.autoHtfStructures=autoStructures;
}

// ── FIND LEVELS (UPTREND) ─────────────────────────
function findLevels(b) {
  const tfs=Array.isArray(b.cfg.scanTFs)&&b.cfg.scanTFs.length>0?b.cfg.scanTFs:['M1','M5'];
  const newStructures=[];

  for(const tf of tfs){
    const data=b.candles[tf];
    if(!data||data.length<10) continue;
    const result=findStructuresInDataUptrend(b,data);

    if(result.smallStruct){
      const existing=b.activeStructures.find(s=>s.type==='small'&&s.tf===tf&&Math.abs(s.peaks[0].price-result.smallStruct.peaks[0].price)<0.05);
      const tradedLevels=existing?existing.tradedLevels:new Set();
      newStructures.push({
        ...result.smallStruct,type:'small',tf,tradedLevels,
        projectedLevels:computeProjectedLevels(b,result.smallStruct,tradedLevels),
        id:`small_${tf}_${result.smallStruct.peaks[0].price.toFixed(2)}`
      });
    }
    if(result.bigStruct){
      const existing=b.activeStructures.find(s=>s.type==='big'&&s.tf===tf&&Math.abs(s.peaks[0].price-result.bigStruct.peaks[0].price)<0.05);
      const tradedLevels=existing?existing.tradedLevels:new Set();
      newStructures.push({
        ...result.bigStruct,type:'big',tf,tradedLevels,
        projectedLevels:computeProjectedLevels(b,result.bigStruct,tradedLevels),
        id:`big_${tf}_${result.bigStruct.peaks[0].price.toFixed(2)}`
      });
    }
  }

  b.activeStructures=newStructures;

  if(b.activeStructures.length>0){
    setTicker(b,`📐 ${b.activeStructures.length} struct(s) | ${b.activeStructures.map(s=>`${s.type}(${s.tf})`).join(', ')}`);
  } else {
    setTicker(b,'⏳ Scanning for structures...');
  }

  // Telegram alert when uptrend confirmed and new level found
  const upCount=Object.values(b.trendStatus).filter(t=>t==='up').length;
  if(upCount>=2){
    b.activeStructures.forEach(s=>{
      if(s.projectedLevels&&s.projectedLevels.length>0){
        const np=s.projectedLevels[0];
        if(!s._lastTeleLevel||Math.abs(s._lastTeleLevel-np)>0.01){
          s._lastTeleLevel=np;
          const r1=s.peaks.length>=2?s.peaks[s.peaks.length-2].price:null;
          const r2=s.peaks[s.peaks.length-1].price;
          const diff=r1?Math.abs(r1-r2).toFixed(2):s.baseDiff.toFixed(2);
          const mkt=MKT_NAMES[b.cfg.market]||b.cfg.market;
          telegram(b,`🎯 <b>NEXT LEVEL ACTIVE</b>\nLevel: <b>${np.toFixed(2)}</b>\n${r1?`SL1: ${r1.toFixed(2)} | SL2: ${r2.toFixed(2)}\n`:''}Diff: ${diff}\nMarket: ${mkt}\nCommand: ${b.cfg.command}\nStruct: ${s.type.toUpperCase()} (${s.tf})`);
        }
      }
    });
  }

  broadcastBotState(b);
}

// ── PROJECTED LEVELS (UPTREND) ────────────────────
// Projects UPWARD — adds baseDiff to last trough (opposite of downtrend)
function computeProjectedLevels(b,struct,tradedLevels) {
  if(!struct||!struct.peaks||struct.peaks.length<1) return [];
  const lastLevel=struct.peaks[struct.peaks.length-1].price;
  // Add baseDiff (going UP) — mirror of downtrend which subtracts
  let np=parseFloat((lastLevel+struct.baseDiff).toFixed(2));
  let safety=0;
  while(safety<20){
    if(
      !tradedLevels.has(np.toFixed(2))&&
      !isLevelInDoNotTradeZone(b,np)&&
      !b.ignoredLevels.has(np.toFixed(2))
    ){
      return [np];
    }
    np=parseFloat((np+struct.baseDiff).toFixed(2));
    safety++;
  }
  return [];
}

// ── HTF ZONE DOWNTREND DETECTION (UPTREND BOT) ───
// Mirror: pauses on DOWNTREND structure forming inside resistance zone
function checkHTFZoneDowntrend(b) {
  detectAutoHTFZones(b);
  const activeZones=b.htfZones.filter(z=>!z.cancelled);
  if(!b.currentPrice) return;

  // ── RESUME CHECKS ────────────────────────────────
  if(b.htfZonePaused){
    const triggerZone=b.activeHtfZoneId?activeZones.find(z=>z.id===b.activeHtfZoneId):null;

    // Resume 1: price broke ABOVE zone A (resistance broken — uptrend continues)
    if(triggerZone){
      const zoneA=Math.max(triggerZone.a,triggerZone.b);
      if(b.currentPrice>zoneA){
        log(b,'✅ Price broke above HTF zone A — resistance broken, resuming');
        b.htfZones=b.htfZones.filter(z=>z.id!==triggerZone.id);
        b.htfZonePaused=false; b.htfPauseReason=''; b.activeHtfZoneId=null;
        setStatus(b,'running','RUNNING');
        setTicker(b,'✅ HTF resistance broken — resuming...');
        broadcastBotState(b);
        return;
      }
    }

    // Resume 2: at least 2 of M1,M5,M15 turned DOWNTREND (reversal confirmed — no longer safe to pause)
    ['M1','M5','M15'].forEach(tf=>{ if(b.candles[tf]&&b.candles[tf].length>=10) analyzeTrend(b,tf); });
    const dnCount=['M1','M5','M15'].filter(tf=>b.trendStatus[tf]==='down').length;
    if(dnCount>=2){
      log(b,`✅ ${dnCount}/3 TFs downtrend — HTF resistance resolved, resuming`);
      if(b.activeHtfZoneId) b.htfZones=b.htfZones.filter(z=>z.id!==b.activeHtfZoneId);
      b.htfZonePaused=false; b.htfPauseReason=''; b.activeHtfZoneId=null;
      setStatus(b,'running','RUNNING');
      setTicker(b,'✅ 2+ TFs downtrend — HTF resolved, resuming...');
      broadcastBotState(b);
      return;
    }
    return;
  }

  if(!activeZones.length) return;
  const nearZone=isPriceInHTFZone(b,b.currentPrice);
  if(!nearZone) return;

  // ── DOWNTREND STRUCTURE DETECTION on M1,M5,M15 ──
  // Mirror of uptrend detection: looks for lower highs + break below swing low
  const TFS_TO_CHECK=['M1','M5','M15'];
  let downtrendDetected=false, pauseReason='';

  for(const tf of TFS_TO_CHECK){
    const data=b.candles[tf];
    if(!data||data.length<20) continue;
    const recent=data.slice(-40);

    const sLows=[], sHighs=[];
    for(let i=2;i<recent.length-2;i++){
      if(recent[i].low<recent[i-1].low&&recent[i].low<recent[i-2].low&&
         recent[i].low<recent[i+1].low&&recent[i].low<recent[i+2].low)
        sLows.push({price:recent[i].low,idx:i});
      if(recent[i].high>recent[i-1].high&&recent[i].high>recent[i-2].high&&
         recent[i].high>recent[i+1].high&&recent[i].high>recent[i+2].high)
        sHighs.push({price:recent[i].high,low:recent[i].low,idx:i});
    }

    if(sHighs.length>=2&&sLows.length>=1){
      const lastHigh=sHighs[sHighs.length-1];
      const prevHigh=sHighs[sHighs.length-2];
      const lastLow=sLows[sLows.length-1];

      // Condition 1: Lower High + Break Below Swing Low (mirror of higher low + break above)
      if(lastHigh.price<prevHigh.price&&lastHigh.idx>prevHigh.idx){
        if(b.currentPrice<lastLow.price&&lastLow.idx>prevHigh.idx){
          downtrendDetected=true;
          pauseReason=`Lower high + break below swing low on ${tf}`;
          break;
        }
      }
      // Condition 2: Higher High + price breaks below that candle's low
      if(lastHigh.price>prevHigh.price&&lastHigh.idx>prevHigh.idx){
        if(b.currentPrice<lastHigh.low){
          downtrendDetected=true;
          pauseReason=`Higher high + break below its low on ${tf}`;
          break;
        }
      }
    }
  }

  if(downtrendDetected){
    log(b,`⚠ HTF resistance pause: ${pauseReason} in zone ${nearZone.id}`);
    b.htfZonePaused=true; b.htfPauseReason=pauseReason; b.activeHtfZoneId=nearZone.id;
    nearZone.active=true;
    setStatus(b,'scanning','PAUSED — HTF ZONE');
    const lo=Math.min(nearZone.a,nearZone.b),hi=Math.max(nearZone.a,nearZone.b);
    setTicker(b,`⚠ HTF resistance ${lo.toFixed(2)}–${hi.toFixed(2)} — ${pauseReason}`);
    telegram(b,`⚠ <b>Bot paused — HTF Resistance</b>\n${pauseReason}\nZone: ${lo.toFixed(2)}–${hi.toFixed(2)}\nResumes: price above zone A OR 2+ TFs downtrend`);
    broadcastBotState(b);
  }
}

// ── ENTRY CHECK (UPTREND) ─────────────────────────
function checkEntry(b) {
  if(!b.botActive||!b.confirmedTrend) return;
  const isMulti=b.cfg.command==='CALL_MULT'||b.cfg.command==='PUT_MULT';
  if(isMulti&&b.inTrade) return;
  if(b.lossCountdownPaused||b.timeOffPaused||b.htfZonePaused) return;
  if(!b.activeStructures.length) return;
  if(b.cfg.maxTrades>0&&b.tradeCount>=b.cfg.maxTrades){stopBot(b);return;}

  // Use M1 candles for entry confirmation (pullback detection)
  const data=b.candles['M1'];
  if(!data||data.length<3) return;

  for(const struct of b.activeStructures){
    if(!struct.projectedLevels||!struct.projectedLevels.length) continue;
    const target=struct.projectedLevels[0];
    if(struct.tradedLevels.has(target.toFixed(2))) continue;
    if(isLevelInDoNotTradeZone(b,target)) continue;
    if(b.ignoredLevels.has(target.toFixed(2))) continue;

    const pct=b.cfg.proximityPct/100;
    const bd=struct.baseDiff||5;
    const maxGap=bd*(1-pct);
    const confirmCount=struct.type==='small'?b.cfg.smallConfirm:b.cfg.bigConfirm;

    let et=b.entryTargets.find(e=>e.structId===struct.id&&Math.abs(e.level-target)<0.01);
    if(!et){ et={structId:struct.id,level:target,pricePassed:false,passedCount:0}; b.entryTargets.push(et); }

    if(!et.pricePassed){
      // UPTREND: count candles ABOVE target (price went past level going up)
      let count=0;
      for(let i=data.length-1;i>=Math.max(0,data.length-40);i--){
        if(Math.min(data[i].open,data[i].close)>target) count++;
        else break;
      }
      if(count>=confirmCount){
        et.pricePassed=true; et.passedCount=count;
        setTicker(b,`✅ ${count} candles above ${target.toFixed(2)} [${struct.type}/${struct.tf}] — waiting pullback...`);
      } else { continue; }
    }

    // UPTREND: price must be BELOW target now (pulling back DOWN toward level)
    if(b.currentPrice<=target){ et.pricePassed=false; et.passedCount=0; continue; }
    // Too far above = not close enough to level
    if(b.currentPrice>target+maxGap) continue;

    // Entry candle: BEARISH (close <= prev.close) — pullback candle coming down to level
    const last=data[data.length-1],prev=data[data.length-2];
    if(last.close>=prev.close) continue;

    setTicker(b,`⚡ ENTRY! ${b.currentPrice.toFixed(2)} at ${target.toFixed(2)} [${struct.type}/${struct.tf}]`);
    struct.tradedLevels.add(target.toFixed(2));
    struct.projectedLevels=computeProjectedLevels(b,struct,struct.tradedLevels);
    b.currentActiveLevel=target; b.currentStructType=struct.type;
    b.entryTargets=b.entryTargets.filter(e=>!(e.structId===struct.id&&Math.abs(e.level-target)<0.01));
    placeTrade(b);
    return;
  }
}

// ── ACTIVE TRADE TIMER ────────────────────────────
function startTradeTimer(b,contractId,durationSecs) {
  const trade={
    contractId, startTime:Date.now(),
    durationSecs, remainingSecs:durationSecs,
    level:b.currentActiveLevel, struct:b.currentStructType,
    timerId:null,
  };
  trade.timerId=setInterval(()=>{
    trade.remainingSecs--;
    broadcast({type:'trade_timer',id:b.id,contractId:trade.contractId,remaining:trade.remainingSecs,total:trade.durationSecs});
    if(trade.remainingSecs<=0) stopTradeTimer(b,contractId);
  },1000);
  b.activeTrades.push(trade);
  broadcast({type:'trade_started',id:b.id,contractId,durationSecs,level:trade.level});
}

function stopTradeTimer(b,contractId) {
  const idx=b.activeTrades.findIndex(t=>t.contractId===contractId);
  if(idx>=0){
    clearInterval(b.activeTrades[idx].timerId);
    b.activeTrades.splice(idx,1);
    broadcast({type:'trade_ended',id:b.id,contractId});
  }
}

// ── PLACE TRADE ───────────────────────────────────
function placeTrade(b) {
  if(!b.derivWs||b.derivWs.readyState!==WebSocket.OPEN){
    log(b,'❌ placeTrade: WebSocket not open'); return;
  }
  const isMulti=b.cfg.command==='CALL_MULT'||b.cfg.command==='PUT_MULT';
  if(isMulti) b.inTrade=true;
  const duration=b.cfg.durationMins*60;
  const type={NOTOUCH:'NOTOUCH',TOUCH:'ONETOUCH',HIGHER:'CALL',LOWER:'PUT',RISE:'CALL',FALL:'PUT',CALL_MULT:'MULTUP',PUT_MULT:'MULTDOWN'}[b.cfg.command]||'NOTOUCH';
  const isLive=b.cfg.accountType==='live';
  const params={contract_type:type,basis:'stake',amount:b.cfg.stake,currency:'USD'};
  if(isLive) params.underlying_symbol=b.cfg.market;
  else params.symbol=b.cfg.market;
  if(isMulti){ params.multiplier=b.cfg.multiplier; }
  else {
    params.duration=duration; params.duration_unit='s';
    if(['NOTOUCH','TOUCH','HIGHER','LOWER'].includes(b.cfg.command)) params.barrier=b.cfg.barrierOffset;
  }
  const proposalMsg={proposal:1,subscribe:1,...params};
  log(b,`📤 Proposal: ${type} ${b.cfg.market} $${b.cfg.stake} dur:${duration}s`);
  b.derivWs.send(JSON.stringify(proposalMsg));
  broadcastBotState(b);
}

function handleProposal(b,d){
  if(d.error){
    log(b,`❌ Proposal error [${d.error.code}]: ${d.error.message}`);
    b.inTrade=false; broadcastBotState(b); return;
  }
  const proposal=d.proposal;
  if(!proposal||!proposal.id){
    log(b,'❌ Proposal: no id — '+JSON.stringify(d));
    b.inTrade=false; broadcastBotState(b); return;
  }
  log(b,`📋 Proposal: ${proposal.id} | payout: ${proposal.payout}`);
  b.derivWs.send(JSON.stringify({buy:proposal.id,price:b.cfg.stake}));
}

// ── LOSS CONTROL ──────────────────────────────────
function startLossCountdown(b,totalSecs) {
  stopLossCountdown(b);
  b.lossCountdownPaused=true; b.lossCountdownRemaining=totalSecs; b.lossCountdownTotal=totalSecs;
  log(b,`⏸ Cooldown: ${totalSecs===1800?'30 MIN':totalSecs===3600?'1 HR':'4 HR'}`);
  setStatus(b,'scanning','PAUSED — COOLDOWN');
  b.lossCountdownTimer=setInterval(()=>{
    b.lossCountdownRemaining--;
    broadcast({type:'loss_countdown',id:b.id,remaining:b.lossCountdownRemaining,total:b.lossCountdownTotal});
    if(b.lossCountdownRemaining<=0) resumeAfterCooldown(b);
  },1000);
}
function stopLossCountdown(b){ if(b.lossCountdownTimer){clearInterval(b.lossCountdownTimer);b.lossCountdownTimer=null;} }
function resumeAfterCooldown(b){
  b.lossCountdownPaused=false; stopLossCountdown(b);
  log(b,'✅ Cooldown done'); setStatus(b,'running','RUNNING');
  setTicker(b,'✅ Cooldown done — scanning...'); broadcastBotState(b);
  if(b.botActive) findLevels(b);
}

function startTimeOff(b,totalSecs) {
  stopTimeOff(b);
  b.timeOffPaused=true; b.timeOffRemaining=totalSecs; b.timeOffTotal=totalSecs;
  log(b,`⏰ Time off: ${totalSecs===1200?'20 MIN':totalSecs===1800?'30 MIN':'1 HR'}`);
  setStatus(b,'scanning','TIME OFF');
  b.timeOffTimer=setInterval(()=>{
    b.timeOffRemaining--;
    broadcast({type:'time_off',id:b.id,remaining:b.timeOffRemaining,total:b.timeOffTotal});
    if(b.timeOffRemaining<=0) resumeAfterTimeOff(b);
  },1000);
}
function stopTimeOff(b){ if(b.timeOffTimer){clearInterval(b.timeOffTimer);b.timeOffTimer=null;} }
function resumeAfterTimeOff(b){
  b.timeOffPaused=false; stopTimeOff(b);
  log(b,'✅ Time off done'); setStatus(b,'running','RUNNING');
  setTicker(b,'✅ Time off done — scanning...'); broadcastBotState(b);
  if(b.botActive) findLevels(b);
}

// ── RESULT ────────────────────────────────────────
function finalizeResult(b,profit,contractId) {
  b.inTrade=false;
  // Stop the timer for this specific contract immediately
  if(contractId) stopTradeTimer(b,contractId);
  b.tradeCount++; b.sessionPnl+=profit;
  const won=profit>0;
  if(won) b.wins++; else b.losses++;
  const wr=Math.round((b.wins/b.tradeCount)*100);

  const card={
    id:Date.now(),tradeNum:b.tradeCount,
    time:new Date().toLocaleTimeString(),date:new Date().toLocaleDateString(),
    timestamp:Date.now(),won,profit,
    level:b.currentActiveLevel?.toFixed(2),struct:b.currentStructType,
    command:b.cfg.command,market:b.cfg.market,stake:b.cfg.stake,wr,
  };
  b.tradeLog.unshift(card);
  if(b.tradeLog.length>500) b.tradeLog.pop();
  saveData();

  log(b,`${won?'✅ WIN':'❌ LOSS'} #${b.tradeCount} | ${profit>=0?'+':''}$${profit.toFixed(2)} | WR:${wr}%`);
  const mkt=MKT_NAMES[b.cfg.market]||b.cfg.market;
  telegram(b,`${won?'✅ WIN':'❌ LOSS'}\nLevel: <b>${b.currentActiveLevel?.toFixed(2)}</b>\nProfit: <b>${profit>=0?'+':''}$${profit.toFixed(2)}</b>\nMarket: ${mkt}\nCommand: ${b.cfg.command}\nWR: ${wr}%`);

  b.currentContractId=null;
  broadcast({type:'trade',id:b.id,card});
  broadcastBotState(b);

  if(won){
    b.consecutiveLosses=0;
    setTicker(b,`✅ WIN +$${profit.toFixed(2)} — scanning...`);
    setTimeout(()=>{if(b.botActive)findLevels(b);},1000);
  } else {
    b.consecutiveLosses++;
    if(b.consecutiveLosses>=b.cfg.maxConsecLosses){
      b.botActive=false; stopLossCountdown(b); stopScanner(b);
      setStatus(b,'stopped',`STOPPED — ${b.cfg.maxConsecLosses} LOSSES`);
      setTicker(b,`🛑 ${b.cfg.maxConsecLosses} losses — restart manually`);
      log(b,`🛑 Stopped after ${b.cfg.maxConsecLosses} consecutive losses`);
      broadcastBotState(b);
    } else {
      // IMMEDIATE cooldown — starts right when loss confirmed, not after duration
      setTicker(b,'❌ LOSS — cooldown starting immediately...');
      startLossCountdown(b,b.cfg.cooldownSecs);
    }
  }
}

// ── OAUTH 2.0 PKCE ────────────────────────────────
function buildOAuthUrl(botId) {
  const verifier=generateCodeVerifier();
  const challenge=generateCodeChallenge(verifier);
  const state=base64url(crypto.randomBytes(16));
  oauthPending.set(state,{botId,verifier});
  setTimeout(()=>oauthPending.delete(state),10*60*1000);
  const params=new URLSearchParams({
    response_type:'code',client_id:APP_ID_LIVE,redirect_uri:REDIRECT_URI,
    scope:'trade',state,code_challenge:challenge,code_challenge_method:'S256',
  });
  return `https://auth.deriv.com/oauth2/auth?${params.toString()}`;
}

app.get('/callback',async(req,res)=>{
  const {code,state,error}=req.query;
  if(error) return res.send(`<script>window.close();</script><p>Login failed: ${error}</p>`);
  if(!code||!state) return res.send('<script>window.close();</script><p>Missing code or state.</p>');
  const pending=oauthPending.get(state);
  if(!pending) return res.send('<script>window.close();</script><p>Session expired.</p>');
  oauthPending.delete(state);
  const {botId,verifier}=pending;
  const b=bots.find(x=>x.id===botId);
  if(!b) return res.send('<script>window.close();</script><p>Bot not found.</p>');
  res.send(`<!DOCTYPE html><html><head><title>EL ROI UPTREND — Connecting Bot ${botId}</title>
  <style>body{background:#040a03;color:#00ff9d;font-family:monospace;display:flex;align-items:center;justify-content:center;height:100vh;flex-direction:column;gap:16px;}
  .spin{width:40px;height:40px;border:3px solid #152840;border-top-color:#00ff9d;border-radius:50%;animation:spin 0.8s linear infinite;}
  @keyframes spin{to{transform:rotate(360deg);}}</style></head>
  <body><div class="spin"></div><p>Connecting Bot ${botId} to Deriv...</p>
  <script>setTimeout(()=>window.close(),8000);</script></body></html>`);
  try {
    broadcast({type:'live_login_status',id:botId,status:'exchanging',msg:'Exchanging auth code...'});
    const tokenRes=await fetch('https://auth.deriv.com/oauth2/token',{
      method:'POST',headers:{'Content-Type':'application/x-www-form-urlencoded'},
      body:new URLSearchParams({grant_type:'authorization_code',client_id:APP_ID_LIVE,
        code,code_verifier:verifier,redirect_uri:REDIRECT_URI}).toString(),
    });
    const tokenData=await tokenRes.json();
    if(!tokenRes.ok||!tokenData.access_token) throw new Error(tokenData.error_description||'Token exchange failed');
    const accessToken=tokenData.access_token;
    broadcast({type:'live_login_status',id:botId,status:'got_token',msg:'Access token obtained...'});
    let accountsRes=await fetch('https://api.derivws.com/trading/v1/options/accounts',{
      headers:{'Authorization':`Bearer ${accessToken}`,'Deriv-App-ID':APP_ID_LIVE},
    });
    let accountsData=await accountsRes.json();
    let accountId=null;
    if(accountsData.data&&accountsData.data.length>0){
      accountId=accountsData.data[0].account_id;
    } else {
      const createRes=await fetch('https://api.derivws.com/trading/v1/options/accounts',{
        method:'POST',headers:{'Authorization':`Bearer ${accessToken}`,'Deriv-App-ID':APP_ID_LIVE,'Content-Type':'application/json'},
        body:JSON.stringify({currency:'USD',group:'row',account_type:'real'}),
      });
      const createData=await createRes.json();
      if(!createData.data||!createData.data[0]) throw new Error('Could not find/create account');
      accountId=createData.data[0].account_id;
    }
    const otpRes=await fetch(`https://api.derivws.com/trading/v1/options/accounts/${accountId}/otp`,{
      method:'POST',headers:{'Authorization':`Bearer ${accessToken}`,'Deriv-App-ID':APP_ID_LIVE},
    });
    const otpData=await otpRes.json();
    if(!otpData.data||!otpData.data.url) throw new Error('OTP URL not returned');
    b.liveAccessToken=accessToken; b.liveAccountId=accountId; b.liveLoggedIn=true;
    broadcast({type:'live_login_status',id:botId,status:'ready',msg:`✅ Bot ${botId} logged in — account ${accountId}`});
    broadcastBotState(b);
    connectDerivLive(b,otpData.data.url);
  } catch(err){
    log(b,`❌ Live login error: ${err.message}`);
    b.liveLoggedIn=false;
    broadcast({type:'live_login_status',id:botId,status:'error',msg:`❌ ${err.message}`});
    broadcastBotState(b);
  }
});

// ── DERIV LIVE CONNECTION ─────────────────────────
function connectDerivLive(b,wssUrl){
  if(b.derivWs){try{b.derivWs.terminate();}catch(e){}}
  log(b,'🔌 Opening live WebSocket...');
  setStatus(b,'connecting','CONNECTING');
  b.derivWs=new WebSocket(wssUrl);
  b.derivWs.on('open',()=>{
    log(b,'✅ Live WebSocket open');
    b.botActive=true; setStatus(b,'running','RUNNING');
    b.derivWs.send(JSON.stringify({ticks:b.cfg.market,subscribe:1}));
    ['M1','M5','M15','M30','H1','H4'].forEach(tf=>fetchCandles(b,tf));
    startScanner(b); broadcastBotState(b);
  });
  b.derivWs.on('message',(raw)=>{ let d;try{d=JSON.parse(raw);}catch(e){return;} handleDerivMessage(b,d); });
  b.derivWs.on('close',()=>{
    log(b,'Disconnected'); b.botActive=false; stopScanner(b);
    setStatus(b,'stopped','DISCONNECTED'); broadcastBotState(b);
    if(b.userStarted&&b.liveLoggedIn&&b.liveAccessToken&&b.liveAccountId){
      if(b.reconnectTimer) clearTimeout(b.reconnectTimer);
      b.reconnectTimer=setTimeout(()=>refreshLiveOTP(b),5000);
    }
  });
  b.derivWs.on('error',(e)=>log(b,'WS error: '+e.message));
}

async function refreshLiveOTP(b){
  log(b,'🔄 Refreshing live OTP...');
  try {
    const otpRes=await fetch(`https://api.derivws.com/trading/v1/options/accounts/${b.liveAccountId}/otp`,{
      method:'POST',headers:{'Authorization':`Bearer ${b.liveAccessToken}`,'Deriv-App-ID':APP_ID_LIVE},
    });
    const otpData=await otpRes.json();
    if(!otpData.data||!otpData.data.url) throw new Error('OTP refresh failed');
    connectDerivLive(b,otpData.data.url);
  } catch(err){
    log(b,`❌ OTP refresh failed: ${err.message}`);
    b.liveLoggedIn=false; b.liveAccessToken=null; b.liveAccountId=null; b.userStarted=false;
    setStatus(b,'stopped','SESSION EXPIRED');
    broadcast({type:'live_login_status',id:b.id,status:'expired',msg:'⚠ Session expired — please login again'});
    broadcastBotState(b);
  }
}

// ── DERIV DEMO CONNECTION ─────────────────────────
function connectDerivDemo(b){
  if(b.derivWs){try{b.derivWs.terminate();}catch(e){}}
  log(b,'🔌 Connecting to Deriv [DEMO]...');
  setStatus(b,'connecting','CONNECTING');
  b.derivWs=new WebSocket(`wss://ws.binaryws.com/websockets/v3?app_id=${APP_ID_DEMO}`);
  b.derivWs.on('open',()=>{ b.derivWs.send(JSON.stringify({authorize:b.cfg.apiToken})); });
  b.derivWs.on('message',(raw)=>{
    let d;try{d=JSON.parse(raw);}catch(e){return;}
    if(d.msg_type==='authorize'){
      if(d.error){ log(b,'❌ Auth failed: '+d.error.message); setStatus(b,'stopped','AUTH FAILED'); b.userStarted=false; b.derivWs.close(); broadcastBotState(b); return; }
      log(b,`✅ Auth: ${d.authorize.loginid} | $${d.authorize.balance}`);
      b.botActive=true; setStatus(b,'running','RUNNING');
      b.derivWs.send(JSON.stringify({ticks:b.cfg.market,subscribe:1}));
      ['M1','M5','M15','M30','H1','H4'].forEach(tf=>fetchCandles(b,tf));
      startScanner(b); broadcastBotState(b);
    }
    handleDerivMessage(b,d);
  });
  b.derivWs.on('close',()=>{
    log(b,'Disconnected'); b.botActive=false; stopScanner(b);
    setStatus(b,'stopped','DISCONNECTED'); broadcastBotState(b);
    if(b.userStarted){ if(b.reconnectTimer) clearTimeout(b.reconnectTimer); b.reconnectTimer=setTimeout(()=>connectDerivDemo(b),5000); }
  });
  b.derivWs.on('error',(e)=>log(b,'WS error: '+e.message));
}

// ── SHARED MESSAGE HANDLER ────────────────────────
function handleDerivMessage(b,d){
  if(d.msg_type==='tick'){
    b.currentPrice=parseFloat(d.tick.quote);
    broadcast({type:'price',id:b.id,price:b.currentPrice});
    if(b.botActive){ checkHTFZoneDowntrend(b); checkEntry(b); }
  }

  if(d.msg_type==='candles'){
    const gran=d.echo_req.granularity;
    const tf=gran===60?'M1':gran===300?'M5':gran===900?'M15':gran===1800?'M30':gran===3600?'H1':gran===14400?'H4':'D1';
    b.candles[tf]=d.candles.map(c=>({time:c.epoch,open:parseFloat(c.open),high:parseFloat(c.high),low:parseFloat(c.low),close:parseFloat(c.close)}));
    if(['M1','M5','M15'].includes(tf)) analyzeTrend(b,tf);
    if(['M30','H1','H4'].includes(tf)) detectAutoHTFZones(b);
    broadcast({type:'candles',id:b.id,tf,candles:b.candles[tf].slice(-100)});
  }

  if(d.msg_type==='ohlc'){
    const gran=d.ohlc.granularity;
    const tf=gran===60?'M1':gran===300?'M5':gran===900?'M15':gran===1800?'M30':gran===3600?'H1':gran===14400?'H4':'D1';
    const c={time:d.ohlc.open_time,open:parseFloat(d.ohlc.open),high:parseFloat(d.ohlc.high),low:parseFloat(d.ohlc.low),close:parseFloat(d.ohlc.close)};
    if(!b.candles[tf]) b.candles[tf]=[];
    if(b.candles[tf].length&&b.candles[tf][b.candles[tf].length-1].time===c.time) b.candles[tf][b.candles[tf].length-1]=c;
    else{b.candles[tf].push(c);if(b.candles[tf].length>300)b.candles[tf].shift();}
    if(['M1','M5','M15'].includes(tf)) analyzeTrend(b,tf);
    if(['M30','H1','H4'].includes(tf)) detectAutoHTFZones(b);
    broadcast({type:'candle_update',id:b.id,tf,candle:c});
  }

  if(d.msg_type==='proposal'){ handleProposal(b,d); return; }

  if(d.msg_type==='buy'){
    if(d.error){
      log(b,`❌ Buy error [${d.error.code||'?'}]: ${d.error.message}`);
      b.inTrade=false; broadcastBotState(b); return;
    }
    b.currentContractId=d.buy.contract_id;
    log(b,`📝 Contract opened: ${b.currentContractId}`);
    const isMulti=b.cfg.command==='CALL_MULT'||b.cfg.command==='PUT_MULT';
    const duration=b.cfg.durationMins*60;
    // Start trade timer immediately on contract open
    startTradeTimer(b,b.currentContractId,duration);
    if(isMulti){
      setTimeout(()=>{
        if(b.currentContractId&&b.derivWs?.readyState===WebSocket.OPEN)
          b.derivWs.send(JSON.stringify({proposal_open_contract:1,contract_id:b.currentContractId,subscribe:1}));
      },2000);
    } else {
      setTimeout(()=>{
        if(b.currentContractId&&b.derivWs?.readyState===WebSocket.OPEN)
          b.derivWs.send(JSON.stringify({proposal_open_contract:1,contract_id:b.currentContractId}));
      },(duration+5)*1000);
    }
  }

  if(d.msg_type==='proposal_open_contract'){
    const con=d.proposal_open_contract; if(!con) return;
    const profit=parseFloat(con.profit)||0;
    if(b.cfg.command==='CALL_MULT'||b.cfg.command==='PUT_MULT'){
      if(profit>=b.cfg.takeProfit||profit<=-b.cfg.stopLoss)
        b.derivWs.send(JSON.stringify({sell:b.currentContractId,price:0}));
    }
    if(con.status==='sold'||con.is_expired||con.is_settleable)
      finalizeResult(b,profit,con.contract_id||b.currentContractId);
  }

  if(d.msg_type==='sell'){
    if(d.sell) finalizeResult(b,parseFloat(d.sell.sold_for)-b.cfg.stake,b.currentContractId);
  }
}

function fetchCandles(b,tf){
  if(!b.derivWs||b.derivWs.readyState!==WebSocket.OPEN) return;
  const gran=tf==='M1'?60:tf==='M5'?300:tf==='M15'?900:tf==='M30'?1800:tf==='H1'?3600:tf==='H4'?14400:86400;
  b.derivWs.send(JSON.stringify({ticks_history:b.cfg.market,adjust_start_time:1,count:200,end:'latest',granularity:gran,start:1,style:'candles',subscribe:1}));
}

function startScanner(b){
  if(b.scanInterval) clearInterval(b.scanInterval);
  findLevels(b); detectAutoHTFZones(b);
  b.scanInterval=setInterval(()=>{
    if(!b.botActive) return;
    detectAutoHTFZones(b);
    if(b.inTrade||b.lossCountdownPaused||b.timeOffPaused) return;
    findLevels(b);
  },1000);
}

function stopScanner(b){ if(b.scanInterval){clearInterval(b.scanInterval);b.scanInterval=null;} }

function stopBot(b){
  b.userStarted=false; b.botActive=false;
  stopScanner(b); stopLossCountdown(b); stopTimeOff(b);
  // Stop all active trade timers
  b.activeTrades.forEach(t=>clearInterval(t.timerId));
  b.activeTrades=[];
  if(b.derivWs){try{b.derivWs.close();}catch(e){}}
  setStatus(b,'stopped','STOPPED');
  setTicker(b,`— BOT ${b.id} STOPPED —`);
  broadcastBotState(b);
}

// ── DASHBOARD WS ──────────────────────────────────
dashWss.on('connection',(ws)=>{
  console.log('📱 Dashboard connected');
  bots.forEach(b=>{
    ws.send(JSON.stringify({type:'bot_state',id:b.id,...getBotState(b)}));
    Object.keys(b.candles).forEach(tf=>{
      if(b.candles[tf]&&b.candles[tf].length)
        ws.send(JSON.stringify({type:'candles',id:b.id,tf,candles:b.candles[tf].slice(-100)}));
    });
  });

  ws.on('message',(raw)=>{
    let msg;try{msg=JSON.parse(raw);}catch(e){return;}
    const b=bots.find(x=>x.id===msg.id);
    if(!b&&msg.type!=='get_all_states') return;

    if(msg.type==='get_live_login_url'){
      const url=buildOAuthUrl(b.id);
      ws.send(JSON.stringify({type:'live_login_url',id:b.id,url}));
      return;
    }

    if(msg.type==='start'){
      if(msg.cfg) b.cfg={...b.cfg,...msg.cfg};
      const isLive=b.cfg.accountType==='live';
      if(isLive){
        if(!b.liveLoggedIn){ ws.send(JSON.stringify({type:'error',id:b.id,msg:'Please login with Deriv first'})); return; }
        b.tradeCount=0;b.wins=0;b.losses=0;b.sessionPnl=0;
        b.consecutiveLosses=0;b.lossCountdownPaused=false;
        b.activeStructures=[];b.entryTargets=[];b.activeTrades=[];
        b.userStarted=true; saveData();
        if(b.derivWs&&b.derivWs.readyState===WebSocket.OPEN){
          b.botActive=true; setStatus(b,'running','RUNNING'); startScanner(b); broadcastBotState(b);
        } else { refreshLiveOTP(b); }
      } else {
        if(!b.cfg.apiToken){ ws.send(JSON.stringify({type:'error',id:b.id,msg:'No API token'})); return; }
        b.tradeCount=0;b.wins=0;b.losses=0;b.sessionPnl=0;
        b.consecutiveLosses=0;b.lossCountdownPaused=false;
        b.activeStructures=[];b.entryTargets=[];b.activeTrades=[];
        b.userStarted=true; saveData(); connectDerivDemo(b);
      }
    }

    if(msg.type==='stop') stopBot(b);
    if(msg.type==='skip_cooldown'&&b.lossCountdownPaused) resumeAfterCooldown(b);
    if(msg.type==='time_off') startTimeOff(b,msg.secs);
    if(msg.type==='cancel_time_off') resumeAfterTimeOff(b);

    if(msg.type==='ignore_level'){
      const lv=parseFloat(msg.level).toFixed(2);
      if(b.ignoredLevels.has(lv)){b.ignoredLevels.delete(lv);}
      else{b.ignoredLevels.add(lv);}
      b.activeStructures.forEach(s=>{s.projectedLevels=computeProjectedLevels(b,s,s.tradedLevels);});
      broadcastBotState(b);
    }

    if(msg.type==='add_dnt_zone'){
      b.doNotTradeZones.push({a:parseFloat(msg.a),b:parseFloat(msg.b)});
      b.activeStructures.forEach(s=>{s.projectedLevels=computeProjectedLevels(b,s,s.tradedLevels);});
      broadcastBotState(b);
    }
    if(msg.type==='remove_dnt_zone'){
      b.doNotTradeZones.splice(msg.idx,1);
      b.activeStructures.forEach(s=>{s.projectedLevels=computeProjectedLevels(b,s,s.tradedLevels);});
      broadcastBotState(b);
    }

    if(msg.type==='add_htf_zone'){
      b.htfZones.push({a:parseFloat(msg.a),b:parseFloat(msg.b),source:'manual',
        id:`manual_${Date.now()}`,label:`Manual ${parseFloat(msg.a).toFixed(2)}–${parseFloat(msg.b).toFixed(2)}`,cancelled:false});
      broadcastBotState(b);
    }
    if(msg.type==='cancel_htf_zone'){
      const zone=b.htfZones.find(z=>z.id===msg.zoneId);
      if(zone){
        zone.cancelled=true;
        if(b.htfZonePaused){
          const stillActive=b.htfZones.filter(z=>!z.cancelled);
          const inAny=stillActive.some(z=>b.currentPrice<=Math.max(z.a,z.b)&&b.currentPrice>=Math.min(z.a,z.b));
          if(!inAny){ b.htfZonePaused=false; setStatus(b,'running','RUNNING'); setTicker(b,'✅ HTF Zone cancelled — resuming...'); }
        }
        broadcastBotState(b);
      }
    }
    if(msg.type==='restore_htf_zone'){
      const zone=b.htfZones.find(z=>z.id===msg.zoneId);
      if(zone){ zone.cancelled=false; broadcastBotState(b); }
    }
    if(msg.type==='remove_htf_zone'){
      const idx=b.htfZones.findIndex(z=>z.id===msg.zoneId&&z.source==='manual');
      if(idx>=0){ b.htfZones.splice(idx,1); broadcastBotState(b); }
    }

    if(msg.type==='test_trade'){
      if(!b.botActive){ ws.send(JSON.stringify({type:'error',id:b.id,msg:'Bot must be running'})); return; }
      log(b,'🔥 TEST TRADE — bypassing all conditions');
      placeTrade(b); return;
    }

    if(msg.type==='update_cfg'){b.cfg={...b.cfg,...msg.cfg};saveData();}
    if(msg.type==='get_history'){ ws.send(JSON.stringify({type:'full_history',id:b.id,tradeLog:b.tradeLog})); }
    if(msg.type==='get_all_states'){ bots.forEach(x=>ws.send(JSON.stringify({type:'bot_state',id:x.id,...getBotState(x)}))); }
  });

  ws.on('close',()=>console.log('📱 Dashboard disconnected'));
});

function getBotState(b){
  return {
    botActive:b.botActive,currentPrice:b.currentPrice,
    trendStatus:b.trendStatus,confirmedTrend:b.confirmedTrend,
    activeStructures:b.activeStructures.map(s=>({
      peaks:s.peaks,baseDiff:s.baseDiff,type:s.type,tf:s.tf,
      projectedLevels:s.projectedLevels,tradedLevels:[...s.tradedLevels],id:s.id
    })),
    ignoredLevels:[...b.ignoredLevels],
    doNotTradeZones:b.doNotTradeZones,
    htfZones:b.htfZones,
    htfZonePaused:b.htfZonePaused,
    htfPauseReason:b.htfPauseReason||'',
    activeHtfZoneId:b.activeHtfZoneId||null,
    autoHtfStructures:b.autoHtfStructures||[],
    activeTrades:b.activeTrades,
    currentActiveLevel:b.currentActiveLevel,currentStructType:b.currentStructType,
    tradeCount:b.tradeCount,wins:b.wins,losses:b.losses,sessionPnl:b.sessionPnl,
    consecutiveLosses:b.consecutiveLosses,
    lossCountdownPaused:b.lossCountdownPaused,lossCountdownRemaining:b.lossCountdownRemaining,lossCountdownTotal:b.lossCountdownTotal,
    timeOffPaused:b.timeOffPaused,timeOffRemaining:b.timeOffRemaining,timeOffTotal:b.timeOffTotal,
    tickerMsg:b.tickerMsg,statusText:b.statusText,cfg:b.cfg,
    liveLoggedIn:b.liveLoggedIn,liveAccountId:b.liveAccountId,
    tradeLog:b.tradeLog.slice(0,100),
  };
}

app.get('/ping',(req,res)=>res.send('OK'));
app.get('/api/state',(req,res)=>res.json(bots.map(b=>({id:b.id,...getBotState(b)}))));

setInterval(()=>{
  bots.forEach(b=>{
    if(!b.botActive) return;
    const wr=b.tradeCount>0?Math.round((b.wins/b.tradeCount)*100):0;
    console.log(`[Bot${b.id}] ${b.currentPrice} Trades:${b.tradeCount} WR:${wr}% P&L:${b.sessionPnl>=0?'+':''}$${b.sessionPnl.toFixed(2)}`);
  });
},5*60*1000);

server.listen(PORT,()=>console.log(`📈 EL ROI UPTREND 4-in-1 running on port ${PORT}`));
process.on('SIGINT',()=>{bots.forEach(b=>stopBot(b));saveData();setTimeout(()=>process.exit(0),1000);});
process.on('SIGTERM',()=>{bots.forEach(b=>stopBot(b));saveData();setTimeout(()=>process.exit(0),1000);});
