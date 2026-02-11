import { useState } from "react";
import { LineChart, Line, BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Cell, Legend, ComposedChart, Area } from "recharts";

const DATA = {
  weekly: [
    {wk:1,sales:934459,gp:215224,gpPct:0.2303,cases:15830,costPerCase:45.43,pricePerCase:59.03,gpPerCase:13.60},
    {wk:2,sales:958181,gp:220527,gpPct:0.2302,cases:16146,costPerCase:45.69,pricePerCase:59.34,gpPerCase:13.66},
    {wk:3,sales:941098,gp:217448,gpPct:0.2311,cases:15810,costPerCase:45.77,pricePerCase:59.53,gpPerCase:13.75},
    {wk:4,sales:947186,gp:217673,gpPct:0.2298,cases:15857,costPerCase:46.01,pricePerCase:59.73,gpPerCase:13.73},
    {wk:5,sales:976083,gp:224008,gpPct:0.2295,cases:16513,costPerCase:45.54,pricePerCase:59.11,gpPerCase:13.57},
    {wk:6,sales:897741,gp:204065,gpPct:0.2273,cases:15264,costPerCase:45.45,pricePerCase:58.81,gpPerCase:13.37},
    {wk:7,sales:969620,gp:222254,gpPct:0.2292,cases:16117,costPerCase:46.37,pricePerCase:60.16,gpPerCase:13.79},
    {wk:8,sales:953212,gp:218903,gpPct:0.2296,cases:15908,costPerCase:46.16,pricePerCase:59.92,gpPerCase:13.76},
    {wk:9,sales:974297,gp:224754,gpPct:0.2307,cases:16176,costPerCase:46.34,pricePerCase:60.23,gpPerCase:13.89},
    {wk:10,sales:975639,gp:224208,gpPct:0.2298,cases:16030,costPerCase:46.88,pricePerCase:60.86,gpPerCase:13.99},
    {wk:11,sales:949864,gp:217479,gpPct:0.2290,cases:15454,costPerCase:47.39,pricePerCase:61.46,gpPerCase:14.07},
    {wk:12,sales:962559,gp:219657,gpPct:0.2282,cases:15850,costPerCase:46.87,pricePerCase:60.73,gpPerCase:13.86},
    {wk:13,sales:1012337,gp:231567,gpPct:0.2287,cases:16826,costPerCase:46.40,pricePerCase:60.17,gpPerCase:13.76},
    {wk:14,sales:984675,gp:224446,gpPct:0.2279,cases:16277,costPerCase:46.71,pricePerCase:60.49,gpPerCase:13.79},
    {wk:15,sales:946992,gp:217247,gpPct:0.2294,cases:15557,costPerCase:46.91,pricePerCase:60.87,gpPerCase:13.96},
    {wk:16,sales:981970,gp:224937,gpPct:0.2291,cases:16063,costPerCase:47.13,pricePerCase:61.13,gpPerCase:14.00},
  ],
  bridge: {gpA:216491,gpB:222545,delta:6055,price:18257,cost:-14206,volume:2499,mix:-496},
  overrides: [
    {customer:"Washington County Detention",seg:"Gov",product:"SYRUP, SORGHAM",curGP:0.1348,gap:-452,recPrice:74.55,change:0.053,risk:"Med-High",conf:"Medium",annualImpact:2202},
    {customer:"AR Veterans Home",seg:"Gov",product:"BUTTER, MARGARINE",curGP:0.1288,gap:-512,recPrice:148.69,change:0.040,risk:"Medium",conf:"Medium",annualImpact:2187},
    {customer:"AR DoC - Tucker",seg:"Gov",product:"BUTTER, MARGARINE",curGP:0.1487,gap:-313,recPrice:148.63,change:0.031,risk:"Medium",conf:"Medium",annualImpact:2064},
    {customer:"Craighead County Det.",seg:"Gov",product:"DRESSING, 1000 ISLAND",curGP:0.1360,gap:-440,recPrice:73.95,change:0.054,risk:"Med-High",conf:"Medium",annualImpact:1546},
    {customer:"AR DoC - Tucker",seg:"Gov",product:"PORK, HAM, COOKED",curGP:0.1636,gap:-164,recPrice:170.96,change:0.019,risk:"Low",conf:"High",annualImpact:1324},
    {customer:"AR DoC - Cummins",seg:"Gov",product:"BAR, FIG",curGP:0.1350,gap:-450,recPrice:62.15,change:0.044,risk:"Medium",conf:"Medium",annualImpact:1304},
    {customer:"Washington County Det.",seg:"Gov",product:"COFFEE, REG",curGP:0.1694,gap:-106,recPrice:105.50,change:0.013,risk:"Low",conf:"High",annualImpact:1261},
    {customer:"AR DoC - Tucker",seg:"Gov",product:"BURRITO, BEEF&BEAN",curGP:0.1496,gap:-304,recPrice:66.79,change:0.025,risk:"Medium",conf:"Medium",annualImpact:1250},
    {customer:"AR Veterans Home",seg:"Gov",product:"DONUT, YEAST GLAZED",curGP:0.1017,gap:-783,recPrice:53.10,change:0.096,risk:"Med-High",conf:"Medium",annualImpact:1206},
    {customer:"Pulaski County Det.",seg:"Gov",product:"MUSHROOM, #10CAN",curGP:0.1045,gap:-755,recPrice:52.09,change:0.092,risk:"Med-High",conf:"Medium",annualImpact:1137},
  ],
  overrideSummary: {total:955,annualGP:147487,high:775,medium:180,low:0},
  scenarios: [
    {name:"Full Pass-Through",desc:"Pass 100% of cost increase to all customers",action:"+4.0% all commodity",gpImpact:48135,gpPct:0.2585,volChange:-0.06,risk:"Medium"},
    {name:"Targeted Overrides",desc:"Differentiated by segment price sensitivity",action:"HC +3.5%, SL +3.0%, FSR +2.0%, K-12 +1.5%, Gov +1.0%",gpImpact:22753,gpPct:0.2479,volChange:-0.055,risk:"Low-Medium"},
    {name:"Temporary Hold",desc:"Absorb 4 weeks, phased recovery with triggers",action:"Hold \u2192 +2.5% at Wk 4 \u2192 +4% at Wk 8",gpImpact:13849,gpPct:0.2289,volChange:0.03,risk:"High (short-term)"},
  ],
  lever: {
    preGP:0.2296,postGP:0.2294,preCost:55.46,postCost:58.08,prePrice:72.43,postPrice:75.84,
    cats:[
      {cat:"Potatoes & Sides",gpDelta:-143,costInc:0.050},
      {cat:"Baking & Dry Goods",gpDelta:79,costInc:0.032},
      {cat:"Canned Vegetables",gpDelta:327,costInc:0.039},
      {cat:"Canned Fruit",gpDelta:419,costInc:0.039},
      {cat:"Protein - Pork",gpDelta:938,costInc:0.063},
      {cat:"Dairy",gpDelta:1278,costInc:0.043},
      {cat:"Protein - Beef",gpDelta:1402,costInc:0.042},
      {cat:"Protein - Poultry",gpDelta:1866,costInc:0.053},
    ]
  },
  basket: {avgProducts:116,avgCats:18,commShare:0.512,weeklyBasket:12747,
    topCats:[
      {cat:"Dairy",rev:533425},{cat:"Protein - Beef",rev:530850},{cat:"Canned Vegetables",rev:431002},
      {cat:"Protein - Poultry",rev:328758},{cat:"Protein - Pork",rev:310276},
      {cat:"Potatoes & Sides",rev:290734},{cat:"Canned Fruit",rev:284573},
      {cat:"Condiments",rev:249110},{cat:"Breakfast",rev:239657},{cat:"Snacks",rev:196222}
    ]},
};

const PAL = {
  bg:"#0f1117",card:"#181b24",cardHover:"#1e2230",border:"#2a2e3b",
  text:"#e4e6ed",textMuted:"#8b8fa3",textDim:"#5c6070",
  accent:"#3b82f6",accentSoft:"rgba(59,130,246,0.12)",
  green:"#22c55e",greenSoft:"rgba(34,197,94,0.12)",
  red:"#ef4444",redSoft:"rgba(239,68,68,0.12)",
  amber:"#f59e0b",amberSoft:"rgba(245,158,11,0.12)",
  purple:"#a855f7",purpleSoft:"rgba(168,85,247,0.12)",
  cyan:"#06b6d4",
};

const fmt = (n) => {
  if (n == null || isNaN(n)) return "$0";
  const abs = Math.abs(n);
  const sign = n < 0 ? "-" : "";
  if (abs >= 1e6) return sign + "$" + (abs/1e6).toFixed(1) + "M";
  if (abs >= 1e3) return sign + "$" + (abs/1e3).toFixed(0) + "K";
  return sign + "$" + abs.toFixed(0);
};
const fmtN = (n) => {
  if (n == null || isNaN(n)) return "0";
  return n >= 1e6 ? (n/1e6).toFixed(1) + "M" : n >= 1e3 ? (n/1e3).toFixed(0) + "K" : n.toFixed(0);
};
const pct = (n) => (n*100).toFixed(1) + "%";
const bps = (n) => n + " bps";

const TABS = ["Weekly Review","Margin Bridge","Override Recs","Lever Impact","Scenarios","Data Integrity"];

function KPI({label,value,sub,color,bg:kbg}){
  return(
    <div style={{padding:"16px 20px",background:kbg||PAL.card,borderRadius:10,border:"1px solid "+PAL.border,flex:"1 1 0",minWidth:140}}>
      <div style={{fontSize:11,color:PAL.textMuted,textTransform:"uppercase",letterSpacing:"0.08em",marginBottom:6}}>{label}</div>
      <div style={{fontSize:22,fontWeight:600,color:color||PAL.text,fontVariantNumeric:"tabular-nums"}}>{value}</div>
      {sub && <div style={{fontSize:12,color:PAL.textMuted,marginTop:3}}>{sub}</div>}
    </div>
  );
}

function Badge({text,type}){
  const t = type || "info";
  const c = t==="critical" ? {bg:PAL.redSoft,fg:PAL.red} : t==="warning" ? {bg:PAL.amberSoft,fg:PAL.amber} : t==="success" ? {bg:PAL.greenSoft,fg:PAL.green} : {bg:PAL.accentSoft,fg:PAL.accent};
  return <span style={{display:"inline-block",padding:"2px 10px",borderRadius:6,fontSize:11,fontWeight:600,background:c.bg,color:c.fg,letterSpacing:"0.03em"}}>{text}</span>;
}

function Section({title,children,sub}){
  return(
    <div style={{marginBottom:28}}>
      <div style={{display:"flex",alignItems:"baseline",gap:10,marginBottom:14}}>
        <h3 style={{fontSize:16,fontWeight:600,color:PAL.text,margin:0}}>{title}</h3>
        {sub && <span style={{fontSize:12,color:PAL.textMuted}}>{sub}</span>}
      </div>
      {children}
    </div>
  );
}

function CustomTooltip({active,payload,label,formatter}){
  if(!active || !payload || !payload.length) return null;
  return(
    <div style={{background:"#1e2230",border:"1px solid "+PAL.border,borderRadius:8,padding:"10px 14px",fontSize:12}}>
      <div style={{color:PAL.textMuted,marginBottom:4}}>{label}</div>
      {payload.map((p,i) => (
        <div key={i} style={{color:p.color||PAL.text,marginBottom:2}}>
          {p.name}: {formatter ? formatter(p.value, p.name) : p.value}
        </div>
      ))}
    </div>
  );
}

function WeeklyReview(){
  const d = DATA.weekly;
  const last = d[d.length-1];
  const prev = d[d.length-2];
  const gpUp = last.gpPct > prev.gpPct;
  return(
    <div>
      <div style={{display:"flex",gap:12,flexWrap:"wrap",marginBottom:24}}>
        <KPI label="Total Net Sales (16 Wk)" value={fmt(d.reduce((s,r)=>s+r.sales,0))} sub="122 products \u00d7 77 customers"/>
        <KPI label="Total Gross Profit" value={fmt(d.reduce((s,r)=>s+r.gp,0))} color={PAL.green}/>
        <KPI label="Current GP%" value={pct(last.gpPct)} sub={(gpUp?"\u25b2":"\u25bc")+" "+pct(Math.abs(last.gpPct-prev.gpPct))+" WoW"} color={gpUp?PAL.green:PAL.amber}/>
        <KPI label="Avg Weekly Cases" value={fmtN(d.reduce((s,r)=>s+r.cases,0)/16)} sub="Across all segments"/>
      </div>

      <Section title="Weekly GP$ Trend" sub="Shaded region marks post-cost-increase period (Week 7+)">
        <div style={{background:PAL.card,borderRadius:10,padding:"16px 8px 8px",border:"1px solid "+PAL.border}}>
          <ResponsiveContainer width="100%" height={260}>
            <ComposedChart data={d} margin={{top:5,right:20,bottom:5,left:10}}>
              <defs>
                <linearGradient id="gpGrad" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor={PAL.green} stopOpacity={0.3}/>
                  <stop offset="100%" stopColor={PAL.green} stopOpacity={0.02}/>
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" stroke={PAL.border} vertical={false}/>
              <XAxis dataKey="wk" tick={{fill:PAL.textMuted,fontSize:11}} tickFormatter={v=>"W"+v}/>
              <YAxis tick={{fill:PAL.textMuted,fontSize:11}} tickFormatter={v=>fmt(v)} width={60}/>
              <Tooltip content={<CustomTooltip formatter={(v)=>fmt(v)}/>}/>
              <Area dataKey="gp" fill="url(#gpGrad)" stroke={PAL.green} strokeWidth={2} name="Gross Profit $" dot={false}/>
            </ComposedChart>
          </ResponsiveContainer>
        </div>
      </Section>

      <Section title="Price vs Cost per Case" sub="Cost shock visible at Week 7 \u2014 pricing response lagging">
        <div style={{background:PAL.card,borderRadius:10,padding:"16px 8px 8px",border:"1px solid "+PAL.border}}>
          <ResponsiveContainer width="100%" height={240}>
            <LineChart data={d} margin={{top:5,right:20,bottom:5,left:10}}>
              <CartesianGrid strokeDasharray="3 3" stroke={PAL.border} vertical={false}/>
              <XAxis dataKey="wk" tick={{fill:PAL.textMuted,fontSize:11}} tickFormatter={v=>"W"+v}/>
              <YAxis tick={{fill:PAL.textMuted,fontSize:11}} tickFormatter={v=>"$"+v} domain={[44,62]} width={50}/>
              <Tooltip content={<CustomTooltip formatter={v=>"$"+(typeof v==="number"?v.toFixed(2):v)}/>}/>
              <Line dataKey="pricePerCase" stroke={PAL.accent} strokeWidth={2} name="Avg Net Price/Case" dot={false}/>
              <Line dataKey="costPerCase" stroke={PAL.red} strokeWidth={2} name="Avg Cost/Case" dot={false} strokeDasharray="6 3"/>
              <Line dataKey="gpPerCase" stroke={PAL.green} strokeWidth={2} name="GP$/Case" dot={false}/>
              <Legend wrapperStyle={{fontSize:11,color:PAL.textMuted}}/>
            </LineChart>
          </ResponsiveContainer>
        </div>
      </Section>

      <Section title="Portfolio Basket Composition" sub="Revenue share by category (Last 4 Weeks)">
        <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fill,minmax(200px,1fr))",gap:8}}>
          {DATA.basket.topCats.map((c,i) => (
            <div key={i} style={{background:PAL.card,borderRadius:8,padding:"12px 14px",border:"1px solid "+PAL.border,display:"flex",justifyContent:"space-between",alignItems:"center"}}>
              <span style={{fontSize:13,color:PAL.text}}>{c.cat}</span>
              <span style={{fontSize:13,fontWeight:600,color:PAL.accent,fontVariantNumeric:"tabular-nums"}}>{fmt(c.rev)}</span>
            </div>
          ))}
        </div>
      </Section>
    </div>
  );
}

function MarginBridge(){
  const b = DATA.bridge;
  const bars = [
    {name:"Period A\nGP/Wk",value:b.gpA,fill:PAL.textMuted,type:"base"},
    {name:"Price\nEffect",value:b.price,fill:PAL.green,type:"delta"},
    {name:"Cost\nEffect",value:b.cost,fill:PAL.red,type:"delta"},
    {name:"Volume\nEffect",value:b.volume,fill:PAL.accent,type:"delta"},
    {name:"Mix\nEffect",value:b.mix,fill:PAL.amber,type:"delta"},
    {name:"Period B\nGP/Wk",value:b.gpB,fill:PAL.cyan,type:"base"},
  ];
  return(
    <div>
      <div style={{display:"flex",gap:12,flexWrap:"wrap",marginBottom:24}}>
        <KPI label="GP/Week (Pre)" value={fmt(b.gpA)} sub="Weeks 1\u20136"/>
        <KPI label="GP/Week (Post)" value={fmt(b.gpB)} sub="Weeks 7\u201316"/>
        <KPI label="Net Delta" value={"+"+fmt(b.delta)} color={PAL.green} sub="Per week improvement"/>
      </div>

      <Section title="GP$ Waterfall: What Moved Between Periods" sub="Price recovery offset the cost increase \u2014 volume and mix were marginal factors">
        <div style={{background:PAL.card,borderRadius:10,padding:20,border:"1px solid "+PAL.border}}>
          <div style={{display:"flex",alignItems:"end",justifyContent:"center",gap:16,height:240,position:"relative"}}>
            {bars.map((bar,i) => {
              var maxVal = b.gpA;
              var h = bar.type==="base" ? 180 : Math.max(Math.abs(bar.value)/maxVal*500, 16);
              var isNeg = bar.value < 0;
              return(
                <div key={i} style={{display:"flex",flexDirection:"column",alignItems:"center",gap:6,width:90}}>
                  <div style={{fontSize:12,fontWeight:600,color:isNeg?PAL.red:bar.fill,fontVariantNumeric:"tabular-nums"}}>
                    {bar.type==="delta" ? (isNeg ? "-" : "+") : ""}{fmt(Math.abs(bar.value))}
                  </div>
                  <div style={{width:56,height:h,borderRadius:6,background:bar.fill,opacity:bar.type==="base"?0.25:0.8,transition:"all 0.3s"}}/>
                  <div style={{fontSize:10,color:PAL.textMuted,textAlign:"center",whiteSpace:"pre-line",lineHeight:"1.3"}}>{bar.name}</div>
                </div>
              );
            })}
          </div>
        </div>
      </Section>

      <Section title="Key Takeaway">
        <div style={{background:PAL.card,borderRadius:10,padding:20,border:"1px solid "+PAL.border,fontSize:14,color:PAL.text,lineHeight:1.7}}>
          <p style={{margin:"0 0 10px"}}>The commodity cost increase at Week 7 introduced <span style={{color:PAL.red,fontWeight:600}}>$14.2K/week</span> in cost headwind. However, pricing actions already recovered <span style={{color:PAL.green,fontWeight:600}}>$18.3K/week</span> through net price improvement \u2014 meaning the portfolio absorbed the shock and generated incremental margin.</p>
          <p style={{margin:0}}>Volume contributed a modest <span style={{color:PAL.accent,fontWeight:600}}>$2.5K/week</span> uplift, while mix was slightly negative at <span style={{color:PAL.amber,fontWeight:600}}>-$496/week</span>, suggesting some customers shifted toward lower-margin products after the price increase.</p>
        </div>
      </Section>

      <Section title="Category-Level Impact of Cost Increase" sub="Commodity categories with largest cost pass-through gaps">
        <div style={{background:PAL.card,borderRadius:10,padding:"16px 8px 8px",border:"1px solid "+PAL.border}}>
          <ResponsiveContainer width="100%" height={280}>
            <BarChart data={DATA.lever.cats} layout="vertical" margin={{top:5,right:20,bottom:5,left:120}}>
              <CartesianGrid strokeDasharray="3 3" stroke={PAL.border} horizontal={false}/>
              <XAxis type="number" tick={{fill:PAL.textMuted,fontSize:11}} tickFormatter={v=>fmt(v)}/>
              <YAxis type="category" dataKey="cat" tick={{fill:PAL.textMuted,fontSize:11}} width={115}/>
              <Tooltip content={<CustomTooltip formatter={(v,n)=> n && n.includes("Cost") ? pct(v) : fmt(v)}/>}/>
              <Bar dataKey="gpDelta" name="GP$/Wk Delta" radius={[0,4,4,0]}>
                {DATA.lever.cats.map((e,i) => <Cell key={i} fill={e.gpDelta>=0 ? PAL.green : PAL.red}/>)}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>
      </Section>
    </div>
  );
}

function OverrideRecs(){
  var o = DATA.overrideSummary;
  var segData = [
    {seg:"Corrections/Gov",val:126488},{seg:"K-12 Education",val:20972},{seg:"Senior Living",val:27}
  ];
  return(
    <div>
      <div style={{display:"flex",gap:12,flexWrap:"wrap",marginBottom:24}}>
        <KPI label="Total Recommendations" value={o.total.toLocaleString()} sub="Below 18% GP floor"/>
        <KPI label="Projected Annual GP Recovery" value={fmt(o.annualGP)} color={PAL.green}/>
        <KPI label="High Confidence" value={String(o.high)} sub={Math.round((o.high/o.total)*100)+"% of total"} color={PAL.green}/>
        <KPI label="Medium Confidence" value={String(o.medium)} color={PAL.amber}/>
      </div>

      <Section title="Override Impact by Segment" sub="Corrections/Government dominates \u2014 price-sensitive segments with below-target margins">
        <div style={{background:PAL.card,borderRadius:10,padding:"16px 8px 8px",border:"1px solid "+PAL.border}}>
          <ResponsiveContainer width="100%" height={180}>
            <BarChart data={segData} margin={{top:5,right:20,bottom:5,left:10}}>
              <CartesianGrid strokeDasharray="3 3" stroke={PAL.border} vertical={false}/>
              <XAxis dataKey="seg" tick={{fill:PAL.textMuted,fontSize:11}}/>
              <YAxis tick={{fill:PAL.textMuted,fontSize:11}} tickFormatter={v=>fmt(v)} width={60}/>
              <Tooltip content={<CustomTooltip formatter={v=>fmt(v)}/>}/>
              <Bar dataKey="val" name="Annual GP$ Impact" fill={PAL.green} radius={[4,4,0,0]}/>
            </BarChart>
          </ResponsiveContainer>
        </div>
      </Section>

      <Section title="Top Override Recommendations \u2014 Action List" sub="Sorted by projected annual GP impact. Ready for weekly review.">
        <div style={{overflowX:"auto",borderRadius:10,border:"1px solid "+PAL.border}}>
          <table style={{width:"100%",borderCollapse:"collapse",fontSize:12,background:PAL.card}}>
            <thead>
              <tr style={{borderBottom:"1px solid "+PAL.border}}>
                {["Customer","Product","Cur GP%","Gap","Rec Price","\u0394 Price","Risk","Conf","Annual GP$"].map(function(h){return(
                  <th key={h} style={{padding:"10px 12px",textAlign:"left",color:PAL.textMuted,fontWeight:500,fontSize:11,textTransform:"uppercase",letterSpacing:"0.05em"}}>{h}</th>
                )})}
              </tr>
            </thead>
            <tbody>
              {DATA.overrides.map(function(r,i){return(
                <tr key={i} style={{borderBottom:"1px solid "+PAL.border}}>
                  <td style={{padding:"10px 12px",color:PAL.text,fontWeight:500}}>{r.customer}</td>
                  <td style={{padding:"10px 12px",color:PAL.textMuted}}>{r.product}</td>
                  <td style={{padding:"10px 12px",color:r.curGP<0.14?PAL.red:PAL.amber,fontWeight:600,fontVariantNumeric:"tabular-nums"}}>{pct(r.curGP)}</td>
                  <td style={{padding:"10px 12px",color:PAL.red,fontVariantNumeric:"tabular-nums"}}>{bps(r.gap)}</td>
                  <td style={{padding:"10px 12px",color:PAL.text,fontVariantNumeric:"tabular-nums"}}>{"$"+r.recPrice.toFixed(2)}</td>
                  <td style={{padding:"10px 12px",color:PAL.green,fontVariantNumeric:"tabular-nums"}}>{"+"+pct(r.change)}</td>
                  <td style={{padding:"10px 12px"}}><Badge text={r.risk} type={r.risk.includes("High")?"warning":"info"}/></td>
                  <td style={{padding:"10px 12px"}}><Badge text={r.conf} type={r.conf==="High"?"success":"warning"}/></td>
                  <td style={{padding:"10px 12px",color:PAL.green,fontWeight:600,fontVariantNumeric:"tabular-nums"}}>{fmt(r.annualImpact)}</td>
                </tr>
              )})}
            </tbody>
          </table>
        </div>
        <div style={{marginTop:8,fontSize:11,color:PAL.textDim}}>{"Showing top 10 of "+o.total+" recommendations. Full dataset exported to weekly override action list."}</div>
      </Section>

      <Section title="Recommendation Methodology">
        <div style={{background:PAL.card,borderRadius:10,padding:20,border:"1px solid "+PAL.border,fontSize:13,color:PAL.text,lineHeight:1.7}}>
          <p style={{margin:"0 0 8px"}}><strong>Floor:</strong> 18% GP target applied uniformly. Each customer-product pair below floor is evaluated for override-up.</p>
          <p style={{margin:"0 0 8px"}}><strong>Confidence scoring:</strong> Based on required price increase magnitude \u2014 less than 2% increase = High confidence (minimal volume risk), 2-5% = Medium, above 5% = needs additional validation with Sales.</p>
          <p style={{margin:0}}><strong>Volume risk:</strong> Estimated using historical price-quantity sensitivity proxy. Accounts with high commodity basket share receive heightened volume-loss estimates.</p>
        </div>
      </Section>
    </div>
  );
}

function LeverImpact(){
  var l = DATA.lever;
  return(
    <div>
      <div style={{background:PAL.amberSoft,borderRadius:10,padding:"14px 20px",border:"1px solid rgba(245,158,11,0.25)",marginBottom:24,display:"flex",alignItems:"center",gap:12}}>
        <span style={{fontSize:18}}>{"\u26a0"}</span>
        <div>
          <div style={{fontSize:13,fontWeight:600,color:PAL.amber}}>Commodity Cost Lever Change Detected \u2014 Week 7</div>
          <div style={{fontSize:12,color:PAL.textMuted,marginTop:2}}>{"Average commodity cost increased "+pct(0.04)+" ("+fmt(l.preCost)+" \u2192 "+fmt(l.postCost)+" per case). This analysis measures the downstream margin and volume impact."}</div>
        </div>
      </div>

      <div style={{display:"flex",gap:12,flexWrap:"wrap",marginBottom:24}}>
        <KPI label="Pre-Lever Commodity GP%" value={pct(l.preGP)} sub="Weeks 1\u20136"/>
        <KPI label="Post-Lever Commodity GP%" value={pct(l.postGP)} sub="Weeks 7\u201316" color={PAL.amber}/>
        <KPI label="Avg Commodity Cost Change" value={"+"+pct(0.04)} color={PAL.red} sub="$55.46 \u2192 $58.08 per case"/>
        <KPI label="Price Recovery" value={"+"+pct(0.047)} color={PAL.green} sub="$72.43 \u2192 $75.84 per case"/>
      </div>

      <Section title="Commodity Cost vs Price Response Over Time" sub="Tracking whether pricing actions are keeping pace with cost increases">
        <div style={{background:PAL.card,borderRadius:10,padding:"16px 8px 8px",border:"1px solid "+PAL.border}}>
          <ResponsiveContainer width="100%" height={260}>
            <LineChart data={DATA.weekly} margin={{top:5,right:20,bottom:5,left:10}}>
              <CartesianGrid strokeDasharray="3 3" stroke={PAL.border} vertical={false}/>
              <XAxis dataKey="wk" tick={{fill:PAL.textMuted,fontSize:11}} tickFormatter={v=>"W"+v}/>
              <YAxis tick={{fill:PAL.textMuted,fontSize:11}} tickFormatter={v=>"$"+v} domain={[44,62]} width={50}/>
              <Tooltip content={<CustomTooltip formatter={v=>"$"+(typeof v==="number"?v.toFixed(2):v)}/>}/>
              <Line dataKey="pricePerCase" stroke={PAL.green} strokeWidth={2.5} name="Avg Price/Case" dot={false}/>
              <Line dataKey="costPerCase" stroke={PAL.red} strokeWidth={2.5} name="Avg Cost/Case" dot={false}/>
              <Legend wrapperStyle={{fontSize:11}}/>
            </LineChart>
          </ResponsiveContainer>
        </div>
      </Section>

      <Section title="Impact by Commodity Category" sub="Weekly GP$ change (post vs pre lever shift)">
        <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fill,minmax(280px,1fr))",gap:10}}>
          {l.cats.map(function(c,i){return(
            <div key={i} style={{background:PAL.card,borderRadius:10,padding:"14px 16px",border:"1px solid "+PAL.border,display:"flex",justifyContent:"space-between",alignItems:"center"}}>
              <div>
                <div style={{fontSize:13,color:PAL.text,fontWeight:500}}>{c.cat}</div>
                <div style={{fontSize:11,color:PAL.textMuted,marginTop:2}}>{"Cost +"+pct(c.costInc)}</div>
              </div>
              <div style={{textAlign:"right"}}>
                <div style={{fontSize:15,fontWeight:600,color:c.gpDelta>=0?PAL.green:PAL.red,fontVariantNumeric:"tabular-nums"}}>
                  {(c.gpDelta>=0?"+":"")+fmt(c.gpDelta)+"/wk"}
                </div>
              </div>
            </div>
          )})}
        </div>
      </Section>

      <Section title="Analysis Summary">
        <div style={{background:PAL.card,borderRadius:10,padding:20,border:"1px solid "+PAL.border,fontSize:13,color:PAL.text,lineHeight:1.7}}>
          <p style={{margin:"0 0 10px"}}>The ~4% commodity cost increase at Week 7 was largely absorbed by corresponding pricing adjustments. Most protein categories (Beef, Poultry, Pork) and Dairy actually improved GP$/week post-lever, indicating that price recovery actions slightly exceeded the cost increase for high-volume categories.</p>
          <p style={{margin:0}}>The exception is <span style={{color:PAL.red,fontWeight:600}}>Potatoes &amp; Sides</span>, where cost increases of 5% outpaced price recovery \u2014 this category warrants targeted overrides to close the margin gap. The remaining categories remain within governance thresholds.</p>
        </div>
      </Section>
    </div>
  );
}

function ScenarioView(){
  var s = DATA.scenarios;
  var colors = [PAL.accent, PAL.green, PAL.amber];
  return(
    <div>
      <Section title="Scenario Comparison: Commodity Cost Pass-Through Strategy" sub="Three approaches modeled against the current 4% commodity cost increase">
        <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(300px,1fr))",gap:14,marginBottom:24}}>
          {s.map(function(sc,i){return(
            <div key={i} style={{background:PAL.card,borderRadius:12,padding:20,border:"2px solid "+(i===1?PAL.green:PAL.border),position:"relative"}}>
              {i===1 && <div style={{position:"absolute",top:-10,right:16,background:PAL.green,color:"#000",fontSize:10,fontWeight:700,padding:"3px 10px",borderRadius:6,letterSpacing:"0.05em"}}>RECOMMENDED</div>}
              <div style={{fontSize:15,fontWeight:600,color:colors[i],marginBottom:4}}>{sc.name}</div>
              <div style={{fontSize:12,color:PAL.textMuted,marginBottom:14,lineHeight:1.5}}>{sc.desc}</div>
              <div style={{fontSize:11,color:PAL.textDim,marginBottom:10,padding:"6px 10px",background:PAL.bg,borderRadius:6}}>{sc.action}</div>
              <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:8}}>
                <div><div style={{fontSize:10,color:PAL.textMuted}}>GP$ Impact</div><div style={{fontSize:18,fontWeight:600,color:sc.gpImpact>0?PAL.green:PAL.red}}>{fmt(sc.gpImpact)}</div></div>
                <div><div style={{fontSize:10,color:PAL.textMuted}}>Proj GP%</div><div style={{fontSize:18,fontWeight:600,color:PAL.text}}>{pct(sc.gpPct)}</div></div>
                <div><div style={{fontSize:10,color:PAL.textMuted}}>{"Volume \u0394"}</div><div style={{fontSize:14,fontWeight:600,color:sc.volChange>=0?PAL.green:PAL.red}}>{(sc.volChange>=0?"+":"")+pct(sc.volChange)}</div></div>
                <div><div style={{fontSize:10,color:PAL.textMuted}}>Risk</div><Badge text={sc.risk} type={sc.risk.includes("High")?"warning":sc.risk.includes("Low")?"success":"info"}/></div>
              </div>
            </div>
          )})}
        </div>
      </Section>

      <Section title="Scenario Impact Comparison">
        <div style={{background:PAL.card,borderRadius:10,padding:"16px 8px 8px",border:"1px solid "+PAL.border}}>
          <ResponsiveContainer width="100%" height={220}>
            <BarChart data={s.map(function(sc){return{name:sc.name,gpImpact:sc.gpImpact}})} margin={{top:5,right:20,bottom:5,left:10}}>
              <CartesianGrid strokeDasharray="3 3" stroke={PAL.border} vertical={false}/>
              <XAxis dataKey="name" tick={{fill:PAL.textMuted,fontSize:11}}/>
              <YAxis tick={{fill:PAL.textMuted,fontSize:11}} tickFormatter={v=>fmt(v)} width={60}/>
              <Tooltip content={<CustomTooltip formatter={v=>fmt(v)}/>}/>
              <Bar dataKey="gpImpact" name="GP$ Impact" fill={PAL.green} radius={[4,4,0,0]}/>
            </BarChart>
          </ResponsiveContainer>
        </div>
      </Section>

      <Section title="Recommendation">
        <div style={{background:"rgba(34,197,94,0.06)",borderRadius:10,padding:20,border:"1px solid rgba(34,197,94,0.2)",fontSize:14,color:PAL.text,lineHeight:1.7}}>
          <p style={{margin:"0 0 10px",fontWeight:600,color:PAL.green}}>Scenario B: Targeted Overrides is the recommended approach.</p>
          <p style={{margin:"0 0 10px"}}>It balances margin recovery (+$22.8K GP) with volume protection (-5.5% vs -6.0% for full pass-through). The segment-differentiated strategy matches the customer price sensitivity profiles: Healthcare and Senior Living can absorb higher pass-through rates, while K-12 and Corrections need gentler treatment to prevent churn.</p>
          <p style={{margin:0}}>Implementation plan: apply overrides in the pricing system this week, monitor volume response over 4 weeks, and escalate any segment showing greater than 8% volume decline to Sales for account-level intervention.</p>
        </div>
      </Section>
    </div>
  );
}

function DataIntegrity(){
  var checks = [
    {check:"Override Audit Trail",sev:"INFO",count:9843,detail:"9,843 active overrides across 16 weeks. All require documented reason codes and expiry dates for governance compliance.",action:"Audit a 10% random sample for valid reason codes; flag and expire any override older than 90 days."},
    {check:"Stale Pricing Detection",sev:"INFO",count:0,detail:"No products detected with zero price movement over 8+ consecutive weeks. This is a healthy signal \u2014 all products are being actively managed.",action:"Continue monitoring. If commodity indices shift >5%, trigger immediate cost-recovery scan."},
    {check:"Price Variance Consistency",sev:"WARNING",count:14,detail:"14 products show coefficient of variation >15% in net price across customer base. May indicate inconsistent tier assignment or unauthorized off-list discounts.",action:"Pull tier assignment for flagged SKUs. Validate that variance is intentional (segment pricing) vs leakage."},
    {check:"Negative Margin Guard",sev:"CRITICAL",count:0,detail:"Zero transactions detected with negative gross margin. Pricing floor constraints are holding as expected.",action:"No action required. Maintain automated cost-floor checks in pricing system config."},
    {check:"Cost Refresh Lag",sev:"WARNING",count:8,detail:"8 commodity products where the last cost update was >14 days ago but commodity index moved >2%. Stale cost inputs lead to underpriced overrides.",action:"Request cost data refresh from Procurement. Cross-check against USDA/CME commodity feeds."},
    {check:"Duplicate Pricing Entries",sev:"INFO",count:0,detail:"No duplicate customer-product pricing configurations detected. Clean state.",action:"Maintain dedup checks in weekly data pipeline."},
  ];
  return(
    <div>
      <Section title="Pricing System Data Integrity Audit" sub="Automated checks run against 82,620 transactions across 16 weeks">
        <div style={{display:"flex",gap:12,flexWrap:"wrap",marginBottom:20}}>
          <KPI label="Checks Passed" value="4/6" color={PAL.green}/>
          <KPI label="Warnings" value="2" color={PAL.amber}/>
          <KPI label="Critical Issues" value="0" color={PAL.green}/>
          <KPI label="Transactions Scanned" value="82.6K"/>
        </div>
        <div style={{display:"flex",flexDirection:"column",gap:10}}>
          {checks.map(function(c,i){return(
            <div key={i} style={{background:PAL.card,borderRadius:10,padding:"16px 20px",border:"1px solid "+PAL.border}}>
              <div style={{display:"flex",alignItems:"center",gap:10,marginBottom:8}}>
                <Badge text={c.sev} type={c.sev==="CRITICAL"?"critical":c.sev==="WARNING"?"warning":"info"}/>
                <span style={{fontSize:14,fontWeight:600,color:PAL.text}}>{c.check}</span>
                <span style={{fontSize:12,color:PAL.textMuted,marginLeft:"auto"}}>{c.count+" items"}</span>
              </div>
              <div style={{fontSize:12,color:PAL.textMuted,lineHeight:1.6,marginBottom:6}}>{c.detail}</div>
              <div style={{fontSize:12,color:PAL.accent}}>{"\u2192 "+c.action}</div>
            </div>
          )})}
        </div>
      </Section>
    </div>
  );
}

export default function Dashboard(){
  const [tab, setTab] = useState(0);

  var views = [WeeklyReview, MarginBridge, OverrideRecs, LeverImpact, ScenarioView, DataIntegrity];
  var View = views[tab];

  return(
    <div style={{minHeight:"100vh",background:PAL.bg,color:PAL.text,fontFamily:"'Helvetica Neue',Arial,sans-serif"}}>
      <div style={{padding:"20px 28px 0",borderBottom:"1px solid "+PAL.border}}>
        <div style={{display:"flex",alignItems:"center",justifyContent:"space-between",marginBottom:14}}>
          <div>
            <div style={{fontSize:11,color:PAL.accent,fontWeight:600,letterSpacing:"0.12em",textTransform:"uppercase",marginBottom:4}}>Sysco Arkansas \u2014 Revenue Management</div>
            <h1 style={{fontSize:22,fontWeight:700,color:PAL.text,margin:0}}>Pricing Analytics &amp; Override Recommendation Engine</h1>
            <div style={{fontSize:12,color:PAL.textMuted,marginTop:4}}>Contract S000000035 \u00b7 Statewide Groceries \u00b7 122 Products \u00b7 77 Customers \u00b7 16-Week Window</div>
          </div>
          <div style={{textAlign:"right"}}>
            <div style={{fontSize:11,color:PAL.textMuted}}>Analysis Date</div>
            <div style={{fontSize:14,fontWeight:600,color:PAL.text}}>Feb 10, 2026</div>
          </div>
        </div>
        <div style={{display:"flex",gap:2,overflowX:"auto"}}>
          {TABS.map(function(t,i){return(
            <button key={i} onClick={function(){setTab(i)}}
              style={{
                padding:"10px 18px",fontSize:13,fontWeight:tab===i?600:400,
                color:tab===i?PAL.accent:PAL.textMuted,
                background:tab===i?PAL.accentSoft:"transparent",
                border:"none",borderBottom:tab===i?"2px solid "+PAL.accent:"2px solid transparent",
                borderRadius:"6px 6px 0 0",cursor:"pointer",whiteSpace:"nowrap",
                transition:"all 0.15s",
              }}>
              {t}
            </button>
          )})}
        </div>
      </div>
      <div style={{padding:"24px 28px 40px",maxWidth:1100}}>
        <View/>
      </div>
      <div style={{padding:"16px 28px",borderTop:"1px solid "+PAL.border,fontSize:11,color:PAL.textDim,display:"flex",justifyContent:"space-between"}}>
        <span>Built with Python (pandas, scipy) + React \u00b7 Data: Sysco Price Sheet (Eff. 4/1/23)</span>
        <span>Ayush Bhardwaj \u00b7 Revenue Management Portfolio Project</span>
      </div>
    </div>
  );
}
