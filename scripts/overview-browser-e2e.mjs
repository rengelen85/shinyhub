#!/usr/bin/env node
// Render the production dashboard with synthetic API data; no live accounts or mutations.
// Screenshots are opt-in local artifacts. Requires the render-driver Playwright install.
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { readFile, writeFile, mkdir } from 'node:fs/promises';
import { resolve, extname } from 'node:path';
import { createRequire } from 'node:module';
const root=process.cwd(), out=process.env.SHINYHUB_E2E_SCREENSHOTS;
if(out) await mkdir(out,{recursive:true});
const require=createRequire(root+'/loadtest/render/driver/package.json');
const {chromium}=require('playwright');
const now=new Date().toISOString(), ago=new Date(Date.now()-3600000).toISOString(), mb=1048576;
let role='admin', metricsFail=false, metricsDelay=0;
const apps=[{slug:'forecast',name:'Regional forecast',description:'Explore regional demand and capacity forecasts.',status:'running',project:'analytics',project_slug:'analytics',can_manage:true,access:'private',replicas:2,replicas_running:2,deploy_count:3,current_version:'3',last_deployment_status:'succeeded',last_deployed_at:ago,framework:'shiny',runtime:'python',entrypoint:'app.py',memory_limit_mb:1024,cpu_quota_percent:100,autoscale_enabled:true,autoscale_min_replicas:1,autoscale_max_replicas:4,max_sessions_per_replica:20}, {slug:'revenue',name:'Revenue explorer',description:'Track performance across markets.',status:'sleeping',can_manage:true,access:'private',deploy_count:1,project:'analytics',project_slug:'analytics'}, {slug:'reports',name:'Weekly reports and financial reconciliation',status:'crashed',can_manage:true,access:'private',deploy_count:2,last_deployment_status:'failed'}];
const reps=[0,1].map(index=>({index,status:'running',desired_state:'running',metrics_available:true,cpu_percent:10+index*20,rss_bytes:(180+index*30)*mb,sessions:3+index,effective_memory_limit_mb:1024,effective_cpu_quota_percent:100,memory_limit_enforced:true,cpu_quota_enforced:true,resource_enforcement_known:true,pid:1000+index,run_id:'fixture-'+index}));
const metrics={replicas:reps,status:'running',replicas_running:2,replicas_desired:2,cpu_cores:.4,cpu_capacity_cores:2,cpu_capacity_source:'quota',cpu_host_cores:8,cpu_saturation_available:true,sessions_cap:20,sessions_ceiling:40,generated_at:now};
apps[0].managed_by='fleet:prod';apps[2].status='stopped';reps[0].rss_bytes=1000*mb;reps[1].effective_memory_limit_mb=0;reps[1].memory_limit_enforced=false;
// Include a genuine measurement gap; unset optional limits alone are healthy.
reps.push({...reps[1],index:2,metrics_available:false,rss_bytes:null,cpu_percent:null});
apps[0].replicas=apps[0].replicas_running=metrics.replicas_running=metrics.replicas_desired=3;
const user={id:1,username:'operator',display_name:'Platform operator',role:'admin',auth_provider:'local',can_manage_self:true};
const list=items=>({items:items,total:items.length,limit:50,offset:0});
const types={'.js':'text/javascript','.css':'text/css','.svg':'image/svg+xml','.woff2':'font/woff2','.png':'image/png','.ico':'image/x-icon'};
const unknown=new Set();
const server=createServer(async(req,res)=>{
try{const path=new URL(req.url,'http://localhost').pathname;
if(path==='/api/apps/metrics'&&metricsDelay)await new Promise(r=>setTimeout(r,metricsDelay));
let body,type='application/json',status=200;
if(path==='/announcements.js'){body=Buffer.from('');type='text/javascript';}
else if(path.startsWith('/static/')){body=await readFile(resolve(root+'/internal/ui','.'+path));type=types[extname(path)]||'application/octet-stream';}
else if(path==='/api/auth/me'){body={user:{...user,role},can_create_apps:role==='admin',can_manage_apps:role==='admin',can_read_audit:role==='admin',can_manage_announcements:role==='admin',session:{refresh_after_seconds:1}};if(role==='guest'){status=401;body={error:'Sign in required'};}}
else if(path==='/api/auth/providers')body={providers:[]};
else if(path==='/api/server-info')body={version:'local review',runtime_mode:'native',can_create_apps:true};
else if(path==='/api/support-sessions/current')body={active:null};
else if(path==='/api/fleet/health')body={status:'healthy',workers:[],alerts:[]};
else if(path==='/api/apps')body=list(apps.map(a=>({...a,can_manage:role==='admin'})));
else if(path==='/api/audit')body={events:[{id:1,action:'app.deploy',username:'operator',actor:'operator',resource:'forecast',resource_type:'app',resource_id:'forecast',created_at:now,details:{version:'3',status:'succeeded'},ip:'192.0.2.1'},{id:2,action:'user.create',username:'operator',resource:'analyst',created_at:ago,details:{role:'developer'},ip:'192.0.2.1'}],total:2,has_more:false};
else if(path==='/api/apps/metrics'&&metricsFail){status=503;body={error:'Metrics temporarily unavailable'};}
else if(path==='/api/apps/metrics')body={metrics:{forecast:metrics},host:{cores:8,memory_mb:16384},generated_at:now};
else if(path.startsWith('/api/') && path.endsWith('/metrics/history'))body={history:{},series:{ts:[],cpu:[],rss:[],sessions:[],instances:[]},window_seconds:900};
else if(path==='/api/apps/forecast')body={app:apps[0],can_manage:role==='admin',replicas_status:reps,runtime_mode:'native',effective_max_sessions_per_replica:20,effective_autoscale_target:.75,global_autoscale_enabled:true};
else if(path.startsWith('/api/') && path.endsWith('/env'))body=list([{key:'REGION',value:'europe',secret:false},{key:'API_KEY',secret:true}]);
else if(path.startsWith('/api/')){unknown.add(path);status=404;body={error:'Missing API fixture'};}
else {body=await readFile(root+'/internal/ui/static/index.html');type='text/html';}
res.writeHead(status,{'content-type':type,'cache-control':'no-store'});res.end(Buffer.isBuffer(body)?body:JSON.stringify(body));
}catch(e){res.writeHead(500);res.end(e.message);}});
await new Promise(r=>server.listen(0,'127.0.0.1',r));
const origin=`http://127.0.0.1:${server.address().port}`;
const browser=await chromium.launch({headless:true,executablePath:process.env.SHINYHUB_E2E_CHROMIUM || undefined});

const results=[];
const context=await browser.newContext({viewport:{width:1440,height:1000},userAgent:'shinyhub-ui-review'});
const page=await context.newPage();const errors=[];page.on('pageerror',e=>errors.push(e.message));
const ready=async()=>{await page.locator('#ov-scope-select').waitFor();};
const focusVisible=async()=>page.evaluate(()=>{const r=document.activeElement.getBoundingClientRect();return r.top>=0 && r.bottom<=innerHeight;});
const key=async()=>page.evaluate(()=>document.activeElement.dataset.focusKey||document.activeElement.id);
async function tabTo(target){for(let i=0;i<80;i++){if(await key()===target)return;await page.keyboard.press('Tab');}throw new Error('Cannot tab to '+target);}
try{
 await page.goto(origin);await ready();await page.evaluate(()=>document.fonts.ready);
 // A keyboard journey through the real router to actual app controls and Back.
 assert.equal(await page.locator('.ov-pulse-actions button').first().textContent(),'Review app issues');
 assert.match(await page.locator('.ov-pulse-detail').textContent(),/Weekly reports and financial reconciliation/);
 await page.locator('[data-focus-key="attention:review"]').click();assert.equal(await key(),'attention:heading');
 assert.equal(await page.locator('[data-focus-key="pressure:review"]').isVisible(),true);
 await page.locator('#ov-scope-select').focus();
 await tabTo('overview:scope');await page.locator('#ov-scope-select').selectOption('fleet:prod');
 await tabTo('activity:jump');await page.keyboard.press('Enter');assert.equal(await key(),'activity:heading');
 await page.locator('#ov-scope-select').focus();await tabTo('coverage:review');await page.keyboard.press('Enter');assert.equal(await key(),'coverage:heading');
 await tabTo('coverage:forecast:unlimited');assert.equal(await page.evaluate(()=>getComputedStyle(document.activeElement).outlineStyle),'solid');
 await page.keyboard.press('Enter');await page.waitForURL('**/apps/forecast/configuration');await page.locator('#resources-memory').waitFor();
 assert.equal(await page.locator('#resources-memory').isEnabled(),true);
 await page.waitForResponse(r=>r.url().endsWith('/api/auth/me')&&r.status()===200);
 await page.goBack();await ready();assert.equal(await page.locator('#ov-scope-select').inputValue(),'fleet:prod');
 assert.equal(await key(),'coverage:forecast:unlimited');
 assert.equal(await focusVisible(),true,'Return focus is within the viewport');
 await page.locator('[data-focus-key="capacity:help"]').click();
 await page.locator('[data-focus-key="pressure:forecast:memory"]').click();
 await page.waitForURL('**/apps/forecast');await page.locator('#app-detail-view').waitFor();
 await page.goBack();await ready();assert.equal(await page.locator('#ov-scope-select').inputValue(),'fleet:prod');
 assert.equal(await page.locator('[data-focus-key="capacity:help"]').getAttribute('aria-expanded'),'true');
 assert.equal(await key(),'pressure:forecast:memory');
 results.push('Actual router: settings controls, pressure inspection, Back scope/disclosure/focus restoration after session renewal');
 // Mounted polling: last good readings retained on outage, then explicit retry.
 metricsFail=true;await page.getByRole('button',{name:'Retry readings',exact:true}).waitFor({timeout:15000});
 assert.match(await page.locator('.ov-resources').innerText(),/Last reported app usage/);
 assert.match(await page.locator('#overview-live').textContent(),/stale/);
 await page.getByRole('button',{name:'Retry readings',exact:true}).focus();metricsDelay=500;
 await page.keyboard.press('Enter');await page.getByRole('button',{name:'Trying again…',exact:true}).waitFor();
 await page.getByRole('button',{name:'Retry readings',exact:true}).waitFor();assert.equal(await key(),'usage:retry');
 assert.match(await page.locator('#overview-live').textContent(),/could not refresh/);
 metricsFail=false;await page.keyboard.press('Enter');await page.locator('[data-focus-key="usage:retry"]').waitFor({state:'detached'});
 assert.equal(await key(),'usage:heading');assert.match(await page.locator('#overview-live').textContent(),/Resource readings refreshed/);
 results.push('Mounted stale failure, retry pending/failure/success, retained readings and keyboard focus');
 // Initial failure is different from stale data; recover via actual poll.
 metricsDelay=0;metricsFail=true;await page.reload();await ready();assert.match(await page.locator('.ov-resources').innerText(),/Metrics unavailable/);
 metricsFail=false;await page.getByRole('button',{name:'Retry readings',exact:true}).waitFor({state:'detached',timeout:15000});
 assert.doesNotMatch(await page.locator('.ov-resources').innerText(),/Metrics unavailable/);
 results.push('Mounted initial metrics failure and automatic recovery');
 await page.evaluate(()=>window.scrollTo(0,0));await page.mouse.move(1,1);
 if(out) await page.screenshot({path:out+'/app-shell-desktop.png',fullPage:true});
 await page.setViewportSize({width:390,height:844});await page.mouse.move(1,1);
 assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth),false);
 const attentionButtons=await page.locator('.ov-attn-actions .ov-btn').all();for(const button of attentionButtons)assert.ok((await button.boundingBox()).height>=44);
 const jump=await page.locator('.ov-recent-jump').boundingBox();assert.ok(jump.y+jump.height<844,'Recent changes shortcut visible without scrolling');
 await page.addScriptTag({path:root+'/node_modules/axe-core/axe.min.js'});
 const a11y=await page.evaluate(async()=>{const r=await axe.run(document.querySelector('#overview-view'),{runOnly:{type:'tag',values:['wcag2a','wcag2aa','wcag21aa']}});return r.violations.map(v=>({id:v.id,nodes:v.nodes.map(n=>n.target)}));});
 assert.deepEqual(a11y,[]);assert.deepEqual(errors,[]);
 // Capture after layout and accessibility checks, once hover transitions settle.
 if(out) await page.screenshot({path:out+'/app-shell-mobile.png',fullPage:true,animations:'disabled'});
 results.push('Mobile shell: shortcut in first viewport, no overflow or axe violations');
 // Live data may push a focused row beyond the initially visible list.
 await page.locator('[data-focus-key="coverage:forecast:unlimited"]').focus();
 for(let i=0;i<5;i++)apps.push({slug:'new-'+i,name:'New reporting gap '+i,status:'running',can_manage:true});
 await page.waitForFunction(()=>document.querySelectorAll('.ov-coverage-row').length>4,{},{timeout:15000});
 assert.equal(await key(),'coverage:forecast:unlimited');
 assert.equal(await page.locator('[data-focus-key="coverage:forecast:unlimited"]').isVisible(),true);
 assert.equal(await page.locator('[data-disclosure-key="coverage-gaps"]').getAttribute('aria-expanded'),'true');
 await page.keyboard.press('Tab');assert.equal(await key(),'coverage-help:forecast:unlimited');
 await page.keyboard.press('Space');
 assert.equal(await page.locator('[data-disclosure-key="coverage-help:forecast:unlimited"]').getAttribute('aria-expanded'),'true');
 results.push('Live poll reveals a focused row pushed beyond the cutoff; keyboard disclosure remains usable');
 apps.splice(3);
 role='operator';await page.goto(origin+'/apps');await page.goto(origin);await ready();
 const inspect=page.locator('[data-focus-key="coverage:forecast:unlimited"]');
 assert.equal(await inspect.textContent(),'Inspect app');assert.equal(await inspect.getAttribute('href'),'/apps/forecast');
 await inspect.click();await page.waitForURL('**/apps/forecast');await page.locator('#app-detail-view').waitFor();
 results.push('Non-managing operator gets accessible app inspection instead of settings');
 assert.deepEqual(errors,[],'No runtime errors across the complete journey');
 assert.deepEqual([...unknown],[],'Every API request has an explicit fixture');
 console.log(JSON.stringify({results,errors,a11y},null,2));if(out) await writeFile(out+'/app-flow-results.json',JSON.stringify({results,errors,a11y},null,2));
}finally{await context.close();await browser.close();server.closeAllConnections();await new Promise(r=>server.close(r));}
