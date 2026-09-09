import { readFile, mkdir } from 'node:fs/promises';
import { inflateSync } from 'node:zlib';

export async function checkReplayUi(page,baseUrl,dsn) {
  const parsed=new URL(dsn);const project=parsed.pathname.slice(1);
  const root=new URL('../../../tests/fixtures/replay/',import.meta.url);
  for(const name of ['plain-2.envelope','plain-0.envelope','plain-2.envelope','plain-1.envelope','compressed-0.envelope','compressed-1.envelope','compressed-2.envelope','feedback.envelope','plain-error.envelope','compressed-error.envelope']) {
    const response=await fetch(`${baseUrl}/api/${project}/envelope/?sentry_key=${parsed.username}`,{method:'POST',body:await readFile(new URL(name,root))});
    if(response.status!==202)throw Error(`Replay fixture HTTP ${response.status}: ${await response.text()}`);
  }
  const bytes=await readFile(new URL('plain-0.envelope',root),'utf8');
  const id=JSON.parse(bytes.split('\n')[2]).replay_id;
  await page.goto(`${baseUrl}/replays?project=${project}`);
  await page.getByRole('button',{name:'Sentry User Feedback',exact:true}).click();
  await page.getByText('Replay fixture feedback',{exact:true}).waitFor();
  await page.locator(`a[href="/replays/${project}/${id}"]`).first().waitFor();
  await page.getByRole('button',{name:'페이지별 Heatmaps'}).click();
  await page.getByRole('heading',{name:'Page maps',exact:true}).waitFor();
  await page.getByRole('img',{name:/Click heatmap/}).waitFor();
  await page.locator(`a[href="/replays/${project}/${id}"]`).first().click();
  await page.getByRole('heading',{name:'Timeline',exact:true}).waitFor();
  await page.getByRole('link',{name:/^Error [a-f0-9]{32}$/}).waitFor();
  const outer=page.frameLocator('iframe[title="Session replay 화면"]');
  const screen=outer.frameLocator('iframe');
  await screen.locator('#dead').waitFor({state:'visible',timeout:15000});
  if(!await screen.locator('#dead').isVisible())throw Error('Initial replay snapshot is blank');
  const sandbox=await page.locator('iframe[title="Session replay 화면"]').getAttribute('sandbox');
  if(sandbox!=='allow-same-origin')throw Error('Replay iframe permits scripts');
  const html=await screen.locator('body').innerHTML();
  if(html.includes('PRIVATE_TEXT_SENTINEL')||html.includes('PRIVATE_PASSWORD_SENTINEL')||html.includes('BLOCKED_SENTINEL')) throw Error('Replay masking was lost');
  await page.getByRole('button',{name:'Play',exact:true}).click();
  await page.getByRole('button',{name:'Pause',exact:true}).click();
  await page.getByLabel('Speed',{exact:true}).selectOption('2');
  await page.getByLabel('Replay seek',{exact:true}).focus();
  await page.getByLabel('Replay seek',{exact:true}).press('End');
  await page.getByRole('heading',{name:'Element clicks',exact:true}).waitFor();
  await page.getByText(/^(dead|rage) click$/).waitFor();
  await page.getByLabel('Map',{exact:true}).selectOption('movement');
  await page.getByRole('img',{name:/Movement heatmap/}).waitFor();
  const directory=new URL('../../../.tools/screenshots/',import.meta.url);await mkdir(directory,{recursive:true});
  await page.getByLabel('Replay seek',{exact:true}).press('Home');
  await screen.locator('#dead').waitFor({state:'visible'});
  await page.screenshot({path:new URL('replay.png',directory).pathname,fullPage:true});
  await checkUntrustedReplay(page.context(),baseUrl,project,parsed.username,bytes);
  await checkShopping(page,baseUrl,project,parsed.username,root);
  console.log('Replay UI: official SDK ingest, duplicate/order, page maps, isolated DOM playback, masking, controls, timeline passed');
}

// Supplemental adversarial variant of an actual SDK snapshot, not a protocol fixture.
async function checkUntrustedReplay(context,baseUrl,project,key,plain) {
  const lines=plain.split('\n');const metadata=JSON.parse(lines[2]);
  metadata.replay_id='eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee';metadata.event_id=metadata.replay_id;
  const events=JSON.parse(lines.slice(5).join('\n'));
  const snapshot=events.find(event=>event.type===2);
  function find(node,tag){if(node.tagName===tag)return node;for(const child of node.childNodes??[]){const result=find(child,tag);if(result)return result;}}
  const body=find(snapshot.data.node,'body');
  let next=90000;const probe=`${baseUrl}/replay-security-probe`;
  const node=(tagName,attributes,childNodes=[])=>({type:2,id:next++,tagName,attributes,childNodes});
  body.childNodes.push(
    node('img',{src:probe+'/image',onerror:'top.__replayEscaped=true'}),
    node('canvas',{rr_dataURL:probe+'/canvas'}),
    node('iframe',{src:probe+'/frame',srcdoc:'<script>top.__replayEscaped=true</script>'}),
    node('script',{},[{type:3,id:next++,textContent:'top.__replayEscaped=true'}]),
    node('style',{},[{type:3,id:next++,textContent:`body{background-image:url(${probe}/css)}`}]),
  );
  const recording=Buffer.from('{"segment_id":0}\n'+JSON.stringify(events));
  const envelope=Buffer.concat([Buffer.from('{}\n{"type":"replay_event"}\n'+JSON.stringify(metadata)+'\n'+JSON.stringify({type:'replay_recording',length:recording.length})+'\n'),recording]);
  const response=await fetch(`${baseUrl}/api/${project}/envelope/?sentry_key=${key}`,{method:'POST',body:envelope});
  if(response.status!==202)throw Error('Adversarial Replay ingest failed');
  const isolatedContext=await context.browser().newContext({storageState:await context.storageState()});
  const isolated=await isolatedContext.newPage();let requests=0;
  await isolated.route('**/replay-security-probe/**',route=>{requests++;return route.abort();});
  try {
    await isolated.goto(`${baseUrl}/replays/${project}/${metadata.replay_id}`);
    const screen=isolated.frameLocator('iframe[title="Session replay 화면"]').frameLocator('iframe');
    await screen.locator('#dead').waitFor({state:'attached'});
    await isolated.getByLabel('Replay seek',{exact:true}).press('End');
    await isolated.getByLabel('Replay seek',{exact:true}).press('Home');
    // Give blocked resource callbacks a chance to run after both snapshot and seek.
    await isolated.waitForTimeout(300);
    if(requests!==0 || await isolated.evaluate(()=>Boolean(window.__replayEscaped)))throw Error(`Replay escaped sandbox: ${requests} requests`);
  } finally {await isolatedContext.close();}
}

async function checkShopping(page,baseUrl,project,key,root) {
  const capture=JSON.parse(await readFile(new URL('shopping-capture.json',root),'utf8'));
  let replayId,productUrl;
  const starts=new Map();
  for(const item of capture.captures){
    const bytes=await readFile(new URL(item.name,root));
    const metadata=JSON.parse(bytes.toString('utf8').split('\n')[2]);
    replayId??=metadata.replay_id;
    const length=JSON.parse(bytes.toString('utf8').split('\n')[3]).length;
    const recording=bytes.subarray(bytes.length-length);
    const events=JSON.parse(inflateSync(recording.subarray(recording.indexOf(10)+1)));
    starts.set(metadata.replay_id,Math.min(...events.map(event=>event.data?.tag==='performanceSpan'?Math.trunc(event.data.payload.startTimestamp*1000):event.timestamp)));
    const url=new URL(metadata.urls.find(url=>url.includes('/products/')));url.search='';productUrl=url.toString();
    const response=await fetch(`${baseUrl}/api/${project}/envelope/?sentry_key=${key}`,{method:'POST',body:bytes});
    if(response.status!==202)throw Error(`Shopping replay ingest ${response.status}`);
  }
  await page.goto(`${baseUrl}/replays?project=${project}&environment=shopping-fixture&url=${encodeURIComponent('/products/linen-shirt')}`);
  await page.getByRole('button',{name:'페이지별 Heatmaps'}).click();
  await page.getByRole('heading',{name:'상품·페이지 분석',exact:true}).waitFor();
  await page.getByRole('button',{name:productUrl,exact:true}).first().click();
  await page.locator('.replay-page-summary > div').first().getByText('2',{exact:true}).waitFor();
  await page.getByText('2–3 화면 높이',{exact:true}).waitFor();
  await page.getByText(/#expand-review/).first().waitFor();
  await page.getByText(/#add-to-cart/).first().waitFor();
  await page.getByRole('heading',{name:'다음 관측 페이지',exact:true}).locator('..').getByRole('button',{name:/\/cart$/}).waitFor();
  await page.screenshot({path:new URL('../../../.tools/screenshots/shopping-analysis.png',import.meta.url).pathname,fullPage:true});
  await page.getByRole('combobox',{name:'분석 화면 크기'}).selectOption('narrow');
  await page.locator('.replay-page-summary > div').first().getByText('1',{exact:true}).waitFor();
  await page.getByRole('button',{name:productUrl,exact:true}).first().click();
  const target=page.getByRole('link',{name:/#expand-review/}).first();
  const href=await target.getAttribute('href');
  if(!href?.includes('?t='))throw Error('Missing exact replay timestamp link');
  const targetUrl=new URL(href,baseUrl);
  const expected=Number(targetUrl.searchParams.get('t'))-starts.get(targetUrl.pathname.split('/').at(-1));
  if(!(expected>0))throw Error('Invalid real SDK sample offset');
  await target.click();
  await page.getByRole('slider',{name:'Replay seek'}).waitFor();
  await page.waitForFunction(expected=>Math.abs(Number(document.querySelector('input[aria-label="Replay seek"]')?.value)-expected)<2,expected);
  await page.reload();
  await page.waitForFunction(expected=>Math.abs(Number(document.querySelector('input[aria-label="Replay seek"]')?.value)-expected)<2,expected);
  await page.goto(`${baseUrl}/replays?project=${project}&environment=shopping-fixture`);
  await page.getByRole('button',{name:'페이지별 Heatmaps'}).click();
  await page.getByRole('heading',{name:'상품·페이지 분석',exact:true}).waitFor();
  await page.setViewportSize({width:390,height:844});
  const overflow=await page.evaluate(()=>document.documentElement.scrollWidth>window.innerWidth+1);
  if(overflow){await page.screenshot({path:new URL('../../../.tools/screenshots/shopping-mobile-overflow.png',import.meta.url).pathname,fullPage:true});const wide=await page.evaluate(()=>[...document.querySelectorAll('body *')].map(e=>({tag:e.tagName,class:e.className,width:e.getBoundingClientRect().width,right:e.getBoundingClientRect().right})).filter(e=>e.right>window.innerWidth+1).slice(0,20));throw Error('Shopping analysis overflows narrow admin viewport: '+JSON.stringify(wide));}
  await page.setViewportSize({width:1280,height:720});
  await page.goto(`${baseUrl}/replays/${project}/${replayId}`);
  const screen=page.frameLocator('iframe[title="Session replay 화면"]').frameLocator('iframe');
  await screen.locator('#view-reviews').waitFor({state:'visible'});
  await page.screenshot({path:new URL('../../../.tools/screenshots/shopping-replay.png',import.meta.url).pathname,fullPage:true});
  console.log('Shopping: actual desktop/mobile SDK captures, page/time/scroll-band/selector/journey summaries, related replay and responsive admin UI passed');
}
