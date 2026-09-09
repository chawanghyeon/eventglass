import { readFile, mkdir } from 'node:fs/promises';

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
  await page.locator(`a[href="/replays/${project}/${id}"]`).waitFor();
  await page.getByRole('button',{name:'페이지별 Heatmaps'}).click();
  await page.getByRole('heading',{name:'Page maps',exact:true}).waitFor();
  await page.getByRole('img',{name:/Click heatmap/}).waitFor();
  await page.locator(`a[href="/replays/${project}/${id}"]`).click();
  await page.getByRole('heading',{name:'Timeline',exact:true}).waitFor();
  await page.getByRole('link',{name:/^Error [a-f0-9]{32}$/}).waitFor();
  const outer=page.frameLocator('iframe[title="Session replay 화면"]');
  const screen=outer.frameLocator('iframe');
  await screen.locator('#dead').waitFor({state:'attached',timeout:15000});
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
  await page.screenshot({path:new URL('replay.png',directory).pathname,fullPage:true});
  await checkUntrustedReplay(page.context(),baseUrl,project,parsed.username,bytes);
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
