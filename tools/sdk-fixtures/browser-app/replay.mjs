#!/usr/bin/env node
// Test harness only. The fixture page records exclusively through the official SDK.
import { build } from 'esbuild';
import { chromium } from 'playwright';
import { createServer } from 'node:http';
import { mkdir, writeFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';

const output = new URL('../../../tests/fixtures/replay/', import.meta.url);
await mkdir(output, { recursive: true });
const bundle = await build({ stdin: { contents: `
import * as Sentry from '@sentry/browser';
Sentry.init({ dsn: location.origin.replace('://', '://public@')+'/1',
 integrations: [Sentry.replayIntegration({useCompression: new URLSearchParams(location.search).get('compressed')==='true'}), Sentry.browserTracingIntegration()],
 replaysSessionSampleRate:1, replaysOnErrorSampleRate:1, tracesSampleRate:1,
 environment:'replay-fixture', release:'replay-fixture@1', sendClientReports:false });
Sentry.setUser({id:'replay-fixture-user'});
window.fixture = { flush:()=>Sentry.getReplay().flush(), error:()=>Sentry.captureException(new Error('Replay fixture error')),
 feedback:()=>Sentry.captureFeedback({message:'Replay fixture feedback',name:'Fixture',email:'fixture@example.invalid'},{includeReplay:true}) };
`, resolveDir: fileURLToPath(new URL('.', import.meta.url)) }, bundle: true, write: false, format: 'iife', platform: 'browser' });
let mode = 'plain';
const captured = [];
const server = createServer(async (req, res) => {
 if (req.method === 'POST') {
  const chunks=[]; for await (const c of req) chunks.push(c);
  const bytes=Buffer.concat(chunks);
  if (bytes.includes(Buffer.from('"type":"replay_event"'))) {
   const name=`${mode}-${captured.filter(x=>x.mode===mode).length}.envelope`;
   await writeFile(new URL(name,output),bytes); captured.push({mode,name,bytes:bytes.length});
  } else if (bytes.includes(Buffer.from('"type":"feedback"'))) await writeFile(new URL('feedback.envelope',output),bytes);
  res.writeHead(200,{'Content-Type':'application/json'}); res.end('{}'); return;
 }
 if (req.url==='/app.js') {res.setHeader('Content-Type','text/javascript');res.end(bundle.outputFiles[0].contents);return;}
 if (req.url==='/orders') {res.setHeader('Content-Type','application/json');res.end('{}');return;}
 res.setHeader('Content-Type','text/html');res.end(`<!doctype html><html><head><title>Replay fixture</title></head><body style="margin:0;height:3000px"><button id="dead">Wait</button><button id="save" onclick="fetch('/orders');history.pushState({},'', '/checkout');document.querySelector('#status').textContent='Saved'">Save</button><div id="status">PRIVATE_TEXT_SENTINEL</div><input type="password" value="PRIVATE_PASSWORD_SENTINEL"><div class="sentry-block">BLOCKED_SENTINEL</div><script src="/app.js"></script></body></html>`);
});
await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
const browser=await chromium.launch({headless:true});
try {
 for (const compressed of [false,true]) {
  mode=compressed?'compressed':'plain';
  const page=await browser.newPage({viewport:{width:1000,height:600}});
  await page.goto(`http://127.0.0.1:${server.address().port}/?compressed=${compressed}`);
  await page.waitForFunction(()=>window.fixture);
  await page.mouse.move(100,100);await page.mouse.move(500,250,{steps:12});
  await page.locator('#dead').click({clickCount:4,delay:80});
  await page.waitForTimeout(8200);
  await page.evaluate(()=>window.fixture.flush());
  await page.locator('#save').click();
  await page.mouse.wheel(0,1200);await page.waitForTimeout(700);
  await page.setViewportSize({width:900,height:650});
  await page.evaluate(()=>{console.warn('Replay fixture console');window.fixture.error();window.fixture.feedback();});
  await page.waitForTimeout(1200);
  await page.evaluate(()=>window.fixture.flush());
  await page.close();
 }
 if(!captured.some(x=>x.mode==='plain')||!captured.some(x=>x.mode==='compressed')) throw Error('No Replay captured');
 await writeFile(new URL('capture.json',output),JSON.stringify({sdk:'@sentry/browser',version:'10.73.0',captured},null,2)+'\n');
 console.log(JSON.stringify(captured));
} finally {await browser.close();await new Promise(resolve=>server.close(resolve));}
