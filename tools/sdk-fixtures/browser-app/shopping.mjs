#!/usr/bin/env node
// Actual official SDK traffic from desktop/mobile product detail flows. No custom recorder.
import {build} from 'esbuild';
import {chromium} from 'playwright';
import {createServer} from 'node:http';
import {mkdir,writeFile} from 'node:fs/promises';
import {fileURLToPath} from 'node:url';
const output=new URL('../../../tests/fixtures/replay/',import.meta.url);
const bundle=await build({stdin:{contents:`import * as Sentry from '@sentry/browser';
Sentry.init({dsn:location.origin.replace('://','://public@')+'/1',integrations:[Sentry.replayIntegration()],replaysSessionSampleRate:1,replaysOnErrorSampleRate:1,environment:'shopping-fixture',release:'storefront@1',sendClientReports:false});
window.flushReplay=()=>Sentry.getReplay().flush();`,resolveDir:fileURLToPath(new URL('.',import.meta.url))},bundle:true,write:false,format:'iife',platform:'browser'});
let mode='desktop';const captures=[];
const server=createServer(async(req,res)=>{
 if(req.method==='POST'&&req.url.startsWith('/api/')){
  const chunks=[];for await(const chunk of req)chunks.push(chunk);const bytes=Buffer.concat(chunks);
  if(bytes.includes(Buffer.from('"type":"replay_event"'))){const name=`shopping-${mode}-${captures.filter(c=>c.mode===mode).length}.envelope`;await writeFile(new URL(name,output),bytes);captures.push({mode,name,bytes:bytes.length});}
  res.writeHead(200,{'Content-Type':'application/json'});res.end('{}');return;
 }
 if(req.url==='/app.js'){res.setHeader('Content-Type','text/javascript');res.end(bundle.outputFiles[0].contents);return;}
 if(req.url==='/cart'){res.writeHead(200,{'Content-Type':'application/json'});res.end('{}');return;}
 res.setHeader('Content-Type','text/html');res.end(`<!doctype html><html><head><meta name="viewport" content="width=device-width,initial-scale=1"><title>Product detail fixture</title><style>body{margin:0;font-family:sans-serif}section{min-height:1000px;padding:24px;box-sizing:border-box}#hero{background:#eee}#description{background:#fafafa}#reviews{background:#eaf4f0}#shipping{background:#f5f0ea}button{padding:16px}footer{position:fixed;bottom:0;background:white;width:100%;padding:12px}</style></head><body><section id="hero"><h1>Linen shirt</h1><p>PRIVATE_TEXT_SENTINEL</p><button id="view-reviews" onclick="document.querySelector('#reviews').scrollIntoView()">Reviews</button></section><section id="description"><h2>Product description</h2></section><section id="reviews"><h2>Customer reviews</h2><button id="expand-review" onclick="this.after(Object.assign(document.createElement('p'),{textContent:'Expanded review'}))">Read more</button></section><section id="shipping"><h2>Shipping details</h2></section><footer><button id="add-to-cart" onclick="fetch('/cart').then(()=>history.pushState({},'', '/cart'))">Add to cart</button></footer><input type="password" value="PRIVATE_PASSWORD_SENTINEL"><script src="/app.js"></script></body></html>`);
});
await mkdir(output,{recursive:true});await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
const browser=await chromium.launch({headless:true});
try{
 for(const [device,width,height]of[['desktop',1280,800],['mobile',390,844]]){
  mode=device;const page=await browser.newPage({viewport:{width,height},isMobile:device==='mobile',hasTouch:device==='mobile'});
  await page.goto(`http://127.0.0.1:${server.address().port}/products/linen-shirt?campaign=fixture`);
  await page.waitForFunction(()=>window.flushReplay);
  await page.getByRole('button',{name:'Reviews',exact:true}).click();
  await page.waitForTimeout(600);
  await page.getByRole('button',{name:'Read more',exact:true}).click();
  await page.mouse.wheel(0,700);await page.waitForTimeout(500);
  await page.getByRole('button',{name:'Add to cart',exact:true}).click();
  await page.waitForTimeout(4500);await page.evaluate(()=>window.flushReplay());
  await page.close();
 }
 if(!captures.some(c=>c.mode==='desktop')||!captures.some(c=>c.mode==='mobile'))throw Error('Missing shopping SDK replay');
 await writeFile(new URL('shopping-capture.json',output),JSON.stringify({sdk:'@sentry/browser',version:'10.73.0',captures},null,2)+'\n');console.log(JSON.stringify(captures));
}finally{await browser.close();await new Promise(resolve=>server.close(resolve));}
