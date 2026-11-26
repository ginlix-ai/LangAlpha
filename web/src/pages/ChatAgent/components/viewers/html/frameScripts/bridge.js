/* eslint-disable no-empty, no-unused-vars -- a call the browser refuses is meant to pass in silence */
// The parent's side of the widget document, called with the prompt data cap
// in bytes: `sendPrompt`, `draftPrompt` and `startThread`, height reports,
// theme pushes and the `widget:inspect` answer. Messages are taken only from
// the parent, since a frame the widget nests could otherwise ask for them. The
// preview document (`widget-stream`) runs it too, for its height reports.
//
// The prompt calls' gate here only explains itself to the widget's author (a
// console warning and a `widget:diag`): code in the widget's realm can replace
// it, so the parent decides from its own side (frameActivation.ts).
//
// Everything from the first line opening `(function` on is the program the
// document carries, byte for byte (frameScripts.ts).
(function(MAX){
  var W=window,embedded=false,unprompted={};
  try{embedded=W.parent!==W;}catch(e){}
  function post(m){if(!embedded)return;try{W.parent.postMessage(m,'*');}catch(e){}}
  // UTF-8, as the parent counts it: a cap in characters lets CJK text through
  // at three times the size.
  function bytes(s){try{return new TextEncoder().encode(s).length;}catch(e){return s.length;}}
  // sendPrompt sends text as the user's next message, draftPrompt leaves it in
  // the message box, startThread opens a new thread with it. Each takes JSON
  // data to attach; the host decides what each does, or does nothing.
  function prompter(name){
    return function(text,data){
      // Only from the user's own click or key press: one on load or on a timer
      // would act for nobody. The parent checks again from its own side.
      var ua=W.navigator&&W.navigator.userActivation;
      if(ua&&!ua.isActive){
        var why=name+' ignored: call it from a click or key press';
        console.warn(why);
        if(!unprompted[name]){unprompted[name]=true;post({type:'widget:diag',kind:'error',message:why});}
        return;
      }
      var m={type:'widget:'+name,text:String(text)};
      if(data!==undefined){
        var s;
        try{s=JSON.stringify(data);}catch(e){}
        if(s===undefined)console.warn(name+': data is not JSON-serializable; sent without it');
        else if(bytes(s)>MAX)console.warn(name+': data is over '+MAX+' bytes serialized; sent without it');
        else m.data=JSON.parse(s);
      }
      post(m);
    };
  }
  W.sendPrompt=prompter('sendPrompt');
  W.draftPrompt=prompter('draftPrompt');
  W.startThread=prompter('startThread');
  var lastH=0,pending=0;
  function measure(){
    var h=document.body.scrollHeight;
    if(h!==W.innerHeight)h+=W.innerHeight-document.documentElement.clientHeight;
    return h;
  }
  function reportHeight(){
    if(!document.body)return;
    var h=measure();
    if(h>0&&Math.abs(h-lastH)>2){
      lastH=h;
      post({type:'widget:resize',height:h});
    }
  }
  function debouncedReport(){
    if(pending)return;
    pending=requestAnimationFrame(function(){pending=0;reportHeight();});
  }
  document.addEventListener('DOMContentLoaded',function(){
    var mo=new MutationObserver(debouncedReport);
    mo.observe(document.body,{childList:true,subtree:true});
    if(typeof ResizeObserver==='function')new ResizeObserver(debouncedReport).observe(document.body);
    reportHeight();
  });
  [100,300,800,2000,5000].forEach(function(ms){setTimeout(reportHeight,ms);});
  // A container width change is a pure reflow, no DOM mutation, so the
  // observers above never see it; track the viewport and body box directly.
  W.addEventListener('resize',debouncedReport);
  function inspect(){
    var b=document.body,de=document.documentElement,all=b?b.getElementsByTagName('*'):[],fixed=0,broken=0,imgs=document.images;
    for(var i=0;i<all.length&&i<5000;i++){if(getComputedStyle(all[i]).position==='fixed')fixed++;}
    for(var j=0;j<imgs.length;j++){var im=imgs[j];if((im.currentSrc||im.getAttribute('src'))&&im.complete&&!im.naturalWidth)broken++;}
    var text=b?(b.innerText||'').trim():'',keys=[];
    try{keys=Object.keys(W.__WIDGET_DATA__||{});}catch(e){}
    return {
      height:b?measure():0,
      width:de.clientWidth,
      scrollWidth:Math.max(de.scrollWidth,b?b.scrollWidth:0),
      textChars:text.length,
      elements:all.length,
      images:{total:imgs.length,broken:broken},
      canvases:document.getElementsByTagName('canvas').length,
      svgs:document.getElementsByTagName('svg').length,
      fixedElements:fixed,
      dataKeys:keys,
      visibleText:text.slice(0,300)
    };
  }
  W.addEventListener('message',function(e){
    if(e.source!==W.parent||!e.data)return;
    if(e.data.type==='widget:themeUpdate'&&e.data.css){
      // Its own element, written whole: a widget or a CSS-in-JS library may put
      // a <style> of its own first.
      var style=document.getElementById('widget-theme');
      if(style){
        style.textContent='\n:root {\n  '+e.data.css+'\n}\n';
        try{W.dispatchEvent(new Event('themechange'));}catch(err){}
      }
    }else if(e.data.type==='widget:inspect'){
      post({type:'widget:inspected',report:inspect()});
    }
  });
})
