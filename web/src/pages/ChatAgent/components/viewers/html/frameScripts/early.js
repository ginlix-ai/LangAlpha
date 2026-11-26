/* eslint-disable no-empty, no-unused-vars -- a stand-in that fails, or a call the browser refuses, is meant to pass in silence */
// The widget document's first script, run before any widget code as one
// function called with the storage seed and the base (buildHtmlSrcDoc.ts):
//
// 1. JSON.parse takes NaN/Infinity from Python's json.dumps (not valid JSON).
//    Quoted strings are left untouched: a flat token scan with a per-parse
//    inString flag, so there is no repetition for the regexp engine to
//    backtrack over and a very long string cannot exhaust its stack. Text
//    with neither spelling skips the scan: the patch is global to the frame,
//    so every other parse (chart libraries included) would pay for it.
// 2. Stand-ins for what an opaque origin is refused, as the served-HTML
//    bridge gives a report (`src/server/preview_proxy/bridge/opaque.js`, which
//    `web/` cannot import, so this mirrors it): storage that lasts the
//    document, IndexedDB reading as absent so a library keeps to localStorage,
//    a cookie jar, and a Worker started from a blob of the document's own
//    that loads the real script. Unlike a report's, localStorage writes reach
//    the parent, which keeps them per widget, and a write another frame of the
//    same widget made (the inline one and its fullscreen copy) comes back as
//    `widget:storageSync`, applied without being sent again and announced
//    with a `storage` event, as a write from another tab would be.
// 3. Diagnostics to the parent (`widget:diag`): script errors, console.error,
//    policy refusals and elements that failed to load; script errors also
//    draw the inline overlay with its Fix button.
// 4. Clicks: `data-zoom` asks the parent for its lightbox; a link opens a real
//    tab with noopener, since a plain <a href> would navigate the FRAME, where
//    a site has no cookies and its bot checks fail. A `#fragment` link
//    scrolls to its element here: under the base it resolves to the base's
//    own address, so followed, it would replace the widget. A link under the base
//    opens nothing: it would put the viewer's file grant in an address bar.
//    With `download` it is fetched and saved from a blob, as the bridge saves
//    a report's own file, and a blob or data: download, or a mail or phone
//    hand-off, is left to the browser. Any other scheme would replace the
//    widget in its frame, so it is cancelled.
// 5. A form submit nothing handled would navigate the frame away from the
//    widget to a blank page, so it is cancelled unless it targets a new tab
//    or only closes a dialog (`method="dialog"`).
// 6. Chart.js (v3 and later) follows the theme: `window.Chart` is an accessor,
//    so the UMD build assigning it hands the library over before any chart is
//    made. Its text, grid and font defaults come from the theme variables, and
//    a plugin gives each dataset with no colour of its own `--color-chart-N`
//    (by dataset, or by slice for pie, doughnut and polarArea), in place of the
//    library's own `colors` plugin. On `themechange` the variables are read
//    again, and only what this set (a default the widget has not changed since,
//    a dataset still wearing the colour this gave it) is repainted. An ESM
//    import never assigns the global, so it keeps the library's defaults.
//
// Everything from the first line opening `(function` on is the program the
// document carries, byte for byte (frameScripts.ts), so these notes stay out of
// every widget. ES5 apart from what every browser the app supports runs.
(function(SEED,BASE){
  var W=window,embedded=false;
  try{embedded=W.parent!==W;}catch(e){}
  function post(m){if(!embedded)return;try{W.parent.postMessage(m,'*');}catch(e){}}
  function clip(s){return String(s==null?'':s).slice(0,500);}
  var _p=JSON.parse;
  JSON.parse=function(t,r){
    if(typeof t==='string'&&(t.indexOf('NaN')!==-1||t.indexOf('Infinity')!==-1)){
      var inString=false;
      t=t.replace(/\\[\s\S]|"|(?<!\w)(?:NaN|-?Infinity)\b/g,function(token){
        if(token==='"'){inString=!inString;return token;}
        return inString||token.charAt(0)==='\\'?token:'null';
      });
    }
    return _p.call(this,t,r);
  };
  function standIn(f){try{f();}catch(e){}}
  function memoryStorage(items,initial,persist){
    if(initial)Object.keys(initial).forEach(function(k){items.set(k,String(initial[k]));});
    function put(key,value){key=String(key);value=String(value);items.set(key,value);if(persist)post({type:'widget:storage',key:key,value:value});}
    // A delete or clear is sent even when nothing here changes: the other
    // frame's write of the key can be kept by the parent and still on its way.
    function drop(key){key=String(key);items.delete(key);if(persist)post({type:'widget:storage',key:key,value:null});}
    var methods={
      getItem:function(key){var v=items.get(String(key));return v===undefined?null:v;},
      setItem:put,
      removeItem:drop,
      clear:function(){items.clear();if(persist)post({type:'widget:storage',clear:true});},
      key:function(i){var k=Array.from(items.keys())[i];return k===undefined?null:k;}
    };
    return new Proxy(methods,{
      get:function(t,key){if(key==='length')return items.size;if(key in t)return t[key];return typeof key==='string'?items.get(key):undefined;},
      set:function(t,key,value){put(key,value);return true;},
      has:function(t,key){return key in t||items.has(String(key));},
      deleteProperty:function(t,key){drop(key);return true;},
      ownKeys:function(){return Array.from(items.keys());},
      getOwnPropertyDescriptor:function(t,key){var v=items.get(String(key));return v===undefined?undefined:{value:v,enumerable:true,configurable:true,writable:true};}
    });
  }
  var refused=false;
  try{W.localStorage.getItem('');}catch(e){refused=true;}
  if(refused){
    var kept=new Map(),local=null;
    standIn(function(){local=memoryStorage(kept,SEED,true);Object.defineProperty(W,'localStorage',{configurable:true,value:local});});
    standIn(function(){Object.defineProperty(W,'sessionStorage',{configurable:true,value:memoryStorage(new Map(),null,false)});});
    standIn(function(){Object.defineProperty(W,'indexedDB',{configurable:true,value:undefined});});
    W.addEventListener('message',function(e){
      var d=e.data;
      if(e.source!==W.parent||!d||d.type!=='widget:storageSync'||!d.write)return;
      var w=d.write,key=null,was=null,now=null;
      if(w.clear===true){if(!kept.size)return;kept.clear();}
      else if(typeof w.key==='string'&&(typeof w.value==='string'||w.value===null)){
        key=w.key;was=kept.has(key)?kept.get(key):null;now=w.value;
        if(was===now)return;
        if(now===null)kept.delete(key);else kept.set(key,now);
      }else return;
      try{
        var ev=new StorageEvent('storage',{key:key,oldValue:was,newValue:now});
        // The init dictionary takes only a native Storage.
        try{Object.defineProperty(ev,'storageArea',{value:local});}catch(err){}
        W.dispatchEvent(ev);
      }catch(err){}
    });
  }
  // When a cookie's attributes end it, or null for one that lasts the document.
  function cookieEnd(attrs){
    var maxAge=null,expires=null;
    for(var i=0;i<attrs.length;i++){
      var at=attrs[i].indexOf('='),key=(at<0?attrs[i]:attrs[i].slice(0,at)).trim().toLowerCase(),val=at<0?'':attrs[i].slice(at+1).trim();
      if(key==='max-age'&&/^-?[0-9]+$/.test(val))maxAge=Number(val);
      else if(key==='expires'&&!isNaN(Date.parse(val)))expires=Date.parse(val);
    }
    return maxAge!==null?Date.now()+maxAge*1000:expires;
  }
  function cookiesKept(){
    try{document.cookie='__widget_cookie_check=1';var kept=document.cookie.indexOf('__widget_cookie_check=1')>=0;document.cookie='__widget_cookie_check=; max-age=0';return kept;}catch(e){return false;}
  }
  if(!cookiesKept())standIn(function(){
    var jar=new Map();
    function ended(c){return c.end!==null&&c.end<=Date.now();}
    Object.defineProperty(document,'cookie',{configurable:true,
      get:function(){
        jar.forEach(function(c,name){if(ended(c))jar.delete(name);});
        return Array.from(jar,function(p){return p[0]+'='+p[1].value;}).join('; ');
      },
      set:function(value){
        var parts=String(value).split(';'),at=parts[0].indexOf('='),name=(at<0?'':parts[0].slice(0,at)).trim();
        var c={value:parts[0].slice(at+1).trim(),end:cookieEnd(parts.slice(1))};
        if(ended(c))jar.delete(name);else jar.set(name,c);
      }
    });
  });
  if(W.origin==='null'&&typeof W.Worker==='function')standIn(function(){
    var Native=W.Worker;
    function prelude(script,module){
      function resolve(u){try{return new URL(String(u),script).href;}catch(e){return u;}}
      var f=self.fetch;
      if(f)self.fetch=function(input,init){return f.call(self,typeof input==='string'||input instanceof URL?resolve(input):input,init);};
      var imp=self.importScripts;
      self.importScripts=function(){return imp.apply(self,Array.prototype.map.call(arguments,resolve));};
      if(!module)return imp(script);
      // A message posted before the module has run finds no handler and is
      // lost: hold each one, ahead of any listener, until it has.
      var held=[];
      function hold(e){e.stopImmediatePropagation();held.push(e);}
      self.addEventListener('message',hold);
      import(script).then(function(){
        self.removeEventListener('message',hold);
        held.forEach(function(e){self.dispatchEvent(new MessageEvent('message',{data:e.data,ports:e.ports}));});
      },function(e){self.removeEventListener('message',hold);setTimeout(function(){throw e;});});
    }
    function Worker(script,options){
      var url=new URL(String(script),document.baseURI);
      if(url.protocol==='blob:'||url.protocol==='data:')return new Native(script,options);
      var o=Object.assign({},options),module=o.type==='module';
      o.type='classic';
      var source='('+String(prelude)+')('+JSON.stringify(url.href)+','+module+');';
      // The worker holds its script from construction, so the URL can go.
      var blob=URL.createObjectURL(new Blob([source],{type:'text/javascript'}));
      try{return new Native(blob,o);}finally{URL.revokeObjectURL(blob);}
    }
    Worker.prototype=Native.prototype;
    W.Worker=Worker;
  });
  var seen={},sent=0;
  function diag(m,key){if(seen[key]||sent>=20)return;seen[key]=1;sent++;m.type='widget:diag';post(m);}
  function scriptError(message,source,line){
    message=clip(message);
    if(!message)return;
    var m={kind:'error',message:message};
    if(source)m.source=clip(source);
    if(typeof line==='number'&&line>0)m.line=line;
    diag(m,'e'+message);
  }
  var shown={},count=0,rendering=false;
  function showError(msg){
    if(rendering||shown[msg])return;
    shown[msg]=1;
    if(++count>5)return;
    rendering=true;
    msg=clip(msg);
    var root=document.body||document.documentElement;
    var d=document.createElement('div');
    d.style.cssText='margin:12px;padding:14px 16px;background:var(--color-bg-card);border:0.5px solid var(--color-border-muted);border-radius:10px;display:flex;align-items:center;gap:12px;';
    var dot=document.createElement('span');
    dot.style.cssText='flex-shrink:0;width:6px;height:6px;border-radius:50%;background:var(--color-loss);';
    var mid=document.createElement('div');
    mid.style.cssText='flex:1;min-width:0;';
    var detail=document.createElement('div');
    detail.style.cssText='font:13px/1.4 ui-monospace,SFMono-Regular,monospace;color:var(--color-text-muted);white-space:pre-wrap;word-break:break-word;';
    detail.textContent=msg;
    mid.appendChild(detail);
    var b=document.createElement('button');
    b.textContent='Fix';
    b.style.cssText='flex-shrink:0;padding:6px 14px;border-radius:8px;border:0.5px solid var(--color-border-default);background:var(--color-bg-elevated);color:var(--color-text-primary);font:500 13px/1 -apple-system,sans-serif;cursor:pointer;transition:background 0.15s;';
    b.onmouseover=function(){b.style.background='var(--color-bg-hover)';};
    b.onmouseout=function(){b.style.background='var(--color-bg-elevated)';};
    b.onclick=function(){window.sendPrompt('The widget threw an error: '+msg+'. Please fix the widget code and call ShowWidget again.');b.textContent='Sent';b.disabled=true;b.style.opacity='0.4';b.style.cursor='default';};
    d.appendChild(dot);d.appendChild(mid);d.appendChild(b);
    root.appendChild(d);
    post({type:'widget:resize',height:root.scrollHeight});
    rendering=false;
  }
  var LOADS=/^(IMG|SCRIPT|LINK|VIDEO|AUDIO|SOURCE|IFRAME|EMBED|OBJECT)$/;
  W.addEventListener('error',function(e){
    var t=e.target;
    if(t&&t!==W&&t.tagName){
      if(!LOADS.test(t.tagName))return;
      var url=clip(t.currentSrc||t.src||t.href||t.data||'');
      diag({kind:'load-failed',tag:t.tagName.toLowerCase(),url:url},'l'+t.tagName+url);
      return;
    }
    var msg=e.error&&e.error.message||e.message||'Script error';
    scriptError(msg,e.filename,e.lineno);
    showError(msg);
  },true);
  W.addEventListener('unhandledrejection',function(e){
    var r=e.reason,msg=r&&r.message||String(r);
    scriptError('Unhandled rejection: '+msg);
    showError(msg);
  });
  if(typeof console!=='undefined'&&console.error){
    var consoleError=console.error;
    console.error=function(){
      try{scriptError(Array.prototype.map.call(arguments,function(a){
        if(a&&a.message)return a.message;
        if(a&&typeof a==='object'){try{return JSON.stringify(a);}catch(err){}}
        return String(a);
      }).join(' '));}catch(err){}
      return consoleError.apply(console,arguments);
    };
  }
  document.addEventListener('securitypolicyviolation',function(e){
    var d=e.effectiveDirective||e.violatedDirective,url=clip(e.blockedURI);
    diag({kind:'blocked',directive:d,url:url},'b'+d+url);
  });
  function saveFile(a){
    var name=a.getAttribute('download');
    if(!name){try{name=decodeURIComponent(new URL(a.href).pathname.split('/').pop()||'');}catch(err){name='';}}
    fetch(a.href).then(function(r){if(!r.ok)throw new Error(String(r.status));return r.blob();}).then(function(blob){
      var s=document.createElement('a');s.href=URL.createObjectURL(blob);s.download=name||'download';
      document.body.appendChild(s);s.click();s.remove();
      setTimeout(function(){URL.revokeObjectURL(s.href);},60000);
    }).catch(function(err){diag({kind:'load-failed',tag:'a',url:clip(a.getAttribute('href'))},'la'+a.href);});
  }
  document.addEventListener('click',function(e){
    if(e.defaultPrevented)return;
    var t=e.target,z=embedded&&t&&t.closest?t.closest('[data-zoom]'):null;
    if(z){
      var v=z.getAttribute('data-zoom'),src='';
      try{src=v?new URL(v,document.baseURI).href:(z.currentSrc||z.src||'');}catch(err){}
      if(src){
        e.preventDefault();
        var m={type:'widget:zoom',src:src},alt=z.getAttribute('alt')||z.getAttribute('title');
        if(alt)m.alt=clip(alt);
        post(m);
        return;
      }
    }
    var a=t&&t.closest?t.closest('a[href]'):null;
    if(!a)return;
    var href=a.getAttribute('href')||'';
    if(href.charAt(0)==='#'){
      e.preventDefault();
      var id,el=null;
      try{id=decodeURIComponent(href.slice(1));}catch(err){id=href.slice(1);}
      if(id)el=document.getElementById(id)||document.getElementsByName(id)[0]||null;
      if(el)el.scrollIntoView();
      else if(!id||id.toLowerCase()==='top')W.scrollTo(0,0);
      return;
    }
    var save=a.hasAttribute('download');
    if(save&&/^(blob|data):/i.test(a.href))return;
    if(/^(mailto|tel):/i.test(a.href))return;
    if(!/^https?:/i.test(a.href)){e.preventDefault();return;}
    if(BASE&&a.href.indexOf(BASE)===0){e.preventDefault();if(save)saveFile(a);return;}
    e.preventDefault();
    window.open(a.href,'_blank','noopener,noreferrer');
  });
  // A middle click opens a link in a new tab without a click event: only a
  // link to another site may, as a click on one opens it anyway.
  document.addEventListener('auxclick',function(e){
    var a=e.target&&e.target.closest?e.target.closest('a[href]'):null;
    if(a&&(!/^https?:/i.test(a.href)||(BASE&&a.href.indexOf(BASE)===0)))e.preventDefault();
  },true);
  W.addEventListener('submit',function(e){
    if(e.defaultPrevented)return;
    var f=e.target,s=e.submitter;
    // A dialog-method submit only closes its dialog. Attributes, not the
    // properties a field named `method` would shadow.
    var method=s&&s.hasAttribute&&s.hasAttribute('formmethod')?s.getAttribute('formmethod'):f&&f.getAttribute&&f.getAttribute('method');
    if(/^dialog$/i.test(method||''))return;
    var target=(s&&s.hasAttribute&&s.hasAttribute('formtarget')?s.getAttribute('formtarget'):f&&f.getAttribute&&f.getAttribute('target'))||'';
    if(!/^_blank$/i.test(target)){e.preventDefault();return;}
    // A new tab at a workspace file would show the grant in its address bar,
    // as a link under the base would.
    var action=s&&s.hasAttribute&&s.hasAttribute('formaction')?s.getAttribute('formaction'):f&&f.getAttribute&&f.getAttribute('action');
    if(BASE&&action){
      var to='';
      try{to=new URL(action,document.baseURI).href;}catch(err){}
      if(!/^https?:/i.test(to)||to.indexOf(BASE)===0)e.preventDefault();
    }
  });
  if(!('Chart' in W)&&typeof WeakMap==='function')standIn(function(){
    var lib,themed=new WeakMap(),painted=new WeakMap(),pal=null;
    var SLICES={pie:1,doughnut:1,polarArea:1};
    function cssVar(n){try{return getComputedStyle(document.documentElement).getPropertyValue(n).trim();}catch(e){return '';}}
    function palette(){
      if(pal)return pal;
      pal=[];
      for(var i=1;i<=8;i++){var v=cssVar('--color-chart-'+i);if(v)pal.push(v);}
      return pal;
    }
    function tint(c,alpha){return /^#[0-9a-f]{6}$/i.test(c)?c+alpha:c;}
    function setDefault(obj,key,value,mark){
      if(!obj||!value)return;
      var was=themed.get(obj)||{};
      if(key in was&&obj[key]!==was[key])return;
      if(!(key in was)&&mark&&obj[key]!==mark)return;
      obj[key]=value;was[key]=value;themed.set(obj,was);
    }
    function applyDefaults(C){
      var d=C.defaults;
      setDefault(d,'color',cssVar('--color-text-secondary'));
      setDefault(d,'borderColor',cssVar('--color-border-subtle'));
      var font='';
      try{if(document.body)font=getComputedStyle(document.body).fontFamily;}catch(e){}
      setDefault(d.font,'family',font);
      setDefault(d.font,'size',12);
      var arc=d.elements&&d.elements.arc;
      setDefault(arc,'borderColor',cssVar('--color-bg-page'),'#fff');
    }
    function own(v){return v!==undefined&&v!==null&&v!=='';}
    function paint(chart){
      var cfg=chart.config,data=cfg&&cfg.data,o=cfg&&cfg.options||{},p=palette();
      if(!data||!data.datasets||!p.length)return;
      if(own(o.borderColor)||own(o.backgroundColor))return;
      data.datasets.forEach(function(ds,i){
        if(!ds||typeof ds!=='object')return;
        var was=painted.get(ds);
        if(was&&(ds.borderColor!==was.b||ds.backgroundColor!==was.g))return;
        if(!was&&(own(ds.borderColor)||own(ds.backgroundColor)))return;
        var type=ds.type||cfg.type,b,g;
        if(SLICES[type]){
          var n=(ds.data&&ds.data.length)||0,list=[];
          for(var j=0;j<n;j++)list.push(type==='polarArea'?tint(p[j%p.length],'b3'):p[j%p.length]);
          g=list;b=was?ds.borderColor:undefined;
          if(was&&was.g&&was.g.length===list.length&&was.g.every(function(c,k){return c===list[k];}))return;
        }else{
          var c=p[i%p.length];
          b=c;g=(type==='line'||type==='radar')&&ds.fill?tint(c,'33'):c;
          if(was&&was.b===b&&was.g===g)return;
        }
        if(b!==undefined)ds.borderColor=b;
        ds.backgroundColor=g;painted.set(ds,{b:b,g:g});
      });
    }
    function hook(C){
      if(!C||typeof C.register!=='function'||!C.defaults||!C.defaults.font||themed.has(C))return;
      themed.set(C,{});
      try{if(C.defaults.plugins&&C.defaults.plugins.colors)C.defaults.plugins.colors.enabled=false;}catch(e){}
      applyDefaults(C);
      C.register({id:'widgetTheme',beforeLayout:function(chart){try{paint(chart);}catch(e){}}});
    }
    Object.defineProperty(W,'Chart',{configurable:true,enumerable:true,
      get:function(){return lib;},
      set:function(v){lib=v;try{hook(v);}catch(e){}}
    });
    function recolor(o,swap,depth){
      if(!o||typeof o!=='object'||depth>3)return;
      Object.keys(o).forEach(function(k){
        var v=o[k];
        if(k==='color'&&typeof v==='string'&&Object.prototype.hasOwnProperty.call(swap,v))o[k]=swap[v];
        else if(v&&typeof v==='object'&&!Array.isArray(v))recolor(v,swap,depth+1);
      });
    }
    W.addEventListener('themechange',function(){
      pal=null;
      try{
        if(!lib||!themed.has(lib))return;
        var d=lib.defaults,was={color:d.color,border:d.borderColor},swap={};
        applyDefaults(lib);
        if(typeof was.color==='string'&&was.color!==d.color)swap[was.color]=d.color;
        if(typeof was.border==='string'&&was.border!==d.borderColor)swap[was.border]=d.borderColor;
        var all=lib.instances||{};
        Object.keys(all).forEach(function(k){try{
          var c=all[k],cfg=c.config;
          // A chart copies the scale defaults into its own options when it is
          // made, so a scale colour that is still the old default is moved here.
          if(cfg&&cfg.options)recolor(cfg.options.scales,swap,0);
          if(cfg&&typeof cfg.clearCache==='function')cfg.clearCache();
          c.update();
        }catch(e){}});
      }catch(e){}
    });
  });
})
