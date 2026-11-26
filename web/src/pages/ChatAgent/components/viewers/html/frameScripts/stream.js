/* eslint-disable no-useless-escape */
// The preview's receiver. The parent posts `{type:'widget:streamHtml', html}`
// with all the markup written so far; this draws it into the body without
// running any of it:
//
// 1. A trailing tag still being written is cut, and the rest parsed inert in a
//    <template>, where nothing loads.
// 2. What could run or leave is dropped: scripts (complete or not), frames,
//    embeds, meta refreshes, a second <base>, inline handlers and javascript:
//    URLs. The document's policy refuses any script without its nonce too,
//    handlers included, so this is the second of two locks.
// 3. The body is patched to match rather than replaced: a node of the same
//    kind in the same place keeps its identity, with text and attributes
//    changed in place, so an image already loaded or a stylesheet already
//    applied is never fetched or applied again as the markup grows.
//
// Clicks, middle clicks and submits do nothing: the preview is replaced the moment the call
// completes, and a link followed here would navigate the frame.
//
// Everything from the first line opening `(function` on is the program the
// document carries, byte for byte (frameScripts.ts).
(function(){
  var W=window,D=document;
  var DROP='script,iframe,frame,frameset,object,embed,applet,portal,meta,base,noscript';
  var URL_ATTR=/^(href|src|action|formaction|data|poster|xlink:href)$/i;
  function cut(h){
    var lt=h.lastIndexOf('<');
    if(lt<0||h.indexOf('>',lt)>=0)return h;
    return lt===h.length-1||/^<[a-zA-Z!\/]/.test(h.slice(lt,lt+2))?h.slice(0,lt):h;
  }
  function clean(root){
    var bad=root.querySelectorAll(DROP),i,j;
    for(i=0;i<bad.length;i++)if(bad[i].parentNode)bad[i].parentNode.removeChild(bad[i]);
    var all=root.querySelectorAll('*');
    for(i=0;i<all.length;i++){
      var el=all[i],attrs=el.attributes;
      for(j=attrs.length-1;j>=0;j--){
        var a=attrs[j];
        if(/^on/i.test(a.name)||(URL_ATTR.test(a.name)&&/^\s*javascript:/i.test(a.value)))el.removeAttributeNode(a);
      }
    }
  }
  function same(a,b){
    return a.nodeType===b.nodeType&&a.nodeName===b.nodeName&&(a.nodeType!==1||a.namespaceURI===b.namespaceURI);
  }
  function attrs(to,from){
    var i,a;
    for(i=to.attributes.length-1;i>=0;i--){a=to.attributes[i];if(!from.hasAttributeNS(a.namespaceURI,a.localName))to.removeAttributeNode(a);}
    for(i=0;i<from.attributes.length;i++){a=from.attributes[i];if(to.getAttributeNS(a.namespaceURI,a.localName)!==a.value)to.setAttributeNS(a.namespaceURI,a.name,a.value);}
  }
  function morph(to,from){
    var t=to.firstChild,f=from.firstChild,next;
    while(f){
      next=f.nextSibling;
      if(!t){to.appendChild(f);}
      else if(same(t,f)&&!(t.nodeName==='TEMPLATE'&&t.innerHTML!==f.innerHTML)){
        if(t.nodeType===1){attrs(t,f);morph(t,f);}
        else if(t.nodeValue!==f.nodeValue)t.nodeValue=f.nodeValue;
        t=t.nextSibling;
      }else{to.replaceChild(f,t);t=f.nextSibling;}
      f=next;
    }
    while(t){next=t.nextSibling;to.removeChild(t);t=next;}
  }
  function draw(html){
    if(!D.body)return;
    var tpl=D.createElement('template');
    tpl.innerHTML=cut(String(html));
    clean(tpl.content);
    morph(D.body,tpl.content);
  }
  W.addEventListener('message',function(e){
    if(e.source!==W.parent||!e.data||e.data.type!=='widget:streamHtml')return;
    draw(e.data.html);
  });
  D.addEventListener('click',function(e){e.preventDefault();},true);
  D.addEventListener('submit',function(e){e.preventDefault();},true);
  D.addEventListener('auxclick',function(e){e.preventDefault();},true);
})
