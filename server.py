import os, json, secrets, csv, io
from collections import defaultdict
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse

app=FastAPI()
TEACHER=os.getenv("TEACHER_CODE") or secrets.token_urlsafe(12)
CLASS=os.getenv("CLASS_CODE") or secrets.token_urlsafe(6)
NAMES=["ABDULLAYEV IN'OMJON","AZIMBAYEV FURQAT","BAXITOV AYBEK","BOLTAYEV ERGASH","EGAMOV ALISHER","ISMOILOV DIYDORBEK","KALANDAROV NOSIR","MURATOV TOXIRJON","RAJABOV NAVRO‘ZBEK","RAXIMOV RUSTAMBEK","SAFOYEV SIROJBEK","SAIDOV SHOHRUH","SHARIPOV UMRBEK","TURIMBETOVA SARBINAZ","XALILLAYEV SHAHRIYOR","YUREV MUNISBEK"]
TASKS=[
"ρ25=831 kg/m³, γ=0.725 kg/(m³·°C). ρ15 ni hisoblang.",
"ρ30=826 kg/m³, γ=0.70 kg/(m³·°C). ρ20 ni toping.",
"V=12 m³, ρ=835 kg/m³. Massani toping.",
"m=25050 kg, ρ=835 kg/m³. Hajmni toping.",
"6000 L: 845 kg/m³; 4000 L: 825 kg/m³. Aralashma zichligini toping.",
"B7: 93% dizel (835 kg/m³), 7% FAME (883 kg/m³). Aralashma zichligini toping."
]
ANS=[838.25,833,10020,30,837,838.36]
board=[{"strokes":[],"teacher_strokes":[],"task":None,"answer":"","grade":None,"comment":"","online":False} for _ in NAMES]
clients=defaultdict(set); student_sockets=defaultdict(set); global_task=0
async def send(ws,obj):
 try: await ws.send_json(obj)
 except Exception: pass
async def broadcast(obj,role=None):
 for ws in list(clients["teacher"] if role=="teacher" else clients["teacher"]|clients["student"]):
  await send(ws,obj)
@app.get("/")
async def home():
 from pathlib import Path
 return HTMLResponse(Path(__file__).with_name("index.html").read_text(encoding="utf8"))
@app.get("/health")
async def health(): return {"ok":True}
@app.get("/export")
async def export(code:str):
 if not secrets.compare_digest(code,TEACHER): raise HTTPException(403)
 s=io.StringIO(); w=csv.writer(s);w.writerow(["Talaba","Baho","Javob","Izoh"])
 for i,n in enumerate(NAMES):w.writerow([n,board[i]["grade"],board[i]["answer"],board[i]["comment"]])
 return StreamingResponse(iter(["\ufeff"+s.getvalue()]),media_type="text/csv",headers={"Content-Disposition":"attachment; filename=results.csv"})
@app.websocket("/ws")
async def ws_endpoint(ws:WebSocket):
 global global_task
 await ws.accept();role=None;idx=None
 try:
  auth=await ws.receive_json();role=auth.get("role");code=str(auth.get("code",""))
  if role=="teacher" and secrets.compare_digest(code,TEACHER):pass
  elif role=="student" and secrets.compare_digest(code,CLASS):
   idx=int(auth.get("student",-1))
   if idx<0 or idx>=len(NAMES):await ws.close();return
  else:await ws.send_json({"type":"error","message":"Kod noto‘g‘ri"});await ws.close();return
  clients[role].add(ws)
  if role=="student":
   student_sockets[idx].add(ws);board[idx]["online"]=True
  if role=="teacher": await send(ws,{"type":"init","role":role,"names":NAMES,"tasks":TASKS,"boards":board,"global_task":global_task})
  else:
   b=board[idx]
   await send(ws,{"type":"init","role":role,"names":NAMES,"tasks":TASKS,"global_task":global_task,"board":b,"student":idx})
  await broadcast({"type":"presence","online":[b["online"] for b in board]},"teacher")
  while True:
   m=await ws.receive_json();t=m.get("type")
   if role=="student":
    b=board[idx]
    if t=="stroke":
     pts=m.get("points",[])
     if not isinstance(pts,list) or len(pts)>500 or len(b["strokes"])>=400:continue
     if not all(isinstance(p,list) and len(p)==2 and all(isinstance(v,(int,float)) and 0<=v<=1 for v in p) for p in pts):continue
     b["strokes"].append(pts)
     await broadcast({"type":"stroke","student":idx,"points":pts,"layer":"student"},"teacher")
    elif t=="answer":
     b["answer"]=str(m.get("value",""))[:80]
     await broadcast({"type":"answer","student":idx,"value":b["answer"]},"teacher")
    elif t=="clear":
     b["strokes"]=[]
     await broadcast({"type":"clear","student":idx,"layer":"student"},"teacher")
   else:
    i=m.get("student")
    if t=="global_task":
     k=int(m.get("task",0))
     if 0<=k<len(TASKS):
      global_task=k;await broadcast({"type":"global_task","task":k})
    elif isinstance(i,int) and 0<=i<len(NAMES):
     b=board[i]
     if t=="task":
      k=m.get("task")
      if k is None or isinstance(k,int) and 0<=k<len(TASKS):
       b["task"]=k;await broadcast({"type":"task","student":i,"task":k})
     elif t=="comment":
      b["comment"]=str(m.get("value",""))[:500]
      await broadcast({"type":"comment","student":i,"value":b["comment"]})
     elif t=="grade":
      v=m.get("value")
      if isinstance(v,(int,float)) and 0<=v<=10:
       b["grade"]=v;await broadcast({"type":"grade","student":i,"value":v})
     elif t=="stroke":
      pts=m.get("points",[])
      if isinstance(pts,list) and len(pts)<=500 and len(b["teacher_strokes"])<400 and all(isinstance(p,list) and len(p)==2 and all(isinstance(v,(int,float)) and 0<=v<=1 for v in p) for p in pts):
       b["teacher_strokes"].append(pts);await broadcast({"type":"stroke","student":i,"points":pts,"layer":"teacher"})
     elif t=="clear_teacher":
      b["teacher_strokes"]=[];await broadcast({"type":"clear","student":i,"layer":"teacher"})
 except (WebSocketDisconnect,ValueError,TypeError):pass
 finally:
  if role in clients:clients[role].discard(ws)
  if role=="student" and idx is not None:
   student_sockets[idx].discard(ws)
   board[idx]["online"]=bool(student_sockets[idx])
   await broadcast({"type":"presence","online":[b["online"] for b in board]},"teacher")
