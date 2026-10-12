"""Calendar-period ten-draw rounds: number sets still change each period."""
from datetime import datetime,timedelta
import json

def ordinal(issue):
    s=str(issue)
    if len(s)==11 and s.isdigit() and 1<=int(s[8:])<=480:
        return datetime.strptime(s[:8],'%Y%m%d').toordinal()*480+int(s[8:])-1
    return int(s)
def advance(issue,n):
    s=str(issue)
    if len(s)==11 and s.isdigit() and 1<=int(s[8:])<=480:
        day=datetime.strptime(s[:8],'%Y%m%d');shift,seq=divmod(int(s[8:])-1+n,480)
        return (day+timedelta(days=shift)).strftime('%Y%m%d')+f'{seq+1:03d}'
    return str(int(s)+n)
def summarize(records,groups,target,version,active_versions,origins=None):
    result={};histories={}
    for k in groups:
        by_target={}
        for x in sorted(records,key=lambda x:(x['target'],x['created_at'],x['version'])):
            codes=json.loads(x['groups_json']).get(k)
            if codes:by_target[x['target']]=x
        origin=(origins or {}).get(k) or (min(by_target,key=ordinal) if by_target else advance(target,1) if active_versions.get(k)!=version else target)
        number=max(0,(ordinal(target)-ordinal(origin))//10)
        def one(round_index):
            start=advance(origin,round_index*10);end=advance(start,9);locked=[];settled=[]
            for x in by_target.values():
                if ordinal(start)<=ordinal(x['target'])<=ordinal(end):
                    locked.append(x)
                    if x['actual'] is not None:settled.append(x)
            hit=sum(x['actual'] in json.loads(x['groups_json'])[k] for x in settled)
            zhit=0;zn=0
            if k=='zodiac24':
                for x in settled:
                    meta=json.loads(x.get('meta_json') or '{}').get('zodiac24',{})
                    zm=meta.get('all_number_zodiac',{})
                    if str(x['actual']) in zm:zn+=1;zhit+=zm[str(x['actual'])] in meta.get('zodiacs',[])
            return {'number':round_index+1,'start':start,'end':end,'opened':len(settled),'hit':hit,'miss':len(settled)-hit,'rate':round(100*hit/len(settled),1) if settled else None,'locked':len(locked),'complete':len(settled)==10,'missing_locks':10-len(locked),'zodiac_opened':zn,'zodiac_hit':zhit,'zodiac_miss':zn-zhit}
        result[k]=one(number)
        histories[k]=[one(i) for i in range(number)]
    return result,histories
