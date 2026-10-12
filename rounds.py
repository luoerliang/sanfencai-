"""Calendar-period ten-draw rounds: number sets still change each period."""
from datetime import datetime,timedelta
import json
from functools import lru_cache

@lru_cache(maxsize=32768)
def ordinal(issue):
    s=str(issue)
    if len(s)==11 and s.isdigit() and 1<=int(s[8:])<=480:
        return datetime.strptime(s[:8],'%Y%m%d').toordinal()*480+int(s[8:])-1
    return int(s)
@lru_cache(maxsize=32768)
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
        buckets={}
        for row in by_target.values():
            idx=(ordinal(row['target'])-ordinal(origin))//10
            item=buckets.setdefault(idx,{'locked':0,'opened':0,'hit':0,'zodiac_opened':0,'zodiac_hit':0})
            item['locked']+=1
            if row['actual'] is not None:
                item['opened']+=1;item['hit']+=row['actual'] in json.loads(row['groups_json'])[k]
                if k=='zodiac24':
                    meta=json.loads(row.get('meta_json') or '{}').get('zodiac24',{});zm=meta.get('all_number_zodiac',{})
                    if str(row['actual']) in zm:item['zodiac_opened']+=1;item['zodiac_hit']+=zm[str(row['actual'])] in meta.get('zodiacs',[])
        def one(round_index):
            start=advance(origin,round_index*10);end=advance(start,9);x=buckets.get(round_index,{})
            opened=x.get('opened',0);hit=x.get('hit',0);locked=x.get('locked',0);zn=x.get('zodiac_opened',0);zh=x.get('zodiac_hit',0)
            return {'number':round_index+1,'start':start,'end':end,'opened':opened,'hit':hit,'miss':opened-hit,'rate':round(100*hit/opened,1) if opened else None,'locked':locked,'complete':opened==10,'missing_locks':10-locked,'zodiac_opened':zn,'zodiac_hit':zh,'zodiac_miss':zn-zh}
        result[k]=one(number)
        histories[k]=[one(i) for i in range(number)]
    return result,histories
