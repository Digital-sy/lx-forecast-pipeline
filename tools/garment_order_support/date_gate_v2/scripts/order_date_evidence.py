"""Pure order-date evidence checks; never chooses an effective production order date."""
from datetime import date,datetime
import re

def parse_explicit_order_date(value, *, explicitly_order_date=False):
 """Only a date cell or explicitly labelled order-date literal. No PO/filename heuristics."""
 if isinstance(value,datetime):return {'status':'EXPLICIT_DATE','date':value.date().isoformat()}
 if isinstance(value,date):return {'status':'EXPLICIT_DATE','date':value.isoformat()}
 if value is None or value=='':return {'status':'UNKNOWN','date':None}
 if not isinstance(value,str):return {'status':'UNSUPPORTED_CELL_TYPE','date':None}
 text=value.strip()
 if explicitly_order_date and re.fullmatch(r'\d{8}|\d{4}[-/.年]\d{1,2}[-/.月]\d{1,2}日?',text):text='下单日期：'+text
 m=re.fullmatch(r'(?:下单时间|下单日期|订单日期)\s*[:：]\s*(\d{8}|\d{4}[-/.年]\d{1,2}[-/.月]\d{1,2}日?)',text)
 if not m:return {'status':'NOT_EXPLICIT_ORDER_DATE','date':None}
 token=m.group(1)
 try:
  if re.fullmatch(r'\d{8}',token):parsed=date(int(token[:4]),int(token[4:6]),int(token[6:]))
  else:
   y,mo,d=re.findall(r'\d+',token);parsed=date(int(y),int(mo),int(d))
 except ValueError:return {'status':'INVALID_CALENDAR_DATE','date':None}
 return {'status':'EXPLICIT_ORDER_DATE_LITERAL','date':parsed.isoformat()}
def audit_order_date(body_value,filename_date,stored_date,stored_conflict, *, explicitly_order_date=False):
 body=parse_explicit_order_date(body_value,explicitly_order_date=explicitly_order_date);dates={x for x in [body['date'],filename_date] if x}
 conflict=len(dates)>1
 return {'body':body,'filename_date':filename_date,'stored_date':stored_date,'stored_conflict':stored_conflict,'date_conflict':conflict,
  'state':'CONFLICT_NO_EFFECTIVE_ORDER_DATE' if conflict else 'BODY_DATE_KNOWN_NOT_EFFECTIVENESS_PROOF' if body['date'] else 'INVALID_BODY_ORDER_DATE' if body['status']=='INVALID_CALENDAR_DATE' else 'BODY_DATE_UNKNOWN' if body['status']=='UNKNOWN' else 'UNPARSED_BODY_ORDER_DATE',
  'stored_conflict_false_negative':conflict and stored_conflict is False,'requires_reparse':bool(conflict and (stored_conflict is not True or stored_date is not None)),'effective_order_date':None,'source_correction_performed':False}
