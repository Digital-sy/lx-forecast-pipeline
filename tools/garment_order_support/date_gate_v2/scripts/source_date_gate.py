"""Inspect parsed order-date claims before a new source package may be built."""
from order_date_evidence import parse_explicit_order_date

def inspect_rows(rows):
    checked=[]
    seen=set()
    for row in rows:
        key=row.get('line_id')
        if not key or key in seen: raise ValueError('MISSING_OR_DUPLICATE_RAW_LINE_ID')
        seen.add(key)
        filename=row.get('filename_date');stored=row.get('order_date');flag=row.get('order_date_conflict')
        known=set();unparsed=[];body_results=[]
        for body in row.get('body_order_dates',[]):
            explicit=body.get('source') in ('inline_label','adjacent_label')
            value=parse_explicit_order_date(body.get('raw'),explicitly_order_date=explicit)
            raw_date=value['date']
            # Already parsed Excel date serials retain their evidence. No serial is guessed here.
            old_date=body.get('value')
            if old_date is not None:
                old=parse_explicit_order_date(old_date,explicitly_order_date=True)
                if old['date'] is None: raise ValueError('INVALID_EXISTING_PARSED_DATE')
                known.add(old['date'])
            if raw_date: known.add(raw_date)
            elif body.get('raw') not in (None,'') and old_date is None: unparsed.append(body.get('cell'))
            body_results.append({'cell':body.get('cell'),'raw':body.get('raw'),'stored_parsed_date':old_date,'independent_parse':value})
        comparisons=known|({filename} if filename else set())
        conflict=len(comparisons)>1
        reasons=[]
        if conflict and (flag is not True or stored is not None): reasons.append('MISSED_OR_UNRESOLVED_DATE_CONFLICT')
        if unparsed and stored is not None: reasons.append('UNPARSED_BODY_DATE_WITH_SELECTED_ORDER_DATE')
        if stored is not None and known and stored not in known: reasons.append('SELECTED_DATE_NOT_SUPPORTED_BY_BODY')
        checked.append({'line_id':key,'source_sha256':row.get('source_sha256'),'sheet':row.get('sheet'),'row':row.get('row'),
          'body_dates':body_results,'known_body_dates':sorted(known),'filename_date':filename,'stored_order_date':stored,
          'stored_conflict_flag':flag,'independent_conflict':conflict,'unparsed_body_cells':unparsed,
          'requires_reparse':bool(reasons),'reasons':reasons,'data_modified':False})
    blocked=sum(x['requires_reparse'] for x in checked)
    return {'status':'REPARSE_REQUIRED_BEFORE_PACKAGE' if blocked else 'PASS_PRESERVING_UNKNOWN_AND_CONFLICTS',
      'rows_checked':len(checked),'requires_reparse_rows':blocked,'rows':checked,'data_modified':False,
      'meaning':'Date consistency only; does not prove normal valid purchase or future applicability.'}
