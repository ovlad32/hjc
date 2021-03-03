select 
count(distinct l.rowid), 
count(distinct c.rowid),
count(distinct c.collateral_amount) as grp1_unq, 
count(distinct c.contract_date) as grp2_unq, 
count(distinct c.contract_id) as grp3_unq, 
count(distinct c.contract_number) as grp4_unq, 
count(distinct c.initial_amount) as grp5_unq, 
count(distinct c.currency) as grp6_unq 
from cra.liabilities l
 inner join mt.contracts c
  on c.collateral_amount = l.collateral_value
  and c.contract_date = l.liability_date
  and c.contract_id = l.informer_deal_id
  and c.contract_number = l.liability_number
  and c.initial_amount = l.original_amount
  and c.currency = l.collateral_value_currency
  and c.currency = l.currency
;
/*
   3   183   327|  327|    0.71| (16560|16560) 
   [collateral_amount/15044/collateral_value]
   [contract_date/6891/liability_date]
   [contract_id/16560/informer_deal_id]
   [contract_number/16560/liability_number]
   [initial_amount/15067/original_amount]
   [currency/6/collateral_value_currency,currency]
