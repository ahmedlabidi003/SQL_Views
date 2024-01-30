/*
 ***********************************************************************

 * Authors:     ahmedlabidi003@gmail.com
 * File:        Report_ARR_MRR.sql
 ***********************************************************************/
with
  transaction as (select * from `{{project | sqlsafe}}.{{dataset | sqlsafe}}.Transaction`)
  , link as (select distinct * from `{{project | sqlsafe}}.{{dataset | sqlsafe}}._RAW_TransactionLink`)
  , arrangement as (select * from `{{project | sqlsafe}}.{{dataset | sqlsafe}}.RevenueArrangement`)
  , element as (select * from `{{project | sqlsafe}}.{{dataset | sqlsafe}}.RevenueElement`)
  , plan as (select * from `{{project | sqlsafe}}.{{dataset | sqlsafe}}.RevenueRecognitionPlan`)
  , item as ( select * from `{{project | sqlsafe}}.{{dataset | sqlsafe}}.Item`)
  , customer as ( select * from `{{project | sqlsafe}}.{{dataset | sqlsafe}}.Customer`)
  , book as ( select * from `{{project | sqlsafe}}.{{dataset | sqlsafe}}._RAW_TransactionBook`)
  , subsidiary as ( select * from `{{project | sqlsafe}}.{{dataset | sqlsafe}}.Subsidiary`)
  , contract as (select * from `{{project | sqlsafe}}.{{dataset | sqlsafe}}.Contract`)
  , postingPeriod as (select * from `{{project | sqlsafe}}.{{dataset | sqlsafe}}._RAW_AccountingPeriod`)
  , VGNAimport as ( select * from `{{project | sqlsafe}}.{{dataset | sqlsafe}}.VGNA_Import`)

select 
  recognized.internal_id
  , recognized.line_id
  , recognized.account
  , recognized.account_num
  , recognized.account_type
  , recognized.document_number
  , recognized.type
  , recognized.class
  , recognized.period_start_date
  , recognized.period_end_date
  , recognized.amount
  , recognized.department
  , recognized.fxsubamount
  , if(link.applied_to_id is null, recognized.fxsubamount, 0) as non_rev_rec_amount
  , customer.internal_id as customer_id
  , recognized.customer_name as cust2
  , customer.name as customer_name
  , customer.sales_rep as sales_rep
  , link.amount as link_amount
  , link.fxamount as link_fxamount
  , element.start_date as element_start_date
  , element.end_date as element_end_date
  , date_diff(date_add(element.end_date, interval 1 day), element.start_date, DAY) as element_term_in_days
  , element.revenue_plan_status
  , element.internal_id as element_id
  , element.number as element_number
  , arrangement.internal_id as arrangement_id
  , arrangement.document_number as arrangement_number
  , plan.internal_id as plan_id  
  , plan.number as plan_number
  , plan.planned_period_name
  , plan.planned_period_id
  , plan.posting_period_name
  , plan.posting_period_id
  , plannedPeriod.period_end_date as planned_period_end_date
  , plan.rev_rec_start_date
  , plan.rev_rec_end_date
  , date_diff(date_add(plan.rev_rec_end_date, interval 1 day), plan.rev_rec_start_date, DAY) as rev_rec_term_in_days
  , transactionItem.internal_id as transaction_item_id
  , transactionItem.name as transaction_item_name
  , transactionItem.vgna_product_family as transaction_item_vgna_product_family
  , renewalItem.internal_id as item_id
  , renewalItem.name as item_name
  , renewalItem.vgna_product_family as vgna_product_family
  , customer.market_segment_name as market_segment_name
  , recognized.status
  , subsidiary.internal_id as subsidiary_id
  , subsidiary.name as subsidiary
  , subsidiary.name_no_hierarchy as subsidiary_no_hierarchy
  , subsidiary.base_currency as subsidiary_currency
  , plan.journal_id
  , if(link.link_type="Sales Order Revenue Revaluation",link.amount*if(recognized.amount < 0, -1, 1 ),if(planned_period_id is not null,plan.amount_line_level,0)) as full_recognized_amount
  , if(link.link_type="Sales Order Revenue Revaluation",link.amount*if(recognized.amount < 0, -1, 1 ),(if(plan.planned_period_id = plan.posting_period_id and element.start_date <= recognized.period_start_Date ,  plan.amount_line_level, 0))) as revenue_amount
  , if(plan.planned_period_id <> plan.posting_period_id,  plan.amount_line_level, 0) as catch_amount
  , if(element.start_date > recognized.period_start_Date and plan.planned_period_id = plan.posting_period_id,  plan.amount_line_level, 0) as pre_rec_amount  
  , (element.sales_amount * element.exchange_rate) as element_sales_amount
  , plan.amount as plan_revenue_amount
  , if(
      (
          (plan.amount<>(element.sales_amount*element.exchange_rate)) 
            or 
          (DATE_TRUNC(element.start_date, MONTH) <> DATE_TRUNC(plan.rev_rec_start_date, MONTH))  
     ) and element.start_date <= recognized.period_start_Date and plan.planned_period_id = plan.posting_period_id,  
      if (plan.amount<>(element.sales_amount*element.exchange_rate),element.sales_amount*element.exchange_rate, plan.amount)/nullif(date_diff(date_add(element.end_date, interval 1 day) , element.start_date, DAY),0) *  (case 
        when element.start_date between recognized.period_Start_date and recognized.period_end_date
          then 32 - EXTRACT(DAY FROM DATE_ADD(DATE_TRUNC(element.start_date, MONTH), INTERVAL 31 DAY)) 
        when element.end_date between recognized.period_Start_date and recognized.period_end_date
          then 32 - EXTRACT(DAY FROM DATE_ADD(DATE_TRUNC(element.end_date, MONTH), INTERVAL 31 DAY)) 
        else
          32 - EXTRACT(DAY FROM DATE_ADD(DATE_TRUNC(recognized.period_start_date, MONTH), INTERVAL 31 DAY))
      end), null) as adjusted_mrr_amount  
  , if(
      (
          (plan.amount<>(element.sales_amount*element.exchange_rate)) 
            or 
          (DATE_TRUNC(element.start_date, MONTH) <> DATE_TRUNC(plan.rev_rec_start_date, MONTH))  
     ) and element.start_date <= recognized.period_start_Date and plan.planned_period_id <> plan.posting_period_id,  
      if (plan.amount<>(element.sales_amount*element.exchange_rate),element.sales_amount*element.exchange_rate, plan.amount)/nullif(date_diff(date_add(element.end_date, interval 1 day) , element.start_date, DAY),0) *  (case 
        when element.start_date between recognized.period_Start_date and recognized.period_end_date
          then 32 - EXTRACT(DAY FROM DATE_ADD(DATE_TRUNC(element.start_date, MONTH), INTERVAL 31 DAY)) 
        when element.end_date between recognized.period_Start_date and recognized.period_end_date
          then 32 - EXTRACT(DAY FROM DATE_ADD(DATE_TRUNC(element.end_date, MONTH), INTERVAL 31 DAY)) 
        else
          32 - EXTRACT(DAY FROM DATE_ADD(DATE_TRUNC(recognized.period_start_date, MONTH), INTERVAL 31 DAY))
      end), null) as adjusted_catch_up_amount                            
  , 0 as booking_amount
  , recognized.currency as transaction_currency
  , element.currency as element_currency
  , book.accounting_book_id
  , book.accounting_book_name
  , transactionItem.product_line_id as transaction_item_product_line_id
  , transactionItem.product_line as transaction_item_product_line
  , renewalItem.product_line_id as product_line_id
  , renewalItem.product_line as product_line  
  , original_tx.internal_id as original_transaction_id
  , original_tx.line_id as original_transaction_line_id
  , original_tx.document_number as original_transaction_number
  , original_tx.trandate as original_transaction_date
  , original_tx.type as original_transaction_type
  , contract.internal_id as contract_id
  , contract.internal_id as contract
  , if(customer.internal_id <> enduser.internal_id, customer.internal_id, null) as partner_id
  , if(customer.internal_id <> enduser.internal_id, customer.name, null) as partner 
  , enduser.internal_id as end_user_id
  , enduser.name as end_user
  , case 
        when element.start_date between recognized.period_Start_date and recognized.period_end_date
          then 32 - EXTRACT(DAY FROM DATE_ADD(DATE_TRUNC(element.start_date, MONTH), INTERVAL 31 DAY)) 
        when element.end_date between recognized.period_Start_date and recognized.period_end_date
          then 32 - EXTRACT(DAY FROM DATE_ADD(DATE_TRUNC(element.end_date, MONTH), INTERVAL 31 DAY)) 
        else
          32 - EXTRACT(DAY FROM DATE_ADD(DATE_TRUNC(recognized.period_start_date, MONTH), INTERVAL 31 DAY))
      end as number_of_days

  , original_tx.from_contract_id as original_contract
  , original_tx.contract_2_id as current_contract
  , original_tx.contract_start_date as current_contract_start_date
  , original_tx.contract_end_date as current_contract_end_date
  , original_tx.is_declined as is_declined
  , if(contract.status in ('Renewal Rejected','Canceled'), true, false) as is_contract_rejected

  , contract.status as contract_status
  , renewingSo.is_declined as is_renewing_so_declined
  , original_tx.status as so_status
  , contract.renewal_transaction_id
  , contract.renewal_transaction
  , VGNAImport.ns_customer_id
  , VGNAImport.journal_id as VGNA_JOUrnal_Id
  

from transaction recognized

left join link on recognized.internal_id = link.applying_id and recognized.line_id = link.applying_line_id and link.link_type in ('Revenue/Amortization Recognition','Sales Order Revenue Revaluation')
left join arrangement on arrangement.internal_id = link.applied_to_id and arrangement.line_id = link.applied_to_line_id
left join element on arrangement.revenue_element_id = element.internal_id
left join plan on element.internal_id = plan.created_from_id and recognized.period_id = plan.posting_period_id and plan.revenue_plan_type = 'Actual' and plan.journal_id = recognized.internal_id
left join VGNAimport on arrangement.internal_id = VGNAimport.revenue_arrangement_id and arrangement.fxamount = VGNAimport.amount_deferred and arrangement.entity_id = VGNAimport.customer_id and VGNAimport.revenue_plan = plan.number --and VGNAimport.item_internal_id=arrangement.item_id
left join item transactionItem on transactionItem.internal_id = ifnull(VGNAimport.item_internal_id,arrangement.item_id)
left join item renewalItem on ifnull(transactionItem.renew_with_id, transactionItem.internal_id) = renewalItem.internal_id 
--add this in the place of the line above if it's correct
--left join item renewalItem on ifnull(VGNAimport.item_internal_id, (ifnull(transactionItem.renew_with_id, transactionItem.internal_id))) = renewalItem.internal_id
left join customer newcustomer on arrangement.customer_id = newcustomer.old_subsidiary_customer_id
left join customer on customer.internal_id = ifnull(newcustomer.internal_id, arrangement.customer_id)
left join subsidiary on ifnull(newcustomer.primary_subsidiary_id, recognized.subsidiary_id) = subsidiary.internal_id
left join book on recognized.internal_id = book.internal_id and recognized.line_id = book.line_id and recognized.trandate=book.trandate
left join transaction original_tx on original_tx.internal_id=cast(REGEXP_EXTRACT(element.reference_id, "[^,]*_(.*)") as INT64) and original_tx.line_id =0 
left join customer enduser on ifnull(VGNAimport.ns_customer_internal_id, ifnull(original_tx.end_user_id, ifnull(newcustomer.internal_id, arrangement.customer_id))) = enduser.internal_id
left join contract on contract.internal_id = original_tx.contract_2_id
--left join Transaction Transbody on Transbody.internal_id = contract.original_sales_order_id and Transbody.line_id=0
left join Transaction renewingSo on renewingSo.internal_id = contract.renewal_transaction_id and renewingSo.line_id=0
left join postingPeriod plannedPeriod on plan.planned_period_id = plannedPeriod.internal_id

where 
  recognized.is_posting = true
  and recognized.account_type = 'Income'
  and (
      lower(recognized.account) like "%subscription%" 
      or lower(recognized.account) like "%maintenance%"
  ) 
  and lower(recognized.account) not like "%intergroup%"
  and book.accounting_book_id=1
