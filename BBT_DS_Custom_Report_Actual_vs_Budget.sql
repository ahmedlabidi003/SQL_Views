/*
 *
 * Author:      
                ahmedlabidi003@gmail.com
 * File:        BBT_DS_Custom_Actual_vs_Budget.sql
 * Date:        October 12th, 2022
 *
 ***********************************************************************/

/*
 * Purpose: Used in Data Studio custom query to create The actual vs budget revenue report
 */

select 

t.* except(actual,fxsubactual,budget),


CASE 
	WHEN date_trunc(PARSE_DATE('%Y%m%d', @DS_END_DATE), MONTH) =  date_trunc(t.period_start_date, MONTH) 
		THEN actual 
	ELSE 
		0 	
END as previous_month_actual,

CASE 
	WHEN date_trunc(PARSE_DATE('%Y%m%d', @DS_END_DATE), MONTH) =  date_trunc(t.period_start_date, MONTH) 
		THEN fxsubactual 
	ELSE 
		0 
END as previous_month_fxsubactual,

CASE 
	WHEN date_trunc(PARSE_DATE('%Y%m%d', @DS_END_DATE), MONTH) =  date_trunc(t.period_start_date, MONTH) 
		THEN if(subsidiary_base_currency = 'CAN', fxsubactual, fxsubactual * @budget_fxrate) 
	ELSE 
		0 
END as previous_month_actual_budgetfx,


CASE WHEN ((period_start_date >= PARSE_DATE('%Y%m%d', @DS_START_DATE)) and (period_start_date <= PARSE_DATE('%Y%m%d', @DS_END_DATE))) THEN actual ELSE 0 END as year_to_date_actual,
CASE WHEN ((period_start_date >= PARSE_DATE('%Y%m%d', @DS_START_DATE)) and (period_start_date <= PARSE_DATE('%Y%m%d', @DS_END_DATE))) THEN fxsubactual ELSE 0 END as year_to_date_fxsubactual,

CASE WHEN ((period_start_date >= PARSE_DATE('%Y%m%d', @DS_START_DATE)) and (period_start_date <= PARSE_DATE('%Y%m%d', @DS_END_DATE))) THEN if(subsidiary_base_currency != 'CAN', fxsubactual * @budget_fxrate, fxsubactual) ELSE 0 END as year_to_date_actual_budgetfx,


CASE WHEN date_trunc(PARSE_DATE('%Y%m%d', @DS_END_DATE), MONTH) =  date_trunc(t.period_start_date, MONTH) THEN budget ELSE 0 END as previous_month_budget,


CASE WHEN ((period_start_date >= PARSE_DATE('%Y%m%d', @DS_START_DATE)) and (period_start_date <= PARSE_DATE('%Y%m%d', @DS_END_DATE))) THEN budget ELSE 0 END as year_to_date_budget,

--new additions Last year fields


CASE 
	WHEN date_sub(date_trunc(PARSE_DATE('%Y%m%d', @DS_END_DATE), MONTH), interval 1 year) =  date_trunc(t.period_start_date, MONTH) 
		THEN actual 
	ELSE 
		0 	
END as ly_previous_month_actual,

CASE 
	WHEN date_sub(date_trunc(PARSE_DATE('%Y%m%d', @DS_END_DATE), MONTH), interval 1 year) =  date_trunc(t.period_start_date, MONTH) 
		THEN fxsubactual 
	ELSE 
		0 
END as ly_previous_month_fxsubactual,

CASE 
	WHEN date_sub(date_trunc(PARSE_DATE('%Y%m%d', @DS_END_DATE), MONTH),interval 1 year) =  date_trunc(t.period_start_date, MONTH) 
		THEN if(subsidiary_base_currency = 'CAN', fxsubactual, fxsubactual * @budget_fxrate) 
	ELSE 
		0 
END as ly_previous_month_actual_budgetfx,

CASE WHEN ((period_start_date >= date_sub(PARSE_DATE('%Y%m%d', @DS_START_DATE), interval 1 year)) and (period_start_date <= date_sub(PARSE_DATE('%Y%m%d', @DS_END_DATE),interval 1 year))) THEN actual ELSE 0 END as ly_year_to_date_actual,

CASE WHEN ((period_start_date >= date_sub(PARSE_DATE('%Y%m%d', @DS_START_DATE),interval 1 year)) and (period_start_date <= date_sub(PARSE_DATE('%Y%m%d', @DS_END_DATE),interval 1 year))) THEN fxsubactual ELSE 0 END as ly_year_to_date_fxsubactual,
  
 
CASE WHEN ((period_start_date >= date_sub(PARSE_DATE('%Y%m%d', @DS_START_DATE),interval 1 year)) and (period_start_date <= date_sub(PARSE_DATE('%Y%m%d', @DS_END_DATE),interval 1 year))) THEN if(subsidiary_base_currency != 'CAN', fxsubactual * @budget_fxrate, fxsubactual) ELSE 0 END as ly_year_to_date_actual_budgetfx,


CASE WHEN date_sub(date_trunc(PARSE_DATE('%Y%m%d', @DS_END_DATE), MONTH), interval 1 year) =  date_trunc(t.period_start_date, MONTH) THEN budget ELSE 0 END as ly_previous_month_budget,


CASE WHEN ((period_start_date >= date_sub(PARSE_DATE('%Y%m%d', @DS_START_DATE), interval 1 year)) and (period_start_date <= date_sub(PARSE_DATE('%Y%m%d', @DS_END_DATE), interval 1 year))) THEN budget ELSE 0 END as ly_year_to_date_budget

	
from `{{project | sqlsafe}}.{{dataset | sqlsafe}}._Report_Actual_vs_Budget_v2` t
