CREATE OR REPLACE PROCEDURE billing_analyst.refresh()
AS 
$$
BEGIN
    REFRESH MATERIALIZED VIEW billing_analyst.mv_mails_sender; 		
 	REFRESH MATERIALIZED VIEW billing_analyst.mv_summary_2pack; 
	REFRESH MATERIALIZED VIEW billing_analyst.mv_summary_new_paid;	
    REFRESH MATERIALIZED VIEW billing_analyst.mv_3pack_combined_data
    COMMIT;
	REFRESH MATERIALIZED VIEW billing_analyst.mv_summary_200;		
	REFRESH MATERIALIZED VIEW billing_analyst.mv_summary_500;		
	REFRESH MATERIALIZED VIEW billing_analyst.mv_summary_1000;		
    COMMIT;
	REFRESH MATERIALIZED VIEW billing_analyst.mv_basis;  
	REFRESH MATERIALIZED VIEW billing_analyst.mv_summary_regs_2;		 
	REFRESH MATERIALIZED VIEW billing_analyst.mv_summary_3pack;
    COMMIT;
END;
$$ 
LANGUAGE plpgsql;

call billing_analyst.refresh()

/*
select date::date, count(*)
from 
group by 1
order by 1 desc
limit 5
*/
