CREATE OR REPLACE PROCEDURE svodnie_total.refresh()
AS 
$$
BEGIN
    REFRESH MATERIALIZED VIEW billing_analyst.mv_mails_sender;
	REFRESH MATERIALIZED VIEW billing_analyst.mv_summary_1000;
	REFRESH MATERIALIZED VIEW billing_analyst.mv_summary_200;
	REFRESH MATERIALIZED VIEW billing_analyst.mv_summary_2pack;
	REFRESH MATERIALIZED VIEW billing_analyst.mv_summary_500;
	REFRESH MATERIALIZED VIEW billing_analyst.mv_summary_new_paid;
	REFRESH MATERIALIZED VIEW billing_analyst.mv_summary_regs_2;
	-- REFRESH MATERIALIZED VIEW billing_analyst.mv_basis;  
	REFRESH MATERIALIZED VIEW billing_analyst.mv_3pack_base_data;
	COMMIT;
    REFRESH MATERIALIZED VIEW billing_analyst.mv_3pack_enriched_data;
	COMMIT;	
    REFRESH MATERIALIZED VIEW billing_analyst.mv_3pack_final_data;
    COMMIT;   
	REFRESH MATERIALIZED VIEW billing_analyst.mv_summary_3pack;
    COMMIT;
END;
$$ 
LANGUAGE plpgsql;


CALL svodnie_total.refresh();




select count(*) from billing_analyst.mv_summary_regs_2