#!/usr/bin/python3
import psycopg2
import os

try:
    connect_str = "dbname=" + os.environ['PG_DATABASE'] + " user=" + \
                  os.environ['PG_USERNAME'] + \
                  " host=" + os.environ['PG_ADDRESS'] + \
                  " password=" + os.environ['PG_PASSWORD']

    # create connection
    conn = psycopg2.connect(connect_str)

    # create cursor
    cursor = conn.cursor()
    check_sql = "SELECT count(*) as cnt FROM sfdc_derived.ss_account where " +\
                "snapshot_date::date = (current_date at time zone 'US/Pacific' - interval '1 day')::date"
    cursor.execute(check_sql)
    rs = cursor.fetchone()
    if rs[0] > 1:
        print("Snapshot exists. Passing.")
        cursor.close()
        pass
    else:
        print("No snapshot found. Creating one.")
        cursor = conn.cursor()
        sql = "INSERT INTO sfdc_derived.ss_account " + \
             "SELECT (current_date at time zone 'US/Pacific' - interval '1 day')::date as snapshot_date, " + \
             "a.__row_id, " + \
             "a.referenceable_customer_notes__c, " + \
             "a.license_utilization__c, " + \
             "a.reseller_discount__c, " + \
             "a.zendesk__last_sync_date__c, " + \
             "a.days_outstanding__c, " + \
             "a.ultimateparentpotentialeeusers__c, " + \
             "a.sub_industry__c, " + \
             "a.using_ce__c, " + \
             "a.parentid, " + \
             "a.ae_comments__c, " + \
             "a.tedd_employees__c, " + \
             "a.isimplementationsupportcustomer__c, " + \
             "a.leandata__reporting_last_marketing_touch_date__c, " + \
             "a.leandata__reporting_target_account_number__c, " + \
             "a.jigsawcompanyid, " + \
             "a.number_of_licenses_total__c, " + \
             "a.count_of_active_subscriptions__c, " + \
             "a.count_of_active_subscription_charges__c, " + \
             "a.account_owner_calc__c, " + \
             "a.leandata__scenario_2_owner__c, " + \
             "a.primary_contact__c, " + \
             "a.customer_since__c, " + \
             "a.it_tedd__c, " + \
             "a.cmrr_this_account__c, " + \
             "a.website, " + \
             "a.reseller__c, " + \
             "a.historical_max_users__c, " + \
             "a.primary_contact_id__c, " + \
             "a.leandata__scenario_1_owner__c, " + \
             "a.territory__c, " + \
             "a.leandata__reporting_has_opportunity__c, " + \
             "a.company_technologies__c, " + \
             "a.isgitlabeecustomer__c, " + \
             "a.billingstatecode, " + \
             "a.absd_campaign__c, " + \
             "a.sdr_account_team__c, " + \
             "a.entity__c, " + \
             "a.entity_bank_information__c, " + \
             "a.potential_users_all_child_accounts__c, " + \
             "a.partner_vat_tax_id__c, " + \
             "a.federal_account__c, " + \
             "a.leandata__scenario_4_owner__c, " + \
             "a.dscorgpkg__fortune_rank__c, " + \
             "a.ultimate_parent_account_id__c, " + \
             "a.gitlab_com_user__c, " + \
             "a.partner_beneficiary_name__c, " + \
             "a.support_level_new__c, " + \
             "a.entity_beneficiary_information__c, " + \
             "a.billingcountrycode, " + \
             "a.terminus_clicks__c, " + \
             "a.iseepluscustomer__c, " + \
             "a.billingstreet, " + \
             "a.reseller_type__c, " + \
             "a.leandata__reporting_last_sales_touch_date__c, " + \
             "a.entity_contact_information__c, " + \
             "a.infer__infer_rating__c, " + \
             "a.shippingstatecode, " + \
             "a.data_quality_description__c, " + \
             "a.referenceable_customer__c, " + \
             "a.isfilelockingcustomer__c, " + \
             "a.masterrecordid, " + \
             "a.max_for_potential_ee_users__c, " + \
             "a.opt_out_geo__c, " + \
             "a.shippingstate, " + \
             "a.sub_region__c, " + \
             "a.dedicated_service_engineer__c, " + \
             "a.description, " + \
             "a.photourl, " + \
             "a.billingcountry, " + \
             "a.account_id_18__c, " + \
             "a.zendesk__zendesk_outofsync__c, " + \
             "a.partner_account_iban_number__c, " + \
             "a.cmrr_all_child_accounts__c, " + \
             "a.active_ce_users__c, " + \
             "a.count_of_products_purchased__c, " + \
             "a.ispartner, " + \
             "a.zuora__active__c, " + \
             "a.leandata__sla__c, " + \
             "a.sdr__c, " + \
             "a.shippingpostalcode, " + \
             "a.isgithostdevelopercustomer__c, " + \
             "a.terminus_velocity_level__c, " + \
             "a.recordtypeid, " + \
             "a.previous_account_owner__c, " + \
             "a.numberofemployees, " + \
             "a.of_licenses__c, " + \
             "a.lastmodifieddate, " + \
             "a.zendesk__zendesk_oldtags__c, " + \
             "a.count_of_new_business_won_opps__c, " + \
             "a.industry, " + \
             "a.product_category__c, " + \
             "a.ce_instances__c, " + \
             "a.zendesk__createdupdatedflag__c, " + \
             "a.revenue__c, " + \
             "a.potential_users__c, " + \
             "a.number_of_licenses_this_account__c, " + \
             "a.leandata__reporting_total_sales_touches__c, " + \
             "a.carr_total__c, " + \
             "a.count_of_opportunities__c, " + \
             "a.business_development_rep__c, " + \
             "a.ultimate_parent_old__c, " + \
             "a.leandata__scenario_3_owner__c, " + \
             "a.isgithoststartupcustomer__c, " + \
             "a.potential_carr_this_account__c, " + \
             "a.potential_ee_subscription_amount__c, " + \
             "a.zuora__customerpriority__c, " + \
             "a.potential_ee_users_total__c, " + \
             "a.account_tier__c, " + \
             "a.billinglatitude, " + \
             "a.dscorgpkg__it_employees__c, " + \
             "a.billingstate, " + \
             "a.iseestandardcustomer__c, " + \
             "a.infer__infer_band__c, " + \
             "a.leandata__reporting_total_marketing_touches__c, " + \
             "a.data_quality_score__c, " + \
             "a.leandata__ld_emaildomains__c, " + \
             "a.initial_start_date__c, " + \
             "a.zendesk__domain_mapping__c, " + \
             "a.zendesk__notes__c, " + \
             "a.zuora__numberoflocations__c, " + \
             "a.leandata__routing_action__c, " + \
             "a.health__c, " + \
             "a.zuora_issue__c, " + \
             "a.shippinggeocodeaccuracy, " + \
             "a.billingpostalcode, " + \
             "a.billingd__c, " + \
             "a.trigger_workflow__c, " + \
             "a.cmrr_total__c, " + \
             "a.leandata__reporting_recent_marketing_touches__c, " + \
             "a.lastreferenceddate, " + \
             "a.zendesk__zendesk_organization__c, " + \
             "a.zuora__upsellopportunity__c, " + \
             "a.type_amount_close_date__c, " + \
             "a.zuora__sla__c, " + \
             "a.lastmodifiedbyid, " + \
             "a.products_purchased__c, " + \
             "a.isdeleted, " + \
             "a.terminus_impressions__c, " + \
             "a.support_level_numeric__c, " + \
             "a.createddate, " + \
             "a.isgitlabgeocustomer__c, " + \
             "a.zendesk__tags__c, " + \
             "a.infer__infer_score__c, " + \
             "a.business_development_rep_account_team__c, " + \
             "a.name, " + \
             "a.sales_segmentation_updated__c, " + \
             "a.invoice_owner__c, " + \
             "a.leandata__reporting_target_account__c, " + \
             "a.sales_segmentation__c, " + \
             "a.shippinglatitude, " + \
             "a.health_score_reasons__c, " + \
             "a.shippinglongitude, " + \
             "a.account_manager__c, " + \
             "a.count_of_won_opportunities__c, " + \
             "a.shippingstreet, " + \
             "a.leandata__search__c, " + \
             "a.manual_support_upgrade__c, " + \
             "a.opt_out_file_locking__c, " + \
             "a.shippingcountrycode__c, " + \
             "a.next_renewal_date__c, " + \
             "a.most_recent_expired_subscription_date__c, " + \
             "a.id, " + \
             "a.zendesk__last_sync_status__c, " + \
             "a.leandata__tag__c, " + \
             "a.gitlab_team__c, " + \
             "a.ultimateparentsalessegmentation__c, " + \
             "a.license_user_count__c, " + \
             "a.systemmodstamp, " + \
             "a.infer__infer_hash__c, " + \
             "a.account_owner_team__c, " + \
             "a.shippingcountry, " + \
             "a.createdbyid, " + \
             "a.paymenttermnumeric__c, " + \
             "a.temp_duplicated_account__c, " + \
             "a.ultimate_parent_is_current_account__c, " + \
             "a.lastvieweddate, " + \
             "a.solutions_architect__c, " + \
             "a.carr_this_account__c, " + \
             "a.leandata__reporting_customer__c, " + \
             "a.ownerid, " + \
             "a.count_of_billing_accounts__c, " + \
             "a.billinglongitude, " + \
             "a.lastactivitydate, " + \
             "a.ultimate_parent_account__c, " + \
             "a.zuora__slaserialnumber__c, " + \
             "a.carr_all_child_accounts__c, " + \
             "a.isgithostbusinesscustomer__c, " + \
             "a.reference_type__c, " + \
             "a.entity_override__c, " + \
             "a.of_carr__c, " + \
             "a.territories_covered__c, " + \
             "a.support_level__c, " + \
             "a.terminus_spend__c, " + \
             "a.region__c, " + \
             "a.leandata__ld_emaildomain__c, " + \
             "a.shippingcountrycode, " + \
             "a.partner_bank_name__c, " + \
             "a.billinggeocodeaccuracy, " + \
             "a.special_terms__c, " + \
             "a.top_list__c, " + \
             "a.number_of_licenses_all_child_accounts__c, " + \
             "a.technology_stack__c, " + \
             "a.shippingcity, " + \
             "a.partner_routing__c, " + \
             "a.subscription_amount__c, " + \
             "a.count_of_open_renewal_opportunities__c, " + \
             "a.partner_name_and_type__c, " + \
             "a.parent_account_type__c, " + \
             "a.leandata__reporting_total_leads_and_contacts__c, " + \
             "a.zuora__slaexpirationdate__c, " + \
             "a.ispremiumsupportcustomer__c, " + \
             "a.type, " + \
             "a.sum_of_open_renewal_opportunities__c, " + \
             "a.infer__infer_last_modified__c, " + \
             "a.zendesk__zendesk_organization_id__c, " + \
             "a.zendesk__result__c, " + \
             "a.concurrent_ee_subscriptions__c, " + \
             "a.iseebasiccustomer__c, " + \
             "a.isgithoststoragecustomer__c, " + \
             "a.zendesk__create_in_zendesk__c, " + \
             "a.billingcity, " + \
             "a.large_account__c, " + \
             "a.domains__c, " + \
             "a.account_initial_start_date__c, " + \
             "a.technical_account_manager_lu__c, " + \
             "a.sales_segmentation_new__c, " + \
             "a.number_of_open_opportunities__c, " + \
             "a.ultimate_parent_sales_segment_emp_text__c, " + \
             "a.ultimate_parent_sales_segment_employees__c, " + \
             "a.total_account_value__c " + \
              "FROM sfdc.account a WHERE isdeleted=FALSE"
        cursor.execute(sql)
        conn.commit()
        cursor.close()
    conn.close()

except Exception as e:
    print("There was an error snapshotting data", e)
    raise
