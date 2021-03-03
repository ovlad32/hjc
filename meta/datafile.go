package meta

import "strings"

type DumpDesc struct {
	TableName string
	Path      string
	ColumnSep string
	Columns   []ColDescr
}
type ColDescr struct {
	ColumnName  string
	DataType    string
	FusionSep   string
	LeadingChar string
}

func GetDumpDesc(code string) DumpDesc {
	switch code {
	case "cra","cra_01":
		return DumpDesc{
			TableName: "LIABILITIES",
			Path:      "/Dev/data.ge/HXE."+ strings.ToUpper(code) +".LIABILITIES.dat",
			ColumnSep: string(0x07),
			Columns: []ColDescr{
				{"id", "int", "", ""},
				{"informer_code", "string", "", ""},
				{"informer_deal_id", "string", "", ""},
				{"liability_date", "date", "", ""},
				{"liability_number", "string", "", ""},
				{"liability_type", "string", "", ""},
				{"original_amount", "float", "", ""},
				{"currency", "string", "", ""},
				{"due_date", "date", "", ""},
				{"collateral_type", "string", "", ""},
				{"collateral_value", "float", "", ""},
				{"collateral_value_currency", "string", "", ""},
				{"collateral_assessment_date", "date", "", ""},
			},
		}

	case "mt", "mt_01":
		return DumpDesc{
			TableName: "CONTRACTS",
			Path:      "/Dev/data.ge/HXE." + strings.ToUpper(code) + ".CONTRACTS.dat",
			ColumnSep: string(0x07),
			Columns: []ColDescr{
				{"contract_id", "int", "", ""},
				{"contract_type", "string", "", ""},
				{"product_code", "string", "", ""},
				{"contract_status", "string", "", ""},
				{"customer_id", "int", "", ""},
				{"contract_number", "string", "", ""},
				{"contract_date", "date", "", ""},
				{"closing_date", "date", "", ""},
				{"initial_amount", "float", "", ""},
				{"currency", "string", "", ""},
				{"maturity_date", "date", "", ""},
				{"collateral_amount", "float", "", ""},
				{"collateral_reevaluation_date", "date", "", ""},
				{"interest_rate", "float", "", ""},
				{"point_id", "int", "", ""},
			},
		}
		/*
			case "grp01_src01":
				return "/Dev/data.ge/3/26/ORCL.GE_P2_GRP01_SRC01._RIGHT.AP_INVOICES_ALL.dat",
					[]string{
						"invoice_id",
						"last_update_date",
						"last_updated_by",
						"vendor_id",
						"invoice_num",
						"set_of_books_id",
						"invoice_currency_code",
						"payment_currency_code",
						"payment_cross_rate",
						"invoice_amount",
						"vendor_site_id",
						"amount_paid",
						"discount_amount_taken",
						"invoice_date",
						"source",
						"invoice_type_lookup_code",
						"description",
						"batch_id",
						"amount_applicable_to_discount",
						"tax_amount",
						"terms_id",
						"terms_date",
						"payment_method_lookup_code",
						"pay_group_lookup_code",
						"accts_pay_code_combination_id",
						"payment_status_flag",
						"creation_date",
						"created_by",
						"base_amount",
						"vat_code",
						"last_update_login",
						"exclusive_payment_flag",
						"po_header_id",
						"freight_amount",
						"goods_received_date",
						"invoice_received_date",
						"voucher_num",
						"approved_amount",
						"recurring_payment_id",
						"exchange_rate",
						"exchange_rate_type",
						"exchange_date",
						"earliest_settlement_date",
						"original_prepayment_amount",
						"doc_sequence_id",
						"doc_sequence_value",
						"doc_category_code",
						"attribute1",
						"attribute2",
						"attribute3",
						"attribute4",
						"attribute5",
						"attribute6",
						"attribute7",
						"attribute8",
						"attribute9",
						"attribute10",
						"attribute11",
						"attribute12",
						"attribute13",
						"attribute14",
						"attribute15",
						"attribute_category",
						"approval_status",
						"approval_description",
						"invoice_distribution_total",
						"posting_status",
						"prepay_flag",
						"authorized_by",
						"cancelled_date",
						"cancelled_by",
						"cancelled_amount",
						"temp_cancelled_amount",
						"project_accounting_context",
						"ussgl_transaction_code",
						"ussgl_trx_code_context",
						"project_id",
						"task_id",
						"expenditure_type",
						"expenditure_item_date",
						"pa_quantity",
						"expenditure_organization_id",
						"pa_default_dist_ccid",
						"vendor_prepay_amount",
						"payment_amount_total",
						"awt_flag",
						"awt_group_id",
						"reference_1",
						"reference_2",
						"org_id",
						"pre_withholding_amount",
						"global_attribute_category",
						"global_attribute1",
						"global_attribute2",
						"global_attribute3",
						"global_attribute4",
						"global_attribute5",
						"global_attribute6",
						"global_attribute7",
						"global_attribute8",
						"global_attribute9",
						"global_attribute10",
						"global_attribute11",
						"global_attribute12",
						"global_attribute13",
						"global_attribute14",
						"global_attribute15",
						"global_attribute16",
						"global_attribute17",
						"global_attribute18",
						"global_attribute19",
						"global_attribute20",
						"auto_tax_calc_flag",
						"payment_cross_rate_type",
						"payment_cross_rate_date",
						"pay_curr_invoice_amount",
						"mrc_base_amount",
						"mrc_exchange_rate",
						"mrc_exchange_rate_type",
						"mrc_exchange_date",
						"mrc_posting_status",
						"gl_date",
						"award_id",
						"paid_on_behalf_employee_id",
						"amt_due_ccard_company",
						"amt_due_employee",
						"approval_ready_flag",
						"approval_iteration",
						"wfapproval_status",
						"requester_id",
						"validation_request_id",
						"validated_tax_amount",
						"quick_credit",
						"credited_invoice_id",
						"distribution_set_id",
						"application_id",
						"product_table",
						"reference_key1",
						"reference_key2",
						"reference_key3",
						"reference_key4",
						"reference_key5",
						"total_tax_amount",
						"self_assessed_tax_amount",
						"tax_related_invoice_id",
						"trx_business_category",
						"user_defined_fisc_class",
						"taxation_country",
						"document_sub_type",
						"supplier_tax_invoice_number",
						"supplier_tax_invoice_date",
						"supplier_tax_exchange_rate",
						"tax_invoice_recording_date",
						"tax_invoice_internal_seq",
						"legal_entity_id",
						"historical_flag",
						"force_revalidation_flag",
						"bank_charge_bearer",
						"remittance_message1",
						"remittance_message2",
						"remittance_message3",
						"unique_remittance_identifier",
						"uri_check_digit",
						"settlement_priority",
						"payment_reason_code",
						"payment_reason_comments",
						"payment_method_code",
						"delivery_channel_code",
						"quick_po_header_id",
						"net_of_retainage_flag",
						"release_amount_net_of_tax",
						"control_amount",
						"party_id",
						"party_site_id",
						"pay_proc_trxn_type_code",
						"payment_function",
						"cust_registration_code",
						"cust_registration_number",
						"port_of_entry_code",
						"external_bank_account_id",
						"vendor_contact_id",
						"internal_contact_email",
						"disc_is_inv_less_tax_flag",
						"exclude_freight_from_discount",
						"pay_awt_group_id",
						"original_invoice_amount",
						"dispute_reason",
						"remit_to_supplier_name",
						"remit_to_supplier_id",
						"remit_to_supplier_site",
						"remit_to_supplier_site_id",
						"relationship_id",
						"po_matched_flag",
						"validation_worker_id",
						"source_system_code",
					},
					map[string]string {},
					map[string]string {}

			case "grp_dst":
				return "/Dev/data.ge/1/25/ORCL.GE_P2_GRP01_DST02._LEFT.GL_ATTRIBUTE_MULTI.dat",
					[]string{
						"id",
						"external_owner",
						"external_table",
						"processing_code",
						"a00",
						"a01",
						"a02",
						"a03",
						"a04",
						"a05",
						"a06",
						"a07",
						"a08",
						"a09",
						"a10",
						"a11",
						"a12",
						"a13",
						"a14",
						"a15",
						"a16",
						"a17",
						"a18",
						"a19",
						"a20",
						"a21",
						"a22",
						"a23",
						"a24",
						"a25",
						"a26",
						"a27",
						"a28",
						"a29",
						"a30",
					},
					map[string]string {},
					map[string]string {}*/

	}
	panic("code " + code + " is wrong")
}