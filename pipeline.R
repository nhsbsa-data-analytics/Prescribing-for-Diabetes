### Pipeline to run PCA annual publication
# clear environment
rm(list = ls())

# source functions
# this is only a temporary step until all functions are built into packages
source("./functions/functions.R")

# 1. Setup --------------------------------------------
# load GITHUB_KEY if available in environment or enter if not

if (Sys.getenv("GITHUB_PAT") == "") {
  usethis::edit_r_environ()
  stop(
    "You need to set your GITHUB_PAT = YOUR PAT KEY in the .Renviron file which pops up. Please restart your R Studio after this and re-run the pipeline."
  )
}

# load GITHUB_KEY if available in environment or enter if not

if (Sys.getenv("DB_DWCP_USERNAME") == "") {
  usethis::edit_r_environ()
  stop(
    "You need to set your DB_DWCP_USERNAME = YOUR DWCP USERNAME and  DB_DWCP_PASSWORD = YOUR DWCP PASSWORD in the .Renviron file which pops up. Please restart your R Studio after this and re-run the pipeline."
  )
}

#install nhsbsaUtils package first as need check_and_install_packages()
devtools::install_github("nhsbsa-data-analytics/nhsbsaUtils",
                         auth_token = Sys.getenv("GITHUB_PAT"))

library(nhsbsaUtils)

#install and library packages
req_pkgs <-
  c(
    "dplyr",
    "stringr",
    "data.table",
    "yaml",
    "openxlsx",
    "rmarkdown",
    "logr",
    "highcharter",
    "lubridate",
    "dbplyr",
    "tidyr",
    "janitor",
    "magrittr",
    "tcltk",
    "DT",
    "htmltools",
    "geojsonsf",
    "readxl",
    "nhsbsa-data-analytics/nhsbsaR",
    "nhsbsa-data-analytics/nhsbsaExternalData",
    "nhsbsa-data-analytics/accessibleTables",
    "nhsbsa-data-analytics/nhsbsaDataExtract",
    "nhsbsa-data-analytics/nhsbsaVis"
  )

#library/install packages as required
nhsbsaUtils::check_and_install_packages(req_pkgs)

# set up logging
lf <-
  logr::log_open(paste0(
    "Y:/Official Stats/PfD/log/pca_log",
    format(Sys.time(), "%d%m%y%H%M%S"),
    ".log"
  ))

# load config
config <- yaml::yaml.load_file("config.yml")
log_print("Config loaded", hide_notes = TRUE)
log_print(config, hide_notes = TRUE)

# load options
nhsbsaUtils::publication_options()
log_print("Options loaded", hide_notes = TRUE)

# 2. connect to DWH and pull max CY/FY  ---------
#build connection to warehouse
con <- nhsbsaR::con_nhsbsa(dsn = "FBS_8192k",
                           driver = "Oracle in OraClient19Home1",
                           "DWCP")


# 3. collect data from DWH ---------------------------------------------------
costpericb_data <-
  costpericb_extract(con = con,
                     schema = "KIGRA",
                     table = "PFD_FACT_202307") |>
  apply_sdc(rounding = F)

costper_patdata <-
  costper_patient_extract(con = con,
                          schema = "KIGRA",
                          table = "PFD_FACT_202307")  |>
  apply_sdc(rounding = F)

pfd_national_data <-
  national_extract(con = con,
                   schema = "KIGRA",
                   table = "PFD_FACT_202307") |>
  apply_sdc(rounding = F)

pfd_paragraph_data <-
  paragraph_extract(con = con,
                    schema = "KIGRA",
                    table = "PFD_FACT_202307") |>
  apply_sdc(rounding = F)

pfd_u18_data <-
  child_adult_extract(con = con,
                      schema = "KIGRA",
                      table = "PFD_FACT_202307") |>
  apply_sdc(rounding = F)

pfd_imd_data <-
  imd_extract(con = con,
              schema = "KIGRA",
              table = "PFD_FACT_202307") |>
  apply_sdc(rounding = F)

pfd_imd_paragraph_data <-
  imd_paragraph_extract(con = con,
                        schema = "KIGRA",
                        table = "PFD_FACT_202307") |>
  apply_sdc(rounding = F)

pfd_ageband_data <-
  ageband_extract(con = con,
                  schema = "KIGRA",
                  table = "PFD_FACT_202307") |>
  apply_sdc(rounding = F)

pfd_ageband_paragraph_data <-
  ageband_paragraph_extract(con = con,
                            schema = "KIGRA",
                            table = "PFD_FACT_202307") |>
  apply_sdc(rounding = F)

pfd_gender_data <-
  gender_extract(con = con,
                 schema = "KIGRA",
                 table = "PFD_FACT_202307") |>
  apply_sdc(rounding = F)

pfd_gender_paragraph_data <-
  gender_paragraph_extract(con = con,
                           schema = "KIGRA",
                           table = "PFD_FACT_202307") |>
  apply_sdc(rounding = F)

pfd_age_gender_data <-
  age_gender_extract(con = con,
                     schema = "KIGRA",
                     table = "PFD_FACT_202307") |>
  apply_sdc(rounding = F)

pfd_age_gender_paragraph_data <-
  age_gender_paragraph_extract(con = con,
                               schema = "KIGRA",
                               table = "PFD_FACT_202307") |>
  apply_sdc(rounding = F)

patient_identification_dt <-
  capture_rate_extract_dt(con = con,
                          schema = "KIGRA",
                          table = "PFD_FACT_202307") |>
  select(1, 2, last_col(4):last_col())

patient_identification <-
  capture_rate_extract(con = con,
                       schema = "KIGRA",
                       table = "PFD_FACT_202307")

pfd_national_overall <-
  tbl(con, dbplyr::in_schema("KIGRA", "PFD_FACT_OVERALL_202307")) |>
  collect() |>
  apply_sdc(rounding = F) |>
  select(
    `Financial Year` = FINANCIAL_YEAR,
    `Drug Type` = DRUG_TYPE,
    `Identified Patient Flag` = PATIENT_IDENTIFIED,
    `Total Identified Patients` = PATIENTS,
    `Total Items` = ITEMS,
    `Total Net Ingredient Cost (GBP)` = NIC
  ) |>
  mutate(`Total Identified Patients` = case_when(
    is.na(`Total Identified Patients`) ~ 0,
    TRUE ~ `Total Identified Patients`
  ))

cost_per_patienticb <- costpericb_data |>
  dplyr::ungroup() |>
  dplyr::filter(`Identified Patient Flag` == "Y") |>
  dplyr::mutate(`Total NIC per patient (GBP)` = `Total Net Ingredient Cost (GBP)` /
                  `Total Identified Patients`)  |>
  dplyr::select(
    `Financial Year`,
    `Integrated Care Board Name`,
    `Integrated Care Board Code`,
    `Total NIC per patient (GBP)`
  ) |>
  apply_sdc(rounding = F)


# 4. Build excel tables ---------------------------------------------------

sheetNames <- c(
  "Patient_Identification",
  "National_Total",
  "National_Paragraph",
  "Cost_per_ICB",
  "Cost_per_Patient"
)

wb <- accessibleTables::create_wb(sheetNames)

#create metadata tab (will need to open file and auto row heights once ran)
meta_fields <- c(
  "BNF Paragraph Code",
  "BNF Paragraph Name",
  "Financial Year",
  "Financial Quarter",
  "Identified Patient",
  "Integrated Care Board Code",
  "Integrated Care Board Name",
  "Total Items",
  "Total Net Ingredient Cost (GBP)",
  "Total NIC per patient (GBP)",
  "Total Patients",
  "Drug Type"
)

meta_descs <-
  c(
    "The unique code used to refer to the British National Formulary (BNF) paragraph.",
    "The name given to the British National Formulary (BNF) paragraph. This level of grouping of the BNF Therapeutical classification system sits below BNF section.",
    "The financial year to which the data belongs.",
    "The financial quarter to which the data belongs.",
    "This shows where an item has been attributed to an NHS number that has been verified by the Personal Demographics Service (PDS).",
    "The name given to the Integrated Care Board (ICB) that a prescribing organisation belongs to. This is based upon NHSBSA administrative records, not geographical boundaries and more closely reflect the operational organisation of practices than other geographical data sources.",
    "The unique code used to refer to an Integrated Care Board (ICB)",
    "The number of prescription items dispensed. 'Items' is the number of times a product appears on a prescription form. Prescription forms include both paper prescriptions and electronic messages.",
    "Total Net Ingredient Cost is the amount that would be paid using the basic price of the prescribed drug or appliance and the quantity prescribed. Sometimes called the 'Net Ingredient Cost' (NIC). The basic price is given either in the Drug Tariff or is determined from prices published by manufacturers, wholesalers or suppliers. Basic price is set out in Parts 8 and 9 of the Drug Tariff. For any drugs or appliances not in Part 8, the price is usually taken from the manufacturer, wholesaler or supplier of the product. This is given in GBP (Â£).",
    "Cost per patient has been calculated by dividing the total cost associated with identified patients by the number of patients that have been identified in that CCG per financial year.",
    "Where patients are identified via the flag, the number of patients that the data corresponds to. This will always be 0 where 'Identified Patient' = N.",
    "Based on the primary therapeutic use, this indicates whether an item is within the paragraphs associated with diabetes or whether the item is in another ('Non-diabetes') paragraph."
  )

accessibleTables::create_metadata(wb,
                                  meta_fields,
                                  meta_descs)

#### Patient identification
# write data to sheet
write_sheet(
  wb,
  "Patient_Identification",
  paste0(
    "Prescribing for Diabetes - 2015/16 to ",
    config$full_year,
    " - Proportion of items for which an NHS number was recorded (%)"
  ),
  c(
    "The below proportions reflect the percentage of prescription items where a PDS verified NHS number was recorded."
  ),
  patient_identification,
  42
)
#left align columns A to C
format_data(wb,
            "Patient_Identification",
            c("A", "B", "C"),
            "left",
            "")
#right align columns and round to 2 DP - D (if using long data not pivoting wider) (!!NEED TO UPDATE AS DATA EXPANDS!!)
format_data(wb,
            "Patient_Identification",
            c("D"),
            "right",
            "0.00")

#### Total items data
# write data to sheet
#suggest note 3. could be condensed to something like "Total costs and items may not match those in our Prescription Cost Analysis (PCA) publication, as they are based on a prescribing view while PCA uses a dispensing view instead."
write_sheet(
  wb,
  "National_Total",
  paste0(
    "Table 1: Prescribing for Diabetes - England 2015/16 to ",
    config$full_year,
    " total dispensed items and costs per financial year"
  ),
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients.",
    "3. Total costs and items may not be reconciled back to Prescribing Cost Analysis (PCA) publication figures as these figures are based around a 'prescribing view' of the data. This is where we use the drug or device that was prescribed to a patient, rather than the drug that was reimbursed to the dispenser to classify a prescription item. PCA uses a dispensing view where the inverse is true."
  ),
  pfd_national_overall,
  14
)

#left align columns A to C
format_data(wb,
            "National_Total",
            c("A", "B", "C"),
            "left",
            "")

#right align columns D and E and round to whole numbers with thousand separator
format_data(wb,
            "National_Total",
            c("D", "E"),
            "right",
            "#,##0")

#right align column F and round to 2dp with thousand separator
format_data(wb,
            "National_Total",
            c("F"),
            "right",
            "#,##0.00")

#### National Paragraph data
# write data to sheet
write_sheet(
  wb,
  "National_Paragraph",
  paste0(
    "Table 2: Prescribing for Diabetes - England 2015/16 to ",
    config$full_year,
    " yearly totals split by BNF paragraph and identified patients"
  ),
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients."
    
  ),
  pfd_paragraph_data,
  14
)
#left align columns A to D
format_data(wb,
            "National_Paragraph",
            c("A", "B", "C", "D"),
            "left",
            "")

#right align columns E and F and round to whole numbers with thousand separator
format_data(wb,
            "National_Paragraph",
            c("E", "F"),
            "right",
            "#,##0")

#right align column G and round to 2dp with thousand separator
format_data(wb,
            "National_Paragraph",
            c("G"),
            "right",
            "#,##0.00")

#### Cost per ICB data
# write data to sheet
write_sheet(
  wb,
  "Cost_per_ICB",
  paste0(
    "Table 3: Prescribing for Diabetes - England 2015/16 to ",
    config$full_year,
    "costs and items per ICB per financial year"
  ),
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients."
    
  ),
  costpericb_data |> filter(`Integrated Care Board Name` != "UNKNOWN ICB"),
  14
)

#left align columns A to D
format_data(wb,
            "Cost_per_ICB",
            c("A", "B", "C", "D"),
            "left",
            "")

#right align columns E and F and round to whole numbers with thousand separator
format_data(wb,
            "Cost_per_ICB",
            c("E", "F"),
            "right",
            "#,##0")

#right align column G and round to 2dp with thousand separator
format_data(wb,
            "Cost_per_ICB",
            c("G"),
            "right",
            "#,##0.00")

#### Cost per patient data
# write data to sheet
write_sheet(
  wb,
  "Cost_per_Patient",
  paste0(
    "Table 4: Prescribing for Diabetes - England 2015/16 to ",
    config$full_year,
    " average cost per patient per ICB per financial year"
  ),
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients.",
    "3. Integrated Care Boards (ICBs) succeeded sustainability and transformation plans (STPs) and replaced the functions of clinical commissioning groups (CCGs) in July 2022 with ICB sub locations replacing CCGs during the transition period of 2022/23. This table now displays data at ICB level to reflect the current intended structure.",
    "4. Only costs where the patient was known have been included in the Total NIC per patient (GBP) calculation. "
  ),
  cost_per_patienticb |> filter(`Integrated Care Board Name` != "UNKNOWN ICB"),
  14
)

#left align columns A to D
format_data(wb,
            "Cost_per_Patient",
            c("A", "B", "C"),
            "left",
            "")

#right align column E and round to 2dp with thousand separator
format_data(wb,
            "Cost_per_Patient",
            c("D"),
            "right",
            "#,##0.00")

# build cover sheet
accessibleTables::makeCoverSheet(
  paste0("Prescribing for Diabetes - England 2015/16 - ", config$full_year),
  "Costs and Items",
  "Publication date: 10 August 2023",
  wb,
  sheetNames,
  c(
    "Metadata",
    "Patient Identification Rates",
    "Table 1: Total items",
    "Table 2: Diabetes Items",
    "Table 3: Costs Per ICB",
    "Table 4: Costs Per Patient"
  ),
  c("Metadata", sheetNames)
)

#save file into outputs folder
openxlsx::saveWorkbook(wb,
                       "outputs/PfD_2022_2023_costs_and_items_v001.xlsx",
                       overwrite = TRUE)


rm(wb)


# 13. disconnect from DWH  ---------
DBI::dbDisconnect(con)
log_print("Disconnected from DWH", hide_notes = TRUE)

#close log
logr::log_close()