### Pipeline to run PfD annual publication
# clear environment
rm(list = ls())

# source functions
# this is only a temporary step until all functions are built into packages

# select all .R files in functions sub-folder
function_files <- list.files(path = "functions", pattern = "\\.R$")

# loop over function_files to source all files in functions sub-folder
for (file in function_files) {
  source(file.path("functions", file))
}

# 1. Setup ---------------------------------------------------------------------

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

install.packages("devtools")
library(devtools)

# install nhsbsaUtils package first as need check_and_install_packages()
devtools::install_github("nhsbsa-data-analytics/nhsbsaUtils",
                         auth_token = Sys.getenv("GITHUB_PAT"))

library(nhsbsaUtils)

# install and library packages
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
    "kableExtra",
    "nhsbsa-data-analytics/nhsbsaR",
    "nhsbsa-data-analytics/nhsbsaExternalData",
    "nhsbsa-data-analytics/accessibleTables",
    "nhsbsa-data-analytics/nhsbsaDataExtract",
    "nhsbsa-data-analytics/nhsbsaVis"
  )

# library/install packages as required
nhsbsaUtils::check_and_install_packages(req_pkgs)

# set up logging
# lf <-
#   logr::log_open(paste0(
#     "Y:/Official Stats/PfD/log/pfd_log",
#     format(Sys.time(), "%d%m%y%H%M%S"),
#     ".log"
#   ))

# load config
config <- yaml::yaml.load_file("config.yml")

# log_print("Config loaded", hide_notes = TRUE)
# log_print(config, hide_notes = TRUE)

# load options
nhsbsaUtils::publication_options()
# log_print("Options loaded", hide_notes = TRUE)

# 2. connect to DWH and pull max CY/FY  ----------------------------------------

# build connection to warehouse
con <- nhsbsaR::con_nhsbsa(dsn = "FBS_8192k",
                           driver = "Oracle in OraClient19Home1",
                           "DWCP")

# 3. collect data from data warehouse ------------------------------------------

cost_per_icb_data <-
  cost_per_icb_extract(con = con,
                       schema = "OST",
                       table = "PFD_FACT_202407") |>
  apply_sdc(rounding = F,
            suppress_column = "Total Identified Patients")

icb_name_lookup <- cost_per_icb_data |>
  select(`Integrated Care Board Code`, `Integrated Care Board Name`) |>
  distinct()

cost_per_icb_overall_data <-
  tbl(con, dbplyr::in_schema("OST", "PFD_OVERALL_ICB_FACT_202407")) |>
  collect() |>
  arrange(FINANCIAL_YEAR,
          ICB_CODE,
          DRUG_TYPE,
          desc(PATIENT_IDENTIFIED)) |>
  select(
    `Financial Year` = FINANCIAL_YEAR,
    `Drug Type` = DRUG_TYPE,
    `Integrated Care Board Code` = ICB_CODE,
    `Identified Patient Flag` = PATIENT_IDENTIFIED,
    `Total Identified Patients` = PATIENTS,
    `Total Items` = ITEMS,
    `Total Net Ingredient Cost (GBP)` = NIC
  ) |>
  mutate(
    `Total Identified Patients` = case_when(
      `Identified Patient Flag` == "N" ~ 0,
      TRUE ~ `Total Identified Patients`
    )
  ) |>
  left_join(icb_name_lookup) |>
  relocate(`Integrated Care Board Name`, .after = `Integrated Care Board Code`) |>
  apply_sdc(rounding = F,
            suppress_column = "Total Identified Patients")

cost_per_pat_data <-
  cost_per_patient_extract(con = con,
                           schema = "OST",
                           table = "PFD_FACT_202407")  |>
  apply_sdc(rounding = F,
            suppress_column = "Total Identified Patients")

pfd_national_data <-
  national_extract(con = con,
                   schema = "OST",
                   table = "PFD_FACT_202407") |>
  apply_sdc(rounding = F,
            suppress_column = "Total Identified Patients")

pfd_paragraph_data <-
  paragraph_extract(con = con,
                    schema = "OST",
                    table = "PFD_FACT_202407") |>
  apply_sdc(rounding = F,
            suppress_column = "Total Identified Patients")

pfd_u18_data <-
  child_adult_extract(con = con,
                      schema = "OST",
                      table = "PFD_FACT_202407") |>
  apply_sdc(rounding = F,
            suppress_column = "Total Identified Patients")

pfd_imd_data <-
  imd_extract(con = con,
              schema = "OST",
              table = "PFD_FACT_202407") |>
  apply_sdc(rounding = F,
            suppress_column = "Total Identified Patients")


pfd_imd_paragraph_data <-
  imd_paragraph_extract(con = con,
                        schema = "OST",
                        table = "PFD_FACT_202407") |>
  apply_sdc(rounding = F,
            suppress_column = "Total Identified Patients")

pfd_ageband_data <-
  ageband_extract(con = con,
                  schema = "OST",
                  table = "PFD_FACT_202407") |>
  apply_sdc(rounding = F,
            suppress_column = "Total Identified Patients")

pfd_ageband_paragraph_data <-
  ageband_paragraph_extract(con = con,
                            schema = "OST",
                            table = "PFD_FACT_202407") |>
  apply_sdc(rounding = F,
            suppress_column = "Total Identified Patients")

pfd_gender_data <-
  gender_extract(con = con,
                 schema = "OST",
                 table = "PFD_FACT_202407") |>
  apply_sdc(rounding = F,
            suppress_column = "Total Identified Patients")

pfd_gender_paragraph_data <-
  gender_paragraph_extract(con = con,
                           schema = "OST",
                           table = "PFD_FACT_202407") |>
  apply_sdc(rounding = F,
            suppress_column = "Total Identified Patients")

pfd_age_gender_data <-
  age_gender_extract(con = con,
                     schema = "OST",
                     table = "PFD_FACT_202407") |>
  apply_sdc(rounding = F,
            suppress_column = "Total Identified Patients")

pfd_age_gender_paragraph_data <-
  age_gender_paragraph_extract(con = con,
                               schema = "OST",
                               table = "PFD_FACT_202407") |>
  apply_sdc(rounding = F,
            suppress_column = "Total Identified Patients")

pfd_national_presentation <- national_presentation(con = con,
                                                   schema = "OST",
                                                   table = "PFD_FACT_202407") 

patient_identification_dt <-
  capture_rate_extract_dt(con = con,
                          schema = "OST",
                          table = "PFD_FACT_202407") |>
  select(1, 2, last_col(4):last_col())

patient_identification <-
  capture_rate_extract(con = con,
                       schema = "OST",
                       table = "PFD_FACT_202407")

pfd_national_overall <-
  tbl(con, dbplyr::in_schema("OST", "PFD_FACT_OVERALL_202406")) |>
  collect() |>
  select(
    `Financial Year` = FINANCIAL_YEAR,
    `Drug Type` = DRUG_TYPE,
    `Identified Patient Flag` = PATIENT_IDENTIFIED,
    `Total Identified Patients` = PATIENTS,
    `Total Items` = ITEMS,
    `Total Net Ingredient Cost (GBP)` = NIC
  ) |>
  mutate(`Total Identified Patients` = case_when(
    `Identified Patient Flag` == "N" ~ 0,
    TRUE ~ `Total Identified Patients`
  )) |>
  apply_sdc(rounding = F,
            suppress_column = "Total Identified Patients")

cost_per_patient_icb <- cost_per_icb_data |>
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
  apply_sdc(rounding = F,
            suppress_column = "Total Identified Patients")

# 4. Build costs/items excel tables --------------------------------------------

sheetNames <- c(
  "Patient_Identification",
  "National_Total",
  "National_Paragraph",
  "Cost_per_ICB",
  "Cost_per_Patient",
  "National_Presentation"
)

wb <- accessibleTables::create_wb(sheetNames)

# create metadata tab (will need to open file and auto row heights once ran)
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
# left align columns A to C
format_data(wb,
            "Patient_Identification",
            c("A", "B", "C"),
            "left",
            "")

# right align columns and round to 2 DP - D (if using long data not pivoting wider) (!!NEED TO UPDATE AS DATA EXPANDS!!)
format_data(wb,
            "Patient_Identification",
            c("D"),
            "right",
            "0.00")

#### Total items data
# write data to sheet
# suggest note 3. could be condensed to something like "Total costs and items may not match those in our Prescription Cost Analysis (PCA) publication, as they are based on a prescribing view while PCA uses a dispensing view instead."
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

# left align columns A to C
format_data(wb,
            "National_Total",
            c("A", "B", "C"),
            "left",
            "")

# right align columns D and E and round to whole numbers with thousand separator
format_data(wb,
            "National_Total",
            c("D", "E"),
            "right",
            "#,##0")

# right align column F and round to 2dp with thousand separator
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
# left align columns A to D
format_data(wb,
            "National_Paragraph",
            c("A", "B", "C", "D"),
            "left",
            "")

# right align columns E and F and round to whole numbers with thousand separator
format_data(wb,
            "National_Paragraph",
            c("E", "F"),
            "right",
            "#,##0")

# right align column G and round to 2dp with thousand separator
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
    " costs and items per ICB per financial year"
  ),
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients."
    
  ),
  cost_per_icb_overall_data,
  14
)

# left align columns A to D
format_data(wb,
            "Cost_per_ICB",
            c("A", "B", "C", "D", "E"),
            "left",
            "")

# right align columns E and F and round to whole numbers with thousand separator
format_data(wb,
            "Cost_per_ICB",
            c("F", "G"),
            "right",
            "#,##0")

# right align column G and round to 2dp with thousand separator
format_data(wb,
            "Cost_per_ICB",
            c("H"),
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
  cost_per_patient_icb |> filter(`Integrated Care Board Name` != "UNKNOWN ICB"),
  14
)

# left align columns A to C
format_data(wb,
            "Cost_per_Patient",
            c("A", "B", "C"),
            "left",
            "")

# right align column D and round to 2dp with thousand separator
format_data(wb,
            "Cost_per_Patient",
            c("D"),
            "right",
            "#,##0.00")

#### national presentation
# write data to sheet
write_sheet(
  wb,
  "National_Presentation",
  paste0(
    "Table 5: Prescribing for Diabetes - England 2015/16 to ",
    config$full_year,
    " total items and costs by BNF Presentation"
  ),
  c("1. Field definitions can be found on the 'Metadata' tab."),
  pfd_national_presentation,
  14
)

# left align columns A to I
format_data(wb,
            "National_Presentation",
            c("A", "B", "C", "D", "E", "F", "G", "H", "I"),
            "left",
            "")

format_data(wb,
            "National_Presentation",
            c("J"),
            "right",
            "#,##0")

# right align column K and round to 2dp with thousand separator
format_data(wb,
            "National_Presentation",
            c("K"),
            "right",
            "#,##0.00")


# build cover sheet
accessibleTables::makeCoverSheet(
  paste0("Prescribing for Diabetes - England 2015/16 - ", config$full_year),
  "Costs and Items",
  paste0("Publication date: ", config$publication_date),
  wb,
  sheetNames,
  c(
    "Metadata",
    "Patient Identification Rates",
    "Table 1: Total items",
    "Table 2: Diabetes Items",
    "Table 3: Costs Per ICB",
    "Table 4: Costs Per Patient",
    "Table 5: Costs and items by BNF presentation"
  ),
  c("Metadata", sheetNames)
)

# save file into outputs folder
openxlsx::saveWorkbook(wb,
                       "outputs/PfD_2023_2024_costs_and_items_v001.xlsx",
                       overwrite = TRUE)

rm(wb)

# 5. Build patient demographics excel tables -----------------------------------

sheetNames <- c(
  "Patient_Identification",
  "National_Total",
  "Gender",
  "Dispensing_By_Gender",
  "Age_Band",
  "Dispensing_By_Age",
  "Age_Band_and_Gender",
  "Dispensing_By_Age_and_Gender",
  "Adults_and_Children",
  "Indices_of_Deprivation",
  "IMD_By_Drug"
)

wb <- create_wb(sheetNames)

# create metadata tab (will need to open file and auto row heights once ran)
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
  "Drug Type",
  "Patient Gender",
  "Age Band",
  "IMD Decile"
  
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
    "Based on the primary therapeutic use, this indicates whether an item is within the paragraphs associated with diabetes or whether the item is in another ('Non-diabetes') paragraph.",
    "The gender of the patient as noted at the time the prescription was processed. This includes where the patient has been identified but the gender has not been recorded.",
    "The age band of the patient as of the 30th September of the corresponding financial year the drug was prescribed.",
    "The IMD decile of the patient, based on the location of their practice, where '1' is the 10% of areas with the highest deprivation score in the Index of Multiple Deprivation (IMD) from the English Indices of Deprivation 2019, and '10' is the 10% of areas with the lowest IMD deprivation score. Unknown values are where the items are attributed to an unidentified practice within a Primary Care Organisation (PCO), or where we have been unable to match the practice postcode to a postcode in the National Statistics Postcode Lookup (NSPL)."
    
  )

create_metadata(wb,
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
# left align columns A to C
format_data(wb,
            "Patient_Identification",
            c("A", "B", "C"),
            "left",
            "")
# right align columns and round to 2 DP - D (if using long data not pivoting wider) (!!NEED TO UPDATE AS DATA EXPANDS!!)
format_data(wb,
            "Patient_Identification",
            c("D"),
            "right",
            "0.00")

### Total items data
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

# left align columns A to C
format_data(wb,
            "National_Total",
            c("A", "B", "C"),
            "left",
            "")

# right align columns D and E and round to whole numbers with thousand separator
format_data(wb,
            "National_Total",
            c("D", "E"),
            "right",
            "#,##0")

# right align column F and round to 2dp with thousand separator
format_data(wb,
            "National_Total",
            c("F"),
            "right",
            "#,##0.00")

#### National Sex data
# write data to sheet
write_sheet(
  wb,
  "Gender",
  paste0(
    "Table 2: Prescribing for Diabetes - England 2015/16 to ",
    config$full_year,
    " national prescribing by gender per financial year"
  ),
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. It is possible for a patient to be marked as 'unknown' or 'indeterminate'. Due to the low number of patients that these two groups contain the NHSBSA has decided to group these classifications together.",
    "3. The NHSBSA does not use the latest national data standard relating to patient gender, and use historic nomenclature in some cases. Please see the detailed Background Information and Methodology notice released with this publication for further information."
  ),
  pfd_gender_data,
  14
)

# left align columns A to D
format_data(wb,
            "Gender",
            c("A", "B", "C"),
            "left",
            "")

# right align columns D and E and round to whole numbers with thousand separator
format_data(wb,
            "Gender",
            c("D", "E"),
            "right",
            "#,##0")

# right align column F and round to 2dp with thousand separator
format_data(wb,
            "Gender",
            c("F"),
            "right",
            "#,##0.00")

#### Paragraph Sex data
# write data to sheet
write_sheet(
  wb,
  "Dispensing_By_Gender",
  paste0(
    "Table 3: Prescribing for Diabetes - England 2015/16 to ",
    config$full_year,
    " yearly patients, items and costs by BNF paragraph split by patient Gender"
  ),
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. It is possible for a patient to be marked as 'unknown' or 'indeterminate'. Due to the low number of patients that these two groups contain the NHSBSA has decided to group these classifications together.",
    "3. The NHSBSA does not use the latest national data standard relating to patient gender, and use historic nomenclature in some cases. Please see the detailed Background Information and Methodology notice released with this publication for further information.",
    "4. Statistical disclosure control is applied where prescribing relates to fewer than 5 patients. These cells will appear blank."
    
  ),
  pfd_gender_paragraph_data,
  14
)

# left align columns A to E
format_data(wb,
            "Dispensing_By_Gender",
            c("A", "B", "C", "D", "E"),
            "left",
            "")

# right align columns F and G and round to whole numbers with thousand separator
format_data(wb,
            "Dispensing_By_Gender",
            c("F", "G"),
            "right",
            "#,##0")

# right align column H and round to 2dp with thousand separator
format_data(wb,
            "Dispensing_By_Gender",
            c("H"),
            "right",
            "#,##0.00")

#### National age data
# write data to sheet
write_sheet(
  wb,
  "Age_Band",
  paste0(
    "Table 4: Prescribing for Diabetes - England 2015/16 to ",
    config$full_year,
    " National prescribing by age per financial year"
  ),
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. A patient's age is calculated at 30 September of the given year in order to be assigned to an age band. Some patients do not hold a date of birth and are assigned an 'unknown' age band."
    
  ),
  pfd_ageband_data,
  14
)

# left align columns A to D
format_data(wb,
            "Age_Band",
            c("A", "B", "C"),
            "left",
            "")

# right align columns D and E and round to whole numbers with thousand separator
format_data(wb,
            "Age_Band",
            c("D", "E"),
            "right",
            "#,##0")

# right align column F and round to 2dp with thousand separator
format_data(wb,
            "Age_Band",
            c("F"),
            "right",
            "#,##0.00")

#### Paragraph Age data
# write data to sheet
write_sheet(
  wb,
  "Dispensing_By_Age",
  paste0(
    "Table 5: Prescribing for Diabetes - England 2015/16 to ",
    config$full_year,
    " yearly patients, items and costs by BNF paragraph split by patient gender"
  ),
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients.",
    "3. Statistical disclosure control is applied where prescribing relates to fewer than 5 patients. These cells will appear blank."
  ),
  pfd_ageband_paragraph_data,
  14
)

# left align columns A to E
format_data(wb,
            "Dispensing_By_Age",
            c("A", "B", "C", "D", "E"),
            "left",
            "")

# right align columns F and G and round to whole numbers with thousand separator
format_data(wb,
            "Dispensing_By_Age",
            c("F", "G"),
            "right",
            "#,##0")

# right align column H and round to 2dp with thousand separator
format_data(wb,
            "Dispensing_By_Age",
            c("H"),
            "right",
            "#,##0.00")

#### National age sex data
# write data to sheet
write_sheet(
  wb,
  "Age_Band_and_Gender",
  paste0(
    "Table 6: Prescribing for Diabetes - England 2015/16 to ",
    config$full_year,
    " national prescribing by age and gender per financial year"
  ),
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. These totals only include patients where gender is known."
    
  ),
  pfd_age_gender_data,
  14
)

# left align columns A to D
format_data(wb,
            "Age_Band_and_Gender",
            c("A", "B", "C", "D"),
            "left",
            "")

# right align columns E and F and round to whole numbers with thousand separator
format_data(wb,
            "Age_Band_and_Gender",
            c("E", "F"),
            "right",
            "#,##0")

# right align column G and round to 2dp with thousand separator
format_data(wb,
            "Age_Band_and_Gender",
            c("G"),
            "right",
            "#,##0.00")

#### Paragraph Age Gender data
# write data to sheet
write_sheet(
  wb,
  "Dispensing_By_Age_and_Gender",
  paste0(
    "Table 7: Prescribing for Diabetes - England 2015/16 to ",
    config$full_year,
    " yearly patients, items and costs by BNF paragraph split by patient age and gender"
  ),
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients.",
    "3. These totals only include patients where gender is known.",
    "4. Statistical disclosure control is applied where prescribing relates to fewer than 5 patients. These cells will appear blank."
  ),
  pfd_age_gender_paragraph_data,
  14
)

# left align columns A to F
format_data(wb,
            "Dispensing_By_Age_and_Gender",
            c("A", "B", "C", "D", "E", "F"),
            "left",
            "")

# right align columns G and H and round to whole numbers with thousand separator
format_data(wb,
            "Dispensing_By_Age_and_Gender",
            c("G", "H"),
            "right",
            "#,##0")

# right align column I and round to 2dp with thousand separator
format_data(wb,
            "Dispensing_By_Age_and_Gender",
            c("I"),
            "right",
            "#,##0.00")

#### National u18 data
# write data to sheet
write_sheet(
  wb,
  "Adults_and_Children",
  paste0(
    "Table 8: Prescribing for Diabetes - England 2015/16 to ",
    config$full_year,
    " prescribing by adult/child split"
  ),
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. A patient's age is calculated at 30 September of the given year in order to be assigned to an age band. Some patients do not hold a date of birth and are assigned an 'unknown' age band."
    
  ),
  pfd_u18_data,
  14
)

# left align columns A to C
format_data(wb,
            "Adults_and_Children",
            c("A", "B", "C"),
            "left",
            "")

# right align columns D and E and round to whole numbers with thousand separator
format_data(wb,
            "Adults_and_Children",
            c("D", "E"),
            "right",
            "#,##0")

format_data(wb,
            "Adults_and_Children",
            c("F"),
            "right",
            "#,##0.00")

#### National imd data
# write data to sheet
write_sheet(
  wb,
  "Indices_of_Deprivation",
  paste0(
    "Table 9: Prescribing for Diabetes - England 2015/16 to ",
    config$full_year,
    " yearly patients, items and costs by IMD Decile"
  ),
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. IMD deciles used are taken from the English Indices of Deprivaton 2019 National Statistics publication.",
    "3. Where a patient's postcode has not been able to to be matched to NSPL or the patient has not been identified the records are reported as 'unknown' IMD quintile.",
    "4. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients."
    
  ),
  pfd_imd_data,
  14
)

# left align columns A to B
format_data(wb,
            "Indices_of_Deprivation",
            c("A", "B"),
            "left",
            "")

# right align columns C and D and round to whole numbers with thousand separator
format_data(wb,
            "Indices_of_Deprivation",
            c("C", "D"),
            "right",
            "#,##0")

# right align column E and round to 2dp with thousand separator
format_data(wb,
            "Indices_of_Deprivation",
            c("E"),
            "right",
            "#,##0.00")

#### Paragraph imd data
# write data to sheet
write_sheet(
  wb,
  "IMD_By_Drug",
  paste0(
    "Table 10: Prescribing for Diabetes - England 2015/16 to ",
    config$full_year,
    " yearly patients, items and costs by BNF paragraph split by patient gender"
  ),
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. IMD deciles used are taken from the English Indices of Deprivaton 2019 National Statistics publication.",
    "3. Where a patient's postcode has not been able to to be matched to NSPL or the patient has not been identified the records are reported as 'unknown' IMD quintile.",
    "4. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients.",
    "5. Statistical disclosure control is applied where prescribing relates to fewer than 5 patients. These cells will appear blank."
  ),
  pfd_imd_paragraph_data,
  14
)

# left align columns A to D
format_data(wb,
            "IMD_By_Drug",
            c("A", "B", "C", "D"),
            "left",
            "")

# right align columns E and F and round to whole numbers with thousand separator
format_data(wb,
            "IMD_By_Drug",
            c("E", "F"),
            "right",
            "#,##0")

# right align column G and round to 2dp with thousand separator
format_data(wb,
            "IMD_By_Drug",
            c("G"),
            "right",
            "#,##0.00")

accessibleTables::makeCoverSheet(
  paste0("Prescribing for Diabetes - England 2015/16 - ", config$full_year),
  "Patient Demographics",
  paste0("Publication date: ", config$publication_date),
  wb,
  sheetNames,
  c(
    "Metadata",
    "Patient Identification Rates",
    "Table 1: Total Items",
    "Table 2: Gender",
    "Table 3: Dispensing By Gender",
    "Table 4: Age Band",
    "Table 5: Dispensing By Age",
    "Table 6: Age Band and Gender",
    "Table 7: Dispensing By Age and Gender",
    "Table 8: Adults and Children",
    "Table 9: Indices of Deprivation",
    "Table 10: IMD By Drug"
  ),
  c("Metadata", sheetNames)
)

# save file into outputs folder
openxlsx::saveWorkbook(wb,
                       "outputs/PfD_2023_2024_patient_demographics_v001.xlsx",
                       overwrite = TRUE)

# 6. build charts and data -----------------------------------------------------

table_1 <- patient_identification_dt |>
  mutate(across(where(is.numeric), round, 2)) |>
  mutate(across(where(is.numeric), format, nsmall = 2)) |>
  mutate(across(contains("20"), ~ paste0(.x, "%")))

table_1_data <- patient_identification |>
  rename_with(~ gsub(" ", "_", toupper(gsub(
    "[^[:alnum:] ]", "", .
  ))), everything())

figure_1_data <- pfd_national_overall |>
  filter(`Drug Type` == "Diabetes") |>
  group_by(`Financial Year`) |>
  summarise(
    `Prescription items` = sum(`Total Items`),
    `Identified patients` = sum(`Total Identified Patients`),
    .groups = "drop"
  ) |>
  pivot_longer(
    cols = c(`Prescription items`, `Identified patients`),
    names_to = "Measure",
    values_to = "Value"
  ) |>
  rename_with(~ gsub(" ", "_", toupper(gsub(
    "[^[:alnum:] ]", "", .
  ))), everything())

figure_1 <- group_chart_hc(
  data = figure_1_data,
  x = FINANCIAL_YEAR,
  y = VALUE,
  group = MEASURE,
  type = "line",
  xLab = "Financial year",
  yLab = "Number of prescription items/identified patients",
  title = ""
) |>
  hc_subtitle(text = "M = Millions",
              align = "left")

table_2 <- figure_1_data |>
  mutate(VALUE = format(VALUE, big.mark = ",")) |>
  rename("Financial year" = 1,
         "Measure" = 2,
         "Value" = 3)


figure_2_data <- pfd_national_overall |>
  filter(`Drug Type` == "Diabetes") |>
  group_by(`Financial Year`) |>
  summarise(
    `Total Net Ingredient Cost (GBP)` = sum(`Total Net Ingredient Cost (GBP)`),
    .groups = "drop"
  ) |>
  rename_with(~ gsub(" ", "_", toupper(gsub(
    "[^[:alnum:] ]", "", .
  ))), everything())

figure_2 <- basic_chart_hc(
  figure_2_data,
  x = FINANCIAL_YEAR,
  y = TOTAL_NET_INGREDIENT_COST_GBP,
  type = "line",
  xLab = "Financial year",
  yLab = "Cost (GBP)",
  title = "",
  currency = TRUE
) |>
  hc_subtitle(text = "M = Millions",
              align = "left") |>
  hc_yAxis(min = 700000000)

figure_2$x$hc_opts$xAxis$lineWidth <- 1
figure_2$x$hc_opts$xAxis$lineColor <- "#E8EDEE"

figure_3_data <- pfd_national_overall |>
  group_by(`Financial Year`, `Drug Type`) |>
  
  summarise(
    `Total Items` = sum(`Total Items`),
    `Total Net Ingredient Cost (GBP)` = sum(`Total Net Ingredient Cost (GBP)`),
    .groups = "drop"
  ) |>
  group_by(`Financial Year`) |>
  mutate(
    `Proportion of items` = `Total Items` / sum(`Total Items`) * 100,
    `Proportion of costs` = `Total Net Ingredient Cost (GBP)` / sum(`Total Net Ingredient Cost (GBP)`) * 100
  ) |>
  ungroup() |>
  filter(`Drug Type` == "Diabetes") |>
  select(-`Total Items`,-`Total Net Ingredient Cost (GBP)`) |>
  pivot_longer(
    cols = c(`Proportion of items`, `Proportion of costs`),
    names_to = "measure",
    values_to = "value"
  ) |>
  rename_with(~ gsub(" ", "_", toupper(gsub(
    "[^[:alnum:] ]", "", .
  ))), everything())

figure_3 <- group_chart_hc(
  figure_3_data,
  x = FINANCIAL_YEAR,
  y = VALUE,
  group = MEASURE,
  type = "line",
  xLab = "Financial year",
  yLab = "Proportion (%)",
  title = ""
)

figure_4_data <- pfd_paragraph_data |>
  group_by(`Financial Year`, `BNF Paragraph Name`) |>
  summarise(`Total Items` = sum(`Total Items`),
            .groups = "drop") |>
  pivot_longer(
    cols = c(`Total Items`),
    names_to = "measure",
    values_to = "value"
  ) |>
  rename_with(~ gsub(" ", "_", toupper(gsub(
    "[^[:alnum:] ]", "", .
  ))), everything()) |>
  mutate(ROUNDED_VALUE = signif(VALUE, 3))

figure_4 <- group_chart_hc(
  figure_4_data,
  x = FINANCIAL_YEAR,
  y = ROUNDED_VALUE,
  group = BNF_PARAGRAPH_NAME,
  type = "line",
  xLab = "Financial year",
  yLab = "Number of prescribed items",
  title = "",
  dlOn = F
) |>
  hc_tooltip(enabled = TRUE,
             shared = TRUE,
             sort = TRUE) |>
  hc_legend(enabled = TRUE) |>
  hc_subtitle(text = "M = Millions",
              align = "left")

figure_5_data <- pfd_paragraph_data |>
  group_by(`Financial Year`, `BNF Paragraph Name`) |>
  summarise(
    `Total Net Ingredient Cost (GBP)` = sum(`Total Net Ingredient Cost (GBP)`),
    .groups = "drop"
  ) |>
  pivot_longer(
    cols = c(`Total Net Ingredient Cost (GBP)`),
    names_to = "measure",
    values_to = "value"
  ) |>
  rename_with(~ gsub(" ", "_", toupper(gsub(
    "[^[:alnum:] ]", "", .
  ))), everything()) |>
  mutate(ROUNDED_VALUE = signif(VALUE, 3))

figure_5 <- group_chart_hc(
  figure_5_data,
  x = FINANCIAL_YEAR,
  y = ROUNDED_VALUE,
  group = BNF_PARAGRAPH_NAME,
  type = "line",
  xLab = "Financial year",
  yLab = "Cost (GBP)",
  title = "",
  dlOn = F
) |>
  hc_tooltip(enabled = TRUE,
             shared = TRUE,
             sort = TRUE) |>
  hc_legend(enabled = TRUE) |>
  hc_subtitle(text = "M = Millions",
              align = "left")

figure_6_data <- pfd_national_overall |>
  filter(`Identified Patient Flag` == "Y", `Drug Type` == "Diabetes") |>
  rename_with(~ gsub(" ", "_", toupper(gsub(
    "[^[:alnum:] ]", "", .
  ))), everything()) |>
  mutate(ITEMS_PER_PATIENT = TOTAL_ITEMS  / TOTAL_IDENTIFIED_PATIENTS)

figure_6 <- basic_chart_hc(
  figure_6_data,
  x = FINANCIAL_YEAR,
  y = ITEMS_PER_PATIENT,
  type = "line",
  xLab = "Financial year",
  yLab = "Prescription items per patient",
  title = ""
) |>
  hc_yAxis(min = 15)

figure_6$x$hc_opts$yAxis$tickPositioner <- JS(
  "function() {
                         var positions = [],
                         tick = Math.floor(this.dataMin - 1) * 1;
                         for (; tick - 1 <= this.dataMax; tick += 1) {
                         positions.push(tick);
                         }
                         return positions;
                         }"
)

figure_6$x$hc_opts$xAxis$lineWidth <- 1
figure_6$x$hc_opts$xAxis$lineColor <- "#E8EDEE"

figure_7_data_boxplot <- cost_per_icb_data |>
  group_by(`Financial Year`) |>
  filter(`Identified Patient Flag` == "Y",
         `Integrated Care Board Name` != "UNKNOWN ICB") |>
  mutate(COST_PER_PAT = `Total Net Ingredient Cost (GBP)`  / `Total Identified Patients`) |>
  data_to_boxplot(
    var = COST_PER_PAT,
    add_outliers = T,
    group_var = `Financial Year`,
    color = "#005EB8",
    fillColor = "rgba(0,94,184,0.5)"
  )

figure_7_data_raw <- data.frame()

for (i in 1:length(figure_7_data_boxplot$data[[1]])) {
  FINANCIAL_YEAR = figure_7_data_boxplot$data[[1]][[i]]$name
  MINIMUM = figure_7_data_boxplot$data[[1]][[i]]$low
  LOWER_QUARTILE = figure_7_data_boxplot$data[[1]][[i]]$q1
  MEDIAN = figure_7_data_boxplot$data[[1]][[i]]$median
  UPPER_QUARTILE = figure_7_data_boxplot$data[[1]][[i]]$q3
  MAXIMUM = figure_7_data_boxplot$data[[1]][[i]]$high
  
  tmp_df <- data.frame(
    FINANCIAL_YEAR = FINANCIAL_YEAR,
    MINIMUM = MINIMUM,
    LOWER_QUARTILE = LOWER_QUARTILE,
    MEDIAN = MEDIAN,
    UPPER_QUARTILE = UPPER_QUARTILE,
    MAXIMUM = MAXIMUM
  )
  
  figure_7_data_raw <- figure_7_data_raw |>
    bind_rows(tmp_df)
}

tooltip <- JS(
  "function () {

              var result = '<b>' + this.point.options.name + '</b>' +
              '<br>Maximum: <b>£' + this.point.options.high.toFixed(2) +
              '</b><br>Upper quartile: <b>£' + this.point.options.q3.toFixed(2) +
              '</b><br>Median: <b>£' + this.point.options.median.toFixed(2) +
              '</b><br>Lower quartile: <b>£' + this.point.options.q1.toFixed(2) +
              '</b><br> Minimum: <b>£' + this.point.options.low.toFixed(2) + '</b>';

              return result

              }"
)

figure_7 <- highchart() |>
  hc_chart(style = list(fontFamily = "Arial")) |>
  hc_xAxis(type = "category",
           title = list(text = "Financial year")) |>
  hc_yAxis(min = 0,
           title = list(text = "Cost per patient (GBP)")) |>
  hc_add_series_list(figure_7_data_boxplot) |>
  hc_legend(enabled = FALSE) |>
  hc_title(text = "",
           style = list(fontSize = "16px",
                        fontWeight = "bold")) |>
  hc_tooltip(formatter = tooltip) |>
  hc_credits(enabled = TRUE)

figure_7$x$hc_opts$xAxis$lineWidth <- 1.5
figure_7$x$hc_opts$xAxis$lineColor <- "#768692"
figure_7$x$hc_opts$xAxis$tickWidth <- 1
figure_7$x$hc_opts$xAxis$tickColor <- "#768692"
figure_7$x$hc_opts$xAxis$tickmarkPlacement <- "on"

figure_8_data <- pfd_gender_data |>
  filter(`Patient Gender` != "Unknown") |>
  group_by(`Financial Year`, `Patient Gender`) |>
  summarise(`Total Identified Patients` = sum(`Total Identified Patients`)) |>
  pivot_longer(
    cols = c(`Total Identified Patients`),
    names_to = "measure",
    values_to = "value"
  ) |>
  rename_with(~ gsub(" ", "_", toupper(gsub(
    "[^[:alnum:] ]", "", .
  ))), everything()) |>
  mutate(ROUNDED_VALUE = signif(VALUE, 3))

figure_8 <- group_chart_hc(
  figure_8_data,
  x = FINANCIAL_YEAR,
  y = VALUE,
  group = PATIENT_GENDER,
  type = "line",
  xLab = "Financial year",
  yLab = "Number of identified patients",
  title = "",
  dlOn = F
) |>
  hc_tooltip(enabled = T,
             shared = T,
             sort = T)

figure_9_data <- pfd_age_gender_data |>
  filter(`Financial Year` == max(`Financial Year`)) |>
  rename_with(~ gsub(" ", "_", toupper(gsub(
    "[^[:alnum:] ]", "", .
  ))), everything())

figure_9 <-  age_gender_chart(figure_9_data,
                              labels = FALSE)

figure_10_data <- pfd_imd_data |>
  ungroup() |>
  filter(`Financial Year` == max(`Financial Year`),
         `IMD Quintile` != "Unknown") |>
  arrange(`IMD Quintile`) |>
  pivot_longer(
    cols = c(`Total Identified Patients`),
    names_to = "measure",
    values_to = "value"
  ) |>
  rename_with(~ gsub(" ", "_", toupper(gsub(
    "[^[:alnum:] ]", "", .
  ))), everything()) |>
  mutate(ROUNDED_VALUE = signif(VALUE, 3)) |>
  select(-TOTAL_ITEMS, -TOTAL_NET_INGREDIENT_COST_GBP)

figure_10 <-  basic_chart_hc(
  figure_10_data,
  x = IMD_QUINTILE,
  y = ROUNDED_VALUE,
  type = "column",
  xLab = "IMD quintile",
  yLab = "Number of identified patients",
  title = ""
)

table_2_data <- pfd_u18_data |>
  filter(`Age Band` != "Unknown") |>
  group_by(`Financial Year`, `Age Band`) |>
  ungroup() |>
  rename_with(~ gsub(" ", "_", toupper(gsub(
    "[^[:alnum:] ]", "", .
  ))), everything()) |>
  select(-TOTAL_ITEMS, -TOTAL_NET_INGREDIENT_COST_GBP)

# 7. create markdowns ----------------------------------------------------------

# save narrative summary as html file into outputs folder
# change file path to save somewhere else if needed
rmarkdown::render("pfd_narrative.Rmd",
                  output_format = "html_document",
                  output_file = "outputs/pfd_summary_narrative_2023_24_v001.html")

# save copy as word document for use in quality review
rmarkdown::render("pfd_narrative.Rmd",
                  output_format = "word_document",
                  output_file = "outputs/pfd_summary_narrative_2023_24_v001.docx")

# save background document as html file into outputs folder
# change file path to save somewhere else if needed
rmarkdown::render("pfd_background_aug_2023.Rmd",
                  output_format = "html_document",
                  output_file = "outputs/pfd_background_info_methodology_v001.html")

rmarkdown::render("pfd_background_aug_2023.Rmd",
                  output_format = "word_document",
                  output_file = "outputs/pfd_background_info_methodology_v001.docx")

# save user engagement document as html file into outputs folder
# change file path to save somewhere else if needed
# rmarkdown::render("pfd_user_engagement_2223.Rmd",
#                   output_format = "html_document",
#                   output_file = "outputs/pfd_user_engagement_2223.html")

# 8. disconnect from DWH  ------------------------------------------------------

DBI::dbDisconnect(con)
# log_print("Disconnected from DWH", hide_notes = TRUE)

# close log
# logr::log_close()
