# pipeline.R --------------------------------------------------------------
# This script is used to run the RAP for the pfd annual publication
load('PfDmyEnvironment.RData')
# 1. install required packages --------------------------------------------
# TODO: investigate using renv package for dependency management
req_pkgs <- c("dplyr", "stringr", "data.table", "yaml", "openxlsx","rmarkdown",
              "logr", "highcharter", "lubridate", "dbplyr","tidyr","janitor")


#uncomment if package installs are needed
utils::install.packages(req_pkgs, dependencies = TRUE)
#
devtools::install_github(
  "nhsbsa-data-analytics/mumhR",
  auth_token = Sys.getenv("GITHUB_PAT")
)

devtools::install_github("nhsbsa-data-analytics/nhsbsaR")
#add PdFR here when fucntions complete
invisible(lapply(c(req_pkgs,  "nhsbsaR"), library, character.only = TRUE))

# 2. setup logging --------------------------------------------------------

#lf <- logr::log_open(autolog = TRUE)

# send code to log
#logr::log_code()

# 3. set options ----------------------------------------------------------

pdfR::pdf_options()
hcoptslang <- getOption("highcharter.lang")
hcoptslang$thousandsSep <- ","
# 4. extract data from NHSBSA DWH -----------------------------------------
# build connection to database
con <- con_nhsbsa(
  dsn = "FBS_8192k",
  driver = "Oracle in OraClient19Home1",
  "DWCP",
  username = rstudioapi::showPrompt(title = "Username", message = "Username"),
  password = rstudioapi::askForPassword()
)

pfd_data<-create_fact(con)
costpericb_data <- costpericb_extract (con = con, table = "PFD_FACT")
costpericb_patdata <- costpericb_patient_extract (con = con, table = "PFD_FACT")
costper_patdata <- costper_patient_extract (con = con, table = "PFD_FACT")
pfd_national_data <- national_extract(con = con,table = "PFD_FACT")
pfd_paragraph_data <- paragraph_extract(con = con,table = "PFD_FACT")
pfd_u18_data <- child_adult_extract(con, table = "PFD_FACT")
pfd_imd_data <- imd_extract(con = con,table = "PFD_FACT")
pfd_imd_paragraph_data <- imd_paragraph_extract(con = con,table = "PFD_FACT")
pfd_ageband_data <- ageband_extract(con = con,table = "PFD_FACT")
pfd_ageband_paragraph_data <- ageband_paragraph_extract(con = con,table = "PFD_FACT")
pfd_gender_data <- gender_extract(con = con,table = "PFD_FACT")
pfd_gender_paragraph_data <- gender_paragraph_extract(con = con,table = "PFD_FACT")
pfd_age_gender_data <- age_gender_extract(con = con,table = "PFD_FACT")
pfd_age_gender_paragraph_data <- age_gender_paragraph_extract (con,table = "PFD_FACT")
patient_identification_dt <- capture_rate_extract_dt(con = con,table = "PFD_FACT")
patient_identification <- capture_rate_extract(con = con,table = "PFD_FACT")
pfd_national_overall <- tbl(con, dbplyr::in_schema("KIGRA", "PFD_FACT_OVERALL")) %>%
  collect()

cost_per_patienticb1 <- costpericb_data%>%
  dplyr::filter(`Identified Patient Flag`=="Y") %>%
  dplyr::mutate(`Total NIC per patient (GBP)`=`Total Net Ingredient Cost (GBP)`/`Total Identified Patients`)  %>%
  dplyr::select(`Financial Year`,
                `Integrated Care Board Name`,
                `Integrated Care Board Code`,
                `Total NIC per patient (GBP)`)`
cost_per_patienticb <- costpericb_patient_extract(con = con,table = "PFD_FACT")

# 5. write data to .xlsx -
max_fy <- max(pfd_age_gender_data$`Financial Year`)

excel_column_to_numeric <- function(column_letter){
  # Uppercase
  s_upper <- toupper(column_letter)
  # Convert string to a vector of single letters
  s_split <- unlist(strsplit(s_upper, split=""))
  # Convert each letter to the corresponding number
  s_number <- sapply(s_split, function(x) {which(LETTERS == x)})
  # Derive the numeric value associated with each letter
  numbers <- 26^((length(s_number)-1):0)
  # Calculate the column number
  column_number <- sum(s_number * numbers)
  column_number
}
#vectorise to allow multiple columns
excel_column_to_numeric <- Vectorize(excel_column_to_numeric)

# create cost and items workbook
# create wb object
# create list of sheetnames needed (overview and metadata created automatically)


workbook_data <- list()

workbook_data$patient_id <- patient_identification %>%
  dplyr::select(`Financial Year`,
                `BNF Paragraph Name`,
                `BNF Paragraph Code`,
                `Identified Patient Rate` = RATE)

workbook_data$national_total <- pfd_national_overall %>%
  dplyr::select(`Financial Year`= FINANCIAL_YEAR,
                `Drug Type`=DRUG_TYPE,
                `Identified Patient Flag` = IDENTIFIED_FLAG,
                `Total Identified Patients`= PATIENT_COUNT,
                `Total Items`=ITEM_COUNT,
                `Total Net Ingredient Cost (GBP)`=ITEM_PAY_DR_NIC)

workbook_data$national_paragraph <- pfd_paragraph_data %>%
  dplyr::select(`Financial Year`,
                `Identified Patient Flag`,
                `BNF Paragraph Name`,
                `BNF Paragraph Code`,
                `Total Identified Patients`,
                `Total Items`,
                `Total Net Ingredient Cost (GBP)`)

workbook_data$cost_per_icb <- costpericb_data %>%
  dplyr::filter(`Integrated Care Board Name`!="UNKNOWN ICB"
  ) %>%
  dplyr::select(`Financial Year`,
                `Identified Patient Flag`,
                `Integrated Care Board Name`,
                `Integrated Care Board Code`,
                `Total Identified Patients`,
                `Total Items`,
                `Total Net Ingredient Cost (GBP)`)

workbook_data$cost_per_patienticb <- costpericb_data %>%
  dplyr::filter(`Identified Patient Flag`=="Y",`Integrated Care Board Name`!="UNKNOWN ICB"
  ) %>%
  dplyr::mutate(`Total NIC per patient (GBP)`=`Total Net Ingredient Cost (GBP)`/`Total Identified Patients`) %>%
  dplyr::select(`Financial Year`,
                `Identified Patient Flag`,
                `Integrated Care Board Name`,
                `Integrated Care Board Code`,
                `Total NIC per patient (GBP)` )

#need to put sheetnames list and workbook creation above metadata code
sheetNames <- c("Patient_Identification",
                "National_Total",
                "National_Paragraph",
                "Cost_per_ICB",
                "Cost_per_Patient")

wb <- create_wb(sheetNames)

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

create_metadata(wb,
                meta_fields,
                meta_descs
)

#### Patient identification
# write data to sheet
write_sheet(
  wb,
  "Patient_Identification",
  "Prescribing for Diabetes - 2015/16 to 2021/22 - Proportion of items for which an NHS number was recorded (%)",
  c(
    "The below proportions reflect the percentage of prescription items where a PDS verified NHS number was recorded."
  ),
  workbook_data$patient_id,
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
  "Table 1: Prescribing for Diabetes - England 2015/16 to 2021/22 Total dispensed items and costs per financial year",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients.",
    "3. Total costs and items may not be reconciled back to Prescribing Cost Analysis (PCA) publication figures as these figures are based around a âprescribing viewâ of the data. This is where we use the drug or device that was prescribed to a patient, rather than the drug that was reimbursed to the dispenser to classify a prescription item. PCA uses a dispensing view where the inverse is true."
  ),
  workbook_data$national_total,
  14
)

#left align columns A to C
format_data(wb,
            "National_Total",
            c("A", "B","C"),
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
  "Table 2: Prescribing for Diabetes - England 2015/16 to 2021/22 Yearly totals split by BNF paragraph and identified patients",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients."

  ),
  workbook_data$national_paragraph,
  14
)
#left align columns A to D
format_data(wb,
            "National_Paragraph",
            c("A", "B","C","D"),
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
  "Table 3: Prescribing for Diabetes - England 2015/16 to 2021/22 Costs and items per ICB per financial year",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients."

  ), workbook_data$cost_per_icb, 14
)

#left align columns A to D
format_data(wb,
            "Cost_per_ICB",
            c("A", "B","C","D"),
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
  "Table 4: Prescribing for Diabetes - England 2015/16 to 2021/22 Average cost per patient per ICB per financial year",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients."

  ),
  workbook_data$cost_per_patienticb,
  14
)

#left align columns A to D
format_data(wb,
            "Cost_per_Patient",
            c("A", "B","C","D"),
            "left",
            "")

#right align column E and round to 2dp with thousand separator
format_data(wb,
            "Cost_per_Patient",
            c("E"),
            "right",
            "#,##0.00")

#save file into outputs folder
openxlsx::saveWorkbook(wb,
                       "outputs/PfD_2021_2022_costs_and_items_v001.xlsx",
                       overwrite = TRUE)
rm(wb)
# create patient demographic workbook
# create wb object
# create list of sheetnames needed (overview and metadata created automatically)

workbook_data <- list()

workbook_data$patient_id <- patient_identification %>%
  dplyr::select(`Financial Year`,
                `BNF Paragraph Name`,
                `BNF Paragraph Code`,
                `Identified Patient Rate` = RATE)

workbook_data$national_total <- pfd_national_overall %>%
  dplyr::select(`Financial Year`= FINANCIAL_YEAR,
                `Drug Type`=DRUG_TYPE,
                `Identified Patient Flag` = IDENTIFIED_FLAG,
                `Total Identified Patients`= PATIENT_COUNT,
                `Total Items`=ITEM_COUNT,
                `Total Net Ingredient Cost (GBP)`=ITEM_PAY_DR_NIC)
workbook_data$national_sex <- pfd_gender_data %>%
  dplyr::select(`Financial Year`,
                `Patient Sex`,
                `Identified Patient Flag`,
                `Total Identified Patients`,
                `Total Items`,
                `Total Net Ingredient Cost (GBP)`)
workbook_data$paragraph_sex <- pfd_gender_paragraph_data %>%
  apply_sdc(level = 5, rounding = F, mask = NA_real_) %>%
  dplyr::select(`Financial Year`,
                `BNF Paragraph Name`=`Paragraph Name`,
                `BNF Paragraph Code` = `Paragraph Code` ,
                `Patient Sex`,
                `Identified Patient Flag`,
                `Total Identified Patients`=`sdc_Total Identified Patients`,
                `Total Items`=`sdc_Total Items`,
                `Total Net Ingredient Cost (GBP)`)
workbook_data$national_age <- pfd_ageband_data %>%
  dplyr::select(`Financial Year`,
                `Age Band`,
                `Identified Patient Flag`,
                `Total Identified Patients`,
                `Total Items`,
                `Total Net Ingredient Cost (GBP)`)

workbook_data$paragraph_age <- pfd_ageband_paragraph_data %>%

  dplyr::select(`Financial Year`,
                `BNF Paragraph Name`,
                `BNF Paragraph Code`,
                `Age Band`,
                `Identified Patient Flag`,
                `Total Identified Patients`,
                `Total Items`,
                `Total Net Ingredient Cost (GBP)`)
workbook_data$national_age_sex <- pfd_age_gender_data %>%
  dplyr::select(`Financial Year`,
                `Age Band`,
                `Patient Sex`,
                `Identified Patient Flag`,
                `Total Identified Patients`,
                `Total Items`,
                `Total Net Ingredient Cost (GBP)`)
workbook_data$paragraph_age_sex <- pfd_age_gender_paragraph_data %>%
  apply_sdc(level = 5, rounding = F, mask = NA_real_) %>%
  dplyr::select(`Financial Year`,
                `BNF Paragraph Name`=`Paragraph Name`,
                `BNF Paragraph Code` = `Paragraph Code`,
                `Age Band`,
                `Patient Sex`,
                `Identified Patient Flag`,
                `Total Identified Patients`=`sdc_Total Identified Patients`,
                `Total Items`=`sdc_Total Items`,
                `Total Net Ingredient Cost (GBP)`)
workbook_data$national_u18 <- pfd_u18_data %>%
  dplyr::select(`Financial Year`,
                `Age Band`,
                `Identified Patient Flag`,
                `Total Identified Patients`,
                `Total Items`,
                `Total Net Ingredient Cost (GBP)`)
workbook_data$national_imd <- pfd_imd_data %>%
  dplyr::select(`Financial Year`,
                `IMD Decile`,
                `Total Identified Patients`,
                `Total Items`,
                `Total Net Ingredient Cost (GBP)`)
workbook_data$paragraph_imd <- pfd_imd_paragraph_data %>%
  dplyr::select(`Financial Year`,
                `BNF Paragraph Name`,
                `BNF Paragraph Code`,
                `IMD Decile`,
                `Total Identified Patients`,
                `Total Items`,
                `Total Net Ingredient Cost (GBP)`)



#need to put sheetnames list and workbook creation above metadata code
sheetNames <- c("Patient_Identification",
                "National_Total",
                "Sex",
                "Dispensing_By_Sex",
                "Age_Band",
                "Dispensing_By_Age",
                "Age_Band_and_Sex",
                "Dispensing_By_Age_and_Sex",
                "Adults_and_Children",
                "Indices_of_Deprivation",
                "IMD_By_Drug")


wb <- create_wb(sheetNames)

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
  "Drug Type",
  "Patient Sex",
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
    "The sex of the patient as noted at the time the prescription was processed. This includes where the patient has been identified but the sex has not been recorded.",
    "The age band of the patient as of the 30th September of the corresponding financial year the drug was prescribed.",
    "The IMD decile of the patient, based on the location of their practice, where '1' is the 10% of areas with the highest deprivation score in the Index of Multiple Deprivation (IMD) from the English Indices of Deprivation 2019, and '10' is the 10% of areas with the lowest IMD deprivation score. Unknown values are where the items are attributed to an unidentified practice within a Primary Care Organisation (PCO), or where we have been unable to match the practice postcode to a postcode in the National Statistics Postcode Lookup (NSPL)."

  )

create_metadata(wb,
                meta_fields,
                meta_descs
)

#### Patient identification
# write data to sheet
write_sheet(
  wb,
  "Patient_Identification",
  "Prescribing for Diabetes - 2015/16 to 2021/22 - Proportion of items for which an NHS number was recorded (%)",
  c(
    "The below proportions reflect the percentage of prescription items where a PDS verified NHS number was recorded."
  ),
  workbook_data$patient_id,
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
  "Table 1: Prescribing for Diabetes - England 2015/16 to 2021/22 Total dispensed items and costs per financial year",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients.",
    "3. Total costs and items may not be reconciled back to Prescribing Cost Analysis (PCA) publication figures as these figures are based around a âprescribing viewâ of the data. This is where we use the drug or device that was prescribed to a patient, rather than the drug that was reimbursed to the dispenser to classify a prescription item. PCA uses a dispensing view where the inverse is true."
  ),
  workbook_data$national_total,
  14
)

#left align columns A to C
format_data(wb,
            "National_Total",
            c("A", "B","C"),
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

#### National Sex data
# write data to sheet
write_sheet(
  wb,
  "Sex",
  "Table 2: Prescribing for Diabetes - England 2015/16 to 2021/22  National prescribing by sex per financial year",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. It is possible for a patient to be marked as 'unknown' or 'indeterminate'. Due to the low number of patients that these two groups contain the NHSBSA has decided to group these classifications together.",
    "3. The NHSBSA does not use the latest national data standard relating to patient gender/sex, and use historic nomenclature in some cases. Please see the detailed Background Information and Methodology notice released with this publication for further information."

  ),
  workbook_data$national_sex,
  14
)
#left align columns A to D
format_data(wb,
            "Sex",
            c("A", "B","C"),
            "left",
            "")

#right align columns D and E and round to whole numbers with thousand separator
format_data(wb,
            "Sex",
            c("D", "E"),
            "right",
            "#,##0")

#right align column F and round to 2dp with thousand separator
format_data(wb,
            "Sex",
            c("G"),
            "right",
            "#,##0.00")

#### Paragraph Sex data
# write data to sheet
write_sheet(
  wb,
  "Dispensing_By_Sex",
  "Table 3: Prescribing for Diabetes - England 2015/16 to 2021/22  Yearly patients, items and costs by BNF paragraph split by patient sex",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. It is possible for a patient to be marked as 'unknown' or 'indeterminate'. Due to the low number of patients that these two groups contain the NHSBSA has decided to group these classifications together.",
    "3. The NHSBSA does not use the latest national data standard relating to patient gender/sex, and use historic nomenclature in some cases. Please see the detailed Background Information and Methodology notice released with this publication for further information.",
    "4. Statistical disclosure control is applied where prescribing relates to fewer than 5 patients. In these instances, figures are replaced with an asterisk (*)"

  ),
  workbook_data$paragraph_sex,
  14
)
#left align columns A to E
format_data(wb,
            "Dispensing_By_Sex",
            c("A", "B", "C", "D", "E"),
            "left",
            "")

#right align columns F and G and round to whole numbers with thousand separator
format_data(wb,
            "Dispensing_By_Sex",
            c("F", "G"),
            "right",
            "#,##0")

#right align column H and round to 2dp with thousand separator
format_data(wb,
            "Dispensing_By_Sex",
            c("H"),
            "right",
            "#,##0.00")

#### National age data
# write data to sheet
write_sheet(
  wb,
  "Age_Band",
  "Table 4: Prescribing for Diabetes - England 2015/16 to 2021/22 National prescribing by age per financial year",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. A patient's age is calculated at 30 September of the given year in order to be assigned to an age band. Some patients do not hold a date of birth and are assigned an 'unknown' age band."

  ),
  workbook_data$national_age,
  14
)
#left align columns A to D
format_data(wb,
            "Age_Band",
            c("A", "B","C"),
            "left",
            "")

#right align columns D and E and round to whole numbers with thousand separator
format_data(wb,
            "Age_Band",
            c("D", "E"),
            "right",
            "#,##0")

#right align column F and round to 2dp with thousand separator
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
  "Table 5: Prescribing for Diabetes - England 2015/16 to 2021/22  Yearly patients, items and costs by BNF paragraph split by patient sex",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients."

  ),
  workbook_data$paragraph_age,
  14
)
#left align columns A to E
format_data(wb,
            "Dispensing_By_Age",
            c("A", "B", "C", "D", "E"),
            "left",
            "")

#right align columns F and G and round to whole numbers with thousand separator
format_data(wb,
            "Dispensing_By_Age",
            c("F", "G"),
            "right",
            "#,##0")

#right align column H and round to 2dp with thousand separator
format_data(wb,
            "Dispensing_By_Age",
            c("H"),
            "right",
            "#,##0.00")
#### National age sex data
# write data to sheet
write_sheet(
  wb,
  "Age_Band_and_Sex",
  "Table 6: Prescribing for Diabetes - England 2015/16 to 2021/22 National prescribing by age and sex per financial year",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. These totals only include patients where both age and sex are known."

  ),
  workbook_data$national_age_sex,
  14
)
#left align columns A to D
format_data(wb,
            "Age_Band_and_Sex",
            c("A", "B", "C", "D"),
            "left",
            "")

#right align columns E and F and round to whole numbers with thousand separator
format_data(wb,
            "Age_Band_and_Sex",
            c("E", "F"),
            "right",
            "#,##0")

#right align column G and round to 2dp with thousand separator
format_data(wb,
            "Age_Band_and_Sex",
            c("G"),
            "right",
            "#,##0.00")

#### Paragraph Age data
# write data to sheet
write_sheet(
  wb,
  "Dispensing_By_Age_and_Sex",
  "Table 7: Prescribing for Diabetes - England 2015/16 to 2021/22  Yearly patients, items and costs by BNF paragraph split by patient age and sex",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients."
  ),
  workbook_data$paragraph_age_sex,
  14
)
#left align columns A to F
format_data(wb,
            "Dispensing_By_Age_and_Sex",
            c("A", "B", "C", "D", "E","F"),
            "left",
            "")

#right align columns G and H and round to whole numbers with thousand separator
format_data(wb,
            "Dispensing_By_Age_and_Sex",
            c("G","H"),
            "right",
            "#,##0")

#right align column I and round to 2dp with thousand separator
format_data(wb,
            "Dispensing_By_Age_and_Sex",
            c("I"),
            "right",
            "#,##0.00")
#### National u18 data
# write data to sheet
write_sheet(
  wb,
  "Adults_and_Children",
  "Table 8: Prescribing for Diabetes - England 2015/16 to 2021/22 Prescribing by adult/child split",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. A patient's age is calculated at 30 September of the given year in order to be assigned to an age band. Some patients do not hold a date of birth and are assigned an 'unknown' age band."

  ),
  workbook_data$national_age,
  14
)
#left align columns A to D
format_data(wb,
            "Adults_and_Children",
            c("A", "B","C"),
            "left",
            "")

#right align columns D and E and round to whole numbers with thousand separator
format_data(wb,
            "Adults_and_Children",
            c("D", "E"),
            "right",
            "#,##0")

#right align column F and round to 2dp with thousand separator
format_data(wb,
            "Adults_and_Children",
            c("G"),
            "right",
            "#,##0.00")

#### National imd data
# write data to sheet
write_sheet(
  wb,
  "Indices_of_Deprivation",
  "Table 9: Prescribing for Diabetes - England 2015/16 to 2021/22 Yearly patients, items and costs by IMD Decile",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. IMD deciles used are taken from the English Indices of Deprivaton 2019 National Statistics publication.",
    "3. Where a prescribing organisation's postcode has not been able to to be matched to National Statistics Postcode Lookup (August 2020) or the prescriber has not been identified the records are reported as 'unknown' IMD decile.",
    "4. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an
      overestimate of the number of patients."

  ),
  workbook_data$national_imd,
  14
)
#left align columns A to B
format_data(wb,
            "Indices_of_Deprivation",
            c("A", "B"),
            "left",
            "")

#right align columns C and D and round to whole numbers with thousand separator
format_data(wb,
            "Indices_of_Deprivation",
            c("C", "D"),
            "right",
            "#,##0")

#right align column E and round to 2dp with thousand separator
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
  "Table 10: Prescribing for Diabetes - England 2015/16 to 2021/22  Yearly patients, items and costs by BNF paragraph split by patient sex",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. IMD deciles used are taken from the English Indices of Deprivaton 2019 National Statistics publication.",
    "3. Where a prescribing organisation's postcode has not been able to to be matched to National Statistics Postcode Lookup (August 2020) or the prescriber has not been identified the records are reported as 'unknown' IMD decile.",
    "4. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an
      overestimate of the number of patients."

  ),
  workbook_data$paragraph_imd,
  14
)
#left align columns A to D
format_data(wb,
            "IMD_By_Drug",
            c("A", "B", "C", "D"),
            "left",
            "")

#right align columns E and F and round to whole numbers with thousand separator
format_data(wb,
            "IMD_By_Drug",
            c("E", "F"),
            "right",
            "#,##0")

#right align column G and round to 2dp with thousand separator
format_data(wb,
            "IMD_By_Drug",
            c("G"),
            "right",
            "#,##0.00")

#save file into outputs folder
openxlsx::saveWorkbook(wb,
                       "outputs/PfD_2021_2022__patient_demographics_v001.xlsx",
                       overwrite = TRUE)

# 6. Automate QR table - tba
warnings()
rm(wb)
# 7. Automate Narrative - tba

# 8. render markdown ------------------------------------------------------

rmarkdown::render("pfd_annual_narrative.Rmd",
                  output_format = "html_document",
                  output_file = "outputs/pfd_annual_2021_22_v001.html")

rmarkdown::render("pfd_annual_narrative.Rmd",
                  output_format = "word_document",
                  output_file = "outputs/pfd_annual_2021_22_v001.docx")
