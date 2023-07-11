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

#check if Excel outputs are required
makeSheet <- menu(c("Yes", "No"),
                  title = "Do you wish to generate the Excel outputs?")

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
costpericb_data <- costpericb_extract (con = con)

costpericb_patdata <- costpericb_patient_extract (con = con, table = "PFD_FACT")

costper_patdata <- costper_patient_extract (con = con, table = "PFD_FACT")

pfd_national_data <- national_extract(con = con, schema = "KIGRA", table = "PFD_FACT_202307")

pfd_paragraph_data <- paragraph_extract(con = con, schema = "KIGRA", table = "PFD_FACT_202307")

pfd_u18_data <- child_adult_extract(con = con, schema = "KIGRA", table = "PFD_FACT_202307")

pfd_imd_data <- imd_extract(con = con, schema = "KIGRA", table = "PFD_FACT_202307")

pfd_imd_paragraph_data <- imd_paragraph_extract(con = con, schema = "KIGRA", table = "PFD_FACT_202307")

pfd_ageband_data <- ageband_extract(con = con, schema = "KIGRA", table = "PFD_FACT_202307")

pfd_ageband_paragraph_data <- ageband_paragraph_extract(con = con, schema = "KIGRA", table = "PFD_FACT_202307")

pfd_gender_data <- gender_extract(con = con, schema = "KIGRA", table = "PFD_FACT_202307")

pfd_gender_paragraph_data <- gender_paragraph_extract(con = con, schema = "KIGRA", table = "PFD_FACT_202307")

pfd_age_gender_data <- age_gender_extract(con = con, schema = "KIGRA", table = "PFD_FACT_202307")

pfd_age_gender_paragraph_data <- age_gender_paragraph_extract (con,table = "PFD_FACT")

patient_identification_dt <- capture_rate_extract_dt(con = con,table = "PFD_FACT")

patient_identification <- capture_rate_extract(con = con,table = "PFD_FACT")

pfd_national_overall <- tbl(con, dbplyr::in_schema("KIGRA", "PFD_FACT_OVERALL")) |>
  collect()

cost_per_patienticb1 <- costpericb_data|>
  dplyr::filter(`Identified Patient Flag`=="Y") |>
  dplyr::mutate(`Total NIC per patient (GBP)`=`Total Net Ingredient Cost (GBP)`/`Total Identified Patients`)  |>
  dplyr::select(`Financial Year`,
                `Integrated Care Board Name`,
                `Integrated Care Board Code`,
                `Total NIC per patient (GBP)`)`

cost_per_patienticb <- costpericb_patient_extract(con = con,table = "PFD_FACT")

