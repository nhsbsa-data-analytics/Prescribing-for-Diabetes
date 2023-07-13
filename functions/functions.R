### data warehouse extracts from fact table

national_extract <- function(con,
                             schema,
                             table) {
  fact <- dplyr::tbl(con,
                     from = dbplyr::in_schema(schema, table)) |>
    dplyr::mutate(PATIENT_COUNT = case_when(PATIENT_IDENTIFIED == "Y" ~ 1,
                                            TRUE ~ 0)) |>
    dplyr::group_by(FINANCIAL_YEAR,
                    IDENTIFIED_PATIENT_ID,
                    PATIENT_IDENTIFIED,
                    PATIENT_COUNT) |>
    dplyr::summarise(
      ITEM_COUNT = sum(ITEM_COUNT, na.rm = T),
      ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC, na.rm = T),
      .groups = "drop"
    )
  
  fact_national <- fact |>
    dplyr::group_by(`Financial Year` = FINANCIAL_YEAR,
                    `Identified Patient Flag` = PATIENT_IDENTIFIED) |>
    dplyr::summarise(
      `Total Identified Patients` = sum(PATIENT_COUNT, na.rm = T),
      `Total Items` = sum(ITEM_COUNT, na.rm = T),
      `Total Net Ingredient Cost (GBP)` = sum(ITEM_PAY_DR_NIC, na.rm = T) /
        100,
      .groups = "drop"
    ) |>
    dplyr::arrange(`Financial Year`,
                   desc(`Identified Patient Flag`)) |>
    collect()
  
  return(fact_national)
  
}

paragraph_extract <- function(con,
                              schema,
                              table) {
  fact <- dplyr::tbl(con,
                     from = dbplyr::in_schema(schema, table)) |>
    dplyr::mutate(PATIENT_COUNT = case_when(PATIENT_IDENTIFIED == "Y" ~ 1,
                                            TRUE ~ 0)) |>
    dplyr::group_by(
      FINANCIAL_YEAR,
      IDENTIFIED_PATIENT_ID,
      PATIENT_IDENTIFIED,
      PARAGRAPH_DESCR,
      BNF_PARAGRAPH,
      PATIENT_COUNT
    ) |>
    dplyr::summarise(
      ITEM_COUNT = sum(ITEM_COUNT, na.rm = T),
      ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC, na.rm = T),
      .groups = "drop"
    )
  fact_paragraph <- fact |>
    dplyr::group_by(
      `Financial Year` = FINANCIAL_YEAR,
      `BNF Paragraph Name` = PARAGRAPH_DESCR,
      `BNF Paragraph Code` = BNF_PARAGRAPH,
      `Identified Patient Flag` = PATIENT_IDENTIFIED
    ) |>
    dplyr::summarise(
      `Total Identified Patients` = sum(PATIENT_COUNT, na.rm = T),
      `Total Items` = sum(ITEM_COUNT, na.rm = T),
      `Total Net Ingredient Cost (GBP)` = sum(ITEM_PAY_DR_NIC, na.rm = T) /
        100,
      .groups = "drop"
    ) |>
    dplyr::arrange(`Financial Year`,
                   `BNF Paragraph Code`,
                   desc(`Identified Patient Flag`)) |>
    collect()
  return(fact_paragraph)
}


child_adult_extract <- function(con,
                                schema,
                                table) {
  fact <- dplyr::tbl(con,
                     from = dbplyr::in_schema(schema, table)) |>
    dplyr::mutate(PATIENT_COUNT = case_when(PATIENT_IDENTIFIED == "Y" ~ 1,
                                            TRUE ~ 0)) |>
    dplyr::group_by(
      FINANCIAL_YEAR,
      IDENTIFIED_PATIENT_ID,
      PATIENT_IDENTIFIED,
      CALC_AGE,
      PATIENT_COUNT
    ) |>
    dplyr::summarise(
      ITEM_COUNT = sum(ITEM_COUNT, na.rm = T),
      ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC, na.rm = T),
      .groups = "drop"
    )
  
  child_adult_extract <- fact |>
    mutate(AGE_BAND = case_when(
      CALC_AGE < 0 ~ "Unknown",
      CALC_AGE <= 17 ~ "17 and under",
      TRUE ~ "18 and over"
    )) |>
    dplyr::group_by(
      `Financial Year` = FINANCIAL_YEAR,
      `Age Band` = AGE_BAND,
      `Identified Patient Flag` = PATIENT_IDENTIFIED
    ) |>
    dplyr::summarise(
      `Total Identified Patients` = sum(PATIENT_COUNT, na.rm = T),
      `Total Items` = sum(ITEM_COUNT, na.rm = T),
      `Total Net Ingredient Cost (GBP)` = sum(ITEM_PAY_DR_NIC, na.rm = T) /
        100,
      .groups = "drop"
    ) |>
    dplyr::arrange(`Financial Year`,
                   `Age Band`,
                   desc(`Identified Patient Flag`)) |>
    collect()
  
  return(child_adult_extract)
  
}

imd_extract <- function(con,
                        schema,
                        table) {
  fact <- dplyr::tbl(con,
                     from = dbplyr::in_schema(schema, table)) |>
    dplyr::mutate(PATIENT_COUNT = case_when(PATIENT_IDENTIFIED == "Y" ~ 1,
                                            TRUE ~ 0)) |>
    dplyr::group_by(
      FINANCIAL_YEAR,
      IDENTIFIED_PATIENT_ID,
      PATIENT_IDENTIFIED,
      IMD_DECILE,
      PATIENT_COUNT
    ) |>
    dplyr::summarise(
      ITEM_COUNT = sum(ITEM_COUNT, na.rm = T),
      ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC, na.rm = T),
      .groups = "drop"
    ) |>
    dplyr::mutate(
      `IMD Quintile` = case_when(
        IMD_DECILE %in% c("1", "2") ~ "1 - Most deprived",
        IMD_DECILE %in% c("3", "4") ~ "2",
        IMD_DECILE %in% c("5", "6") ~ "3",
        IMD_DECILE %in% c("7", "8") ~ "4",
        IMD_DECILE %in% c("9", "10") ~ "5 - Least deprived",
        is.na(IMD_DECILE) ~ "Unknown"
      )
    ) |>
    dplyr::group_by(`Financial Year` = FINANCIAL_YEAR,
                    `IMD Quintile`) |>
    dplyr::summarise(
      `Total Identified Patients` = sum(PATIENT_COUNT, na.rm = T),
      `Total Items` = sum(ITEM_COUNT, na.rm = T),
      `Total Net Ingredient Cost (GBP)` = sum(ITEM_PAY_DR_NIC, na.rm = T) /
        100,
      .groups = "drop"
    ) |>
    dplyr::arrange(`Financial Year`,
                   `IMD Quintile`) |>
    collect()
  
  return(fact)
}

imd_paragraph_extract <- function(con,
                                  schema,
                                  table) {
  fact <- dplyr::tbl(con,
                     from = dbplyr::in_schema(schema, table)) |>
    dplyr::mutate(PATIENT_COUNT = case_when(PATIENT_IDENTIFIED == "Y" ~ 1,
                                            TRUE ~ 0)) |>
    dplyr::group_by(
      FINANCIAL_YEAR,
      IDENTIFIED_PATIENT_ID,
      PATIENT_IDENTIFIED,
      PARAGRAPH_DESCR,
      BNF_PARAGRAPH,
      IMD_DECILE,
      PATIENT_COUNT
    ) |>
    dplyr::summarise(
      ITEM_COUNT = sum(ITEM_COUNT, na.rm = T),
      ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC, na.rm = T),
      .groups = "drop"
    ) |>
    dplyr::mutate(
      `IMD Quintile` = case_when(
        IMD_DECILE %in% c("1", "2") ~ "1 - Most deprived",
        IMD_DECILE %in% c("3", "4") ~ "2",
        IMD_DECILE %in% c("5", "6") ~ "3",
        IMD_DECILE %in% c("7", "8") ~ "4",
        IMD_DECILE %in% c("9", "10") ~ "5 - Least deprived",
        is.na(IMD_DECILE) ~ "Unknown"
      )
    ) |>
    dplyr::group_by(
      `Financial Year` = FINANCIAL_YEAR,
      `IMD Quintile`,
      `BNF Paragraph Name` = PARAGRAPH_DESCR,
      `BNF Paragraph Code` = BNF_PARAGRAPH
    ) |>
    dplyr::summarise(
      `Total Identified Patients` = sum(PATIENT_COUNT, na.rm = T),
      `Total Items` = sum(ITEM_COUNT, na.rm = T),
      `Total Net Ingredient Cost (GBP)` = sum(ITEM_PAY_DR_NIC, na.rm = T) /
        100,
      .groups = "drop"
    ) |>
    arrange(`Financial Year`,
            `BNF Paragraph Code`,
            `IMD Quintile`) |>
    collect()
  
  
  return(fact)
}

ageband_extract <- function(con,
                            schema,
                            table) {
  fact <- dplyr::tbl(con,
                     from = dbplyr::in_schema(schema, table)) |>
    dplyr::mutate(PATIENT_COUNT = case_when(PATIENT_IDENTIFIED == "Y" ~ 1,
                                            TRUE ~ 0)) |>
    dplyr::group_by(
      FINANCIAL_YEAR,
      IDENTIFIED_PATIENT_ID,
      PATIENT_IDENTIFIED,
      CALC_AGE,
      PATIENT_COUNT
    ) |>
    dplyr::summarise(
      ITEM_COUNT = sum(ITEM_COUNT, na.rm = T),
      ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC, na.rm = T),
      .groups = "drop"
    )
  
  fact_age <- fact |>
    dplyr::inner_join(dplyr::tbl(con,
                                 from = dbplyr::in_schema("DIM", "AGE_DIM")),
                      by = c("CALC_AGE" = "AGE")) |>
    dplyr::mutate(AGE_BAND = dplyr::case_when(is.na(DALL_5YR_BAND) ~ "Unknown",
                                              TRUE ~ DALL_5YR_BAND)) |>
    dplyr::group_by(
      `Financial Year` = FINANCIAL_YEAR,
      `Age Band` = AGE_BAND,
      `Identified Patient Flag` = PATIENT_IDENTIFIED
    ) |>
    dplyr::summarise(
      `Total Identified Patients` = sum(PATIENT_COUNT, na.rm = T),
      `Total Items` = sum(ITEM_COUNT, na.rm = T),
      `Total Net Ingredient Cost (GBP)` = sum(ITEM_PAY_DR_NIC, na.rm = T) /
        100,
      .groups = "drop"
    ) |>
    dplyr::arrange(`Financial Year`,
                   `Age Band`,
                   desc(`Identified Patient Flag`)) |>
    collect()
  
  return(fact_age)
  
}
ageband_paragraph_extract <- function(con,
                                      schema,
                                      table) {
  fact <- dplyr::tbl(con,
                     from = dbplyr::in_schema(schema, table)) |>
    dplyr::mutate(PATIENT_COUNT = case_when(PATIENT_IDENTIFIED == "Y" ~ 1,
                                            TRUE ~ 0)) |>
    dplyr::group_by(
      FINANCIAL_YEAR,
      IDENTIFIED_PATIENT_ID,
      PATIENT_IDENTIFIED,
      PARAGRAPH_DESCR,
      BNF_PARAGRAPH,
      CALC_AGE,
      PATIENT_COUNT
    ) |>
    dplyr::summarise(
      ITEM_COUNT = sum(ITEM_COUNT, na.rm = T),
      ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC, na.rm = T),
      .groups = "drop"
    )
  
  fact_age <- fact |>
    dplyr::inner_join(dplyr::tbl(con,
                                 from = dbplyr::in_schema("DIM", "AGE_DIM")),
                      by = c("CALC_AGE" = "AGE")) |>
    dplyr::mutate(AGE_BAND = dplyr::case_when(is.na(DALL_5YR_BAND) ~ "Unknown",
                                              TRUE ~ DALL_5YR_BAND)) |>
    dplyr::group_by(
      `Financial Year` = FINANCIAL_YEAR,
      `Age Band` = AGE_BAND,
      `BNF Paragraph Name` = PARAGRAPH_DESCR,
      `BNF Paragraph Code` = BNF_PARAGRAPH,
      `Identified Patient Flag` = PATIENT_IDENTIFIED
    ) |>
    dplyr::summarise(
      `Total Identified Patients` = sum(PATIENT_COUNT, na.rm = T),
      `Total Items` = sum(ITEM_COUNT, na.rm = T),
      `Total Net Ingredient Cost (GBP)` = sum(ITEM_PAY_DR_NIC, na.rm = T) /
        100,
      .groups = "drop"
    ) |>
    dplyr::arrange(`Financial Year`,
                   `BNF Paragraph Code`,
                   `Age Band`,
                   desc(`Identified Patient Flag`)) |>
    collect()
  
  return(fact_age)
  
}

gender_extract <- function(con,
                           schema,
                           table) {
  fact <- dplyr::tbl(con,
                     from = dbplyr::in_schema(schema, table)) |>
    dplyr::mutate(PATIENT_COUNT = case_when(PATIENT_IDENTIFIED == "Y" ~ 1,
                                            TRUE ~ 0)) |>
    dplyr::group_by(
      FINANCIAL_YEAR,
      IDENTIFIED_PATIENT_ID,
      PATIENT_IDENTIFIED,
      GENDER_DESCR,
      PATIENT_COUNT
    ) |>
    dplyr::summarise(
      ITEM_COUNT = sum(ITEM_COUNT, na.rm = T),
      ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC, na.rm = T),
      .groups = "drop"
    )
  
  fact_gender <- fact |>
    dplyr::group_by(
      `Financial Year` = FINANCIAL_YEAR,
      `Patient Sex` = GENDER_DESCR,
      `Identified Patient Flag` = PATIENT_IDENTIFIED
    ) |>
    dplyr::summarise(
      `Total Identified Patients` = sum(PATIENT_COUNT, na.rm = T),
      `Total Items` = sum(ITEM_COUNT, na.rm = T),
      `Total Net Ingredient Cost (GBP)` = sum(ITEM_PAY_DR_NIC, na.rm = T) /
        100,
      .groups = "drop"
    ) |>
    dplyr::arrange(`Financial Year`,
                   `Patient Sex`,
                   desc(`Identified Patient Flag`)) |>
    collect()
  
  return(fact_gender)
  
}

gender_paragraph_extract <- function(con,
                                     schema,
                                     table) {
  fact <- dplyr::tbl(con,
                     from = dbplyr::in_schema(schema, table)) |>
    dplyr::mutate(PATIENT_COUNT = case_when(PATIENT_IDENTIFIED == "Y" ~ 1,
                                            TRUE ~ 0)) |>
    dplyr::group_by(
      FINANCIAL_YEAR,
      IDENTIFIED_PATIENT_ID,
      PATIENT_IDENTIFIED,
      PARAGRAPH_DESCR,
      BNF_PARAGRAPH,
      GENDER_DESCR,
      PATIENT_COUNT
    ) |>
    dplyr::summarise(
      ITEM_COUNT = sum(ITEM_COUNT, na.rm = T),
      ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC, na.rm = T),
      .groups = "drop"
    )
  
  fact_gender <- fact |>
    dplyr::group_by(
      `Financial Year` = FINANCIAL_YEAR,
      `Paragraph Name` = PARAGRAPH_DESCR,
      `Paragraph Code` = BNF_PARAGRAPH,
      `Patient Sex` = GENDER_DESCR,
      `Identified Patient Flag` = PATIENT_IDENTIFIED
    ) |>
    dplyr::summarise(
      `Total Identified Patients` = sum(PATIENT_COUNT, na.rm = T),
      `Total Items` = sum(ITEM_COUNT, na.rm = T),
      `Total Net Ingredient Cost (GBP)` = sum(ITEM_PAY_DR_NIC, na.rm = T) /
        100,
      .groups = "drop"
    ) |>
    dplyr::arrange(`Financial Year`,
                   `Paragraph Code`,
                   `Patient Sex`,
                   desc(`Identified Patient Flag`)) |>
    
    collect()
  
  return(fact_gender)
  
}


age_gender_extract <- function(con,
                               schema,
                               table) {
  fact <- dplyr::tbl(con,
                     from = dbplyr::in_schema(schema, table)) |>
    dplyr::mutate(PATIENT_COUNT = case_when(PATIENT_IDENTIFIED == "Y" ~ 1,
                                            TRUE ~ 0)) |>
    dplyr::group_by(
      FINANCIAL_YEAR,
      IDENTIFIED_PATIENT_ID,
      PATIENT_IDENTIFIED,
      CALC_AGE,
      GENDER_DESCR,
      PATIENT_COUNT
    ) |>
    dplyr::summarise(
      ITEM_COUNT = sum(ITEM_COUNT, na.rm = T),
      ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC, na.rm = T),
      .groups = "drop"
    )
  
  fact_age_gender <- fact |>
    filter(GENDER_DESCR != "Unknown") |>
    dplyr::inner_join(dplyr::tbl(con,
                                 from = dbplyr::in_schema("DIM", "AGE_DIM")),
                      by = c("CALC_AGE" = "AGE")) |>
    dplyr::mutate(AGE_BAND = dplyr::case_when(is.na(DALL_5YR_BAND) ~ "Unknown",
                                              TRUE ~ DALL_5YR_BAND)) |>
    dplyr::group_by(
      `Financial Year` = FINANCIAL_YEAR,
      `Age Band` = AGE_BAND,
      `Patient Sex` = GENDER_DESCR,
      `Identified Patient Flag` = PATIENT_IDENTIFIED
    ) |>
    dplyr::summarise(
      `Total Identified Patients` = sum(PATIENT_COUNT, na.rm = T),
      `Total Items` = sum(ITEM_COUNT, na.rm = T),
      `Total Net Ingredient Cost (GBP)` = sum(ITEM_PAY_DR_NIC, na.rm = T) /
        100,
      .groups = "drop"
    ) |>
    dplyr::arrange(`Financial Year`,
                   `Age Band`,
                   desc(`Identified Patient Flag`)) |>
    collect()
  
  return(fact_age_gender)
  
}

age_gender_paragraph_extract <- function(con,
                                         schema,
                                         table) {
  fact <- dplyr::tbl(con,
                     from = dbplyr::in_schema(schema, table)) |>
    dplyr::mutate(PATIENT_COUNT = case_when(PATIENT_IDENTIFIED == "Y" ~ 1,
                                            TRUE ~ 0)) |>
    dplyr::group_by(
      FINANCIAL_YEAR,
      IDENTIFIED_PATIENT_ID,
      PATIENT_IDENTIFIED,
      PARAGRAPH_DESCR,
      BNF_PARAGRAPH,
      CALC_AGE,
      GENDER_DESCR,
      PATIENT_COUNT
    ) |>
    dplyr::summarise(
      ITEM_COUNT = sum(ITEM_COUNT, na.rm = T),
      ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC, na.rm = T),
      .groups = "drop"
    )
  
  fact_age_gender <- fact |>
    filter(GENDER_DESCR != "Unknown") |>
    dplyr::inner_join(dplyr::tbl(con,
                                 from = dbplyr::in_schema("DIM", "AGE_DIM")),
                      by = c("CALC_AGE" = "AGE")) |>
    dplyr::mutate(AGE_BAND = dplyr::case_when(is.na(DALL_5YR_BAND) ~ "Unknown",
                                              TRUE ~ DALL_5YR_BAND)) |>
    dplyr::group_by(
      `Financial Year` = FINANCIAL_YEAR,
      `Age Band` = AGE_BAND,
      `Patient Sex` = GENDER_DESCR,
      `Paragraph Name` = PARAGRAPH_DESCR,
      `Paragraph Code` = BNF_PARAGRAPH,
      `Identified Patient Flag` = PATIENT_IDENTIFIED
    ) |>
    dplyr::summarise(
      `Total Identified Patients` = sum(PATIENT_COUNT, na.rm = T),
      `Total Items` = sum(ITEM_COUNT, na.rm = T),
      `Total Net Ingredient Cost (GBP)` = sum(ITEM_PAY_DR_NIC, na.rm = T) /
        100,
      .groups = "drop"
    ) |>
    dplyr::arrange(`Financial Year`,
                   `Age Band`,
                   desc(`Identified Patient Flag`)) |>
    collect ()
  
  return(fact_age_gender)
  
}

national_presentation <- function(con,
                                  schema,
                                  table) {
  fact <- dplyr::tbl(con,
                     from = dbplyr::in_schema(schema, table)) |>
    group_by(
      `Financial Year` = FINANCIAL_YEAR,
      `BNF Section Code` = BNF_SECTION,
      `BNF Section Name` = SECTION_DESCR,
      `BNF Paragraph Code` = BNF_PARAGRAPH,
      `BNF Paragraph Name` = PARAGRAPH_DESCR,
      `BNF Chemical Substance Code` = BNF_CHEMICAL_SUBSTANCE,
      `BNF Chemical Substance Name` = CHEMICAL_SUBSTANCE_BNF_DESCR,
      `BNF Presentation Code` = PRESENTATION_BNF,
      `BNF Presentation Name` = PRESENTATION_BNF_DESCR
    ) |>
    summarise(
      `Total Items` = sum(ITEM_COUNT, na.rm = T),
      `Total Net Ingredient Cost (GBP)` = sum(ITEM_PAY_DR_NIC, na.rm = T) /
        100,
      .groups = "drop"
    ) |>
    arrange(
      `Financial Year`,
      `BNF Section Code`,
      `BNF Paragraph Code`,
      `BNF Chemical Substance Code`,
      `BNF Presentation Code`
    ) |>
    collect()
  
  return(fact)
  
}

capture_rate_extract <- function(con,
                                 schema,
                                 table) {
  fact <- dplyr::tbl(con,
                     from = dbplyr::in_schema(schema, table))  |>
    dplyr::group_by(
      `Financial Year` = FINANCIAL_YEAR,
      `BNF Paragraph Name` = PARAGRAPH_DESCR,
      `BNF Paragraph Code` = BNF_PARAGRAPH,
      PATIENT_IDENTIFIED
    ) |>
    dplyr::summarise(ITEM_COUNT = sum(ITEM_COUNT, na.rm = T),
                     .groups = "drop") |>
    dplyr::arrange(`Financial Year`) |>
    collect() |>
    tidyr::pivot_wider(names_from = PATIENT_IDENTIFIED,
                       values_from = ITEM_COUNT) |>
    mutate(
      `Identified Patient Rate` = Y / (Y + N) * 100,
      `BNF Paragraph Code` = factor(
        `BNF Paragraph Code`,
        levels = c("060101", "060102", "060104", "060106", "2148")
      )
    ) |>
    dplyr::select(-Y,-N) |>
    dplyr::arrange(`Financial Year`, `BNF Paragraph Code`)
  return(fact)
}

capture_rate_extract_dt <- function(con,
                                    schema,
                                    table) {
  fact <- dplyr::tbl(con,
                     from = dbplyr::in_schema(schema, table)) |>
    dplyr::group_by(
      FINANCIAL_YEAR,
      `BNF Paragraph name` = PARAGRAPH_DESCR,
      `BNF Paragraph code` = BNF_PARAGRAPH,
      PATIENT_IDENTIFIED
    ) |>
    dplyr::summarise(ITEM_COUNT = sum(ITEM_COUNT, na.rm = T),
                     .groups = "drop") |>
    dplyr::arrange(FINANCIAL_YEAR) |>
    collect() |>
    tidyr::pivot_wider(names_from = PATIENT_IDENTIFIED,
                       values_from = ITEM_COUNT) |>
    mutate(`Identified Patient Rate` = Y / (Y + N) * 100) |>
    dplyr::select(-Y,-N) |>
    tidyr::pivot_wider(names_from = FINANCIAL_YEAR,
                       values_from = `Identified Patient Rate`) |>
    dplyr::arrange(`BNF Paragraph code`)
  
  return(fact)
}

costpericb_extract <- function(con,
                               schema,
                               table) {
  fact <- dplyr::tbl(con,
                     from = dbplyr::in_schema(schema, table)) |>
    #    dplyr::filter(CCG_FLAG=="Y") |>
    dplyr::mutate(PATIENT_COUNT = case_when(PATIENT_IDENTIFIED == "Y" ~ 1,
                                            TRUE ~ 0)) |>
    dplyr::group_by(
      FINANCIAL_YEAR,
      IDENTIFIED_PATIENT_ID,
      PATIENT_IDENTIFIED,
      ICB_NAME,
      ICB_CODE,
      PATIENT_COUNT
    ) |>
    dplyr::summarise(
      ITEM_COUNT = sum(ITEM_COUNT, na.rm = T),
      ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC, na.rm = T),
      .groups = "drop"
    )
  
  fact_icb <- fact |>
    dplyr::group_by(
      `Financial Year` = FINANCIAL_YEAR,
      `Identified Patient Flag` = PATIENT_IDENTIFIED,
      `Integrated Care Board Name` = ICB_NAME,
      `Integrated Care Board Code` = ICB_CODE
    ) |>
    dplyr::summarise(
      `Total Identified Patients` = sum(PATIENT_COUNT, na.rm = T),
      `Total Items` = sum(ITEM_COUNT, na.rm = T),
      `Total Net Ingredient Cost (GBP)` = sum(ITEM_PAY_DR_NIC, na.rm = T) /
        100,
      .groups = "drop"
    ) |>
    dplyr::arrange(`Financial Year`,
                   `Integrated Care Board Code`,
                   desc(`Identified Patient Flag`)) |>
    collect()
  
  return(fact_icb)
  
}

costper_patient_extract <- function(con,
                                    schema,
                                    table) {
  fact <- dplyr::tbl(con,
                     from = dbplyr::in_schema(schema, table)) |>
    dplyr::mutate(PATIENT_COUNT = case_when(PATIENT_IDENTIFIED == "Y" ~ 1,
                                            TRUE ~ 0)) |>
    dplyr::group_by(FINANCIAL_YEAR,
                    PATIENT_IDENTIFIED,
                    PATIENT_COUNT) |>
    dplyr::summarise(
      ITEM_COUNT = sum(ITEM_COUNT, na.rm = T),
      ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC, na.rm = T),
      PATIENT_COUNT = sum(PATIENT_COUNT, na.rm = T),
      .groups = "drop"
    )
  
  fact_pat <- fact |>
    dplyr::filter(PATIENT_IDENTIFIED == "Y") |>
    dplyr::group_by(`Financial Year` = FINANCIAL_YEAR) |>
    dplyr::summarise(
      `Total Identified Patients` = sum(PATIENT_COUNT, na.rm = T),
      `Total Items` = sum(ITEM_COUNT, na.rm = T),
      `Total Net Ingredient Cost (GBP)` = sum(ITEM_PAY_DR_NIC, na.rm = T) /
        100,
      .groups = "drop"
    ) |>
    dplyr::mutate(`Total NIC per patient (GBP)` = `Total Net Ingredient Cost (GBP)` /
                    `Total Identified Patients`)  |>
    dplyr::arrange(`Financial Year`) |>
    collect()
  
  return(fact_pat)
  
}

### INFO BOXES

infoBox_border <- function(header = "Header here",
                           text = "More text here",
                           backgroundColour = "#ccdff1",
                           borderColour = "#005EB8",
                           width = "31%",
                           fontColour = "black") {
  #set handling for when header is blank
  display <- "block"
  
  if (header == "") {
    display <- "none"
  }
  
  paste(
    "<div class='infobox_border' style = 'border: 1px solid ",
    borderColour,
    "!important;
  border-left: 5px solid ",
  borderColour,
  "!important;
  background-color: ",
  backgroundColour,
  "!important;
  padding: 10px;
  width: ",
  width,
  "!important;
  display: inline-block;
  vertical-align: top;
  flex: 1;
  height: 100%;'>
  <h4 style = 'color: ",
  fontColour,
  ";
  font-weight: bold;
  font-size: 18px;
  margin-top: 0px;
  margin-bottom: 10px;
  display: ",
  display,
  ";'>",
  header,
  "</h4>
  <p style = 'color: ",
  fontColour,
  ";
  font-size: 16px;
  margin-top: 0px;
  margin-bottom: 0px;'>",
  text,
  "</p>
</div>"
  )
}

infoBox_no_border <- function(header = "Header here",
                              text = "More text here",
                              backgroundColour = "#005EB8",
                              width = "31%",
                              fontColour = "white") {
  #set handling for when header is blank
  display <- "block"
  
  if (header == "") {
    display <- "none"
  }
  
  paste(
    "<div class='infobox_no_border',
    style = 'background-color: ",
    backgroundColour,
    "!important;padding: 10px;
    width: ",
    width,
    ";
    display: inline-block;
    vertical-align: top;
    flex: 1;
    height: 100%;'>
  <h4 style = 'color: ",
  fontColour,
  ";
  font-weight: bold;
  font-size: 18px;
  margin-top: 0px;
  margin-bottom: 10px;
  display: ",
  display,
  ";'>",
  header,
  "</h4>
  <p style = 'color: ",
  fontColour,
  ";
  font-size: 16px;
  margin-top: 0px;
  margin-bottom: 0px;'>",
  text,
  "</p>
</div>"
  )
}

### Chart functions

age_gender_chart <- function(data,
                             labels = FALSE) {
  age_gender_chart_data <- data |>
    dplyr::select(`Age Band`,
                  `Patient Gender`,
                  `Total Identified Patients`) |>
    tidyr::complete(`Patient Gender`,
                    `Age Band`,
                    fill = list(`Total Identified Patients` = 0))
  
  categories = c(unique(age_gender_chart_data$`Age Band`))
  
  max <-
    max(age_gender_chart_data$`Total Identified Patients`)
  min <-
    max(age_gender_chart_data$`Total Identified Patients`) * -1
  
  male <- age_gender_chart_data |>
    dplyr::filter(`Patient Gender` == "Male")
  
  female <- age_gender_chart_data |>
    dplyr::filter(`Patient Gender` == "Female") |>
    dplyr::mutate(`Total Identified Patients` = 0 - `Total Identified Patients`)
  
  hc <- highcharter::highchart() |>
    highcharter::hc_chart(type = 'bar') |>
    hc_chart(style = list(fontFamily = "Arial")) |>
    highcharter::hc_xAxis(
      list(
        title = list(text = "Age group"),
        categories = categories,
        reversed = FALSE,
        labels = list(step = 1)
      ),
      list(
        categories = categories,
        opposite = TRUE,
        reversed = FALSE,
        linkedTo = 0,
        labels = list(step = 1)
      )
    ) |>
    highcharter::hc_tooltip(
      shared = FALSE,
      formatter = JS(
        "function () {
                   return this.point.category + '<br/>' +
                   '<b>' + this.series.name + '</b> ' +
                   Highcharts.numberFormat(Math.abs(this.point.y), 0);}"
      )
    ) |>
    highcharter::hc_yAxis(
      title = list(text = "Identified patients"),
      max = max,
      min = min,
      labels = list(
        formatter = JS(
          'function () {
               result = Math.abs(this.value);
               if (result >= 1000000) {result = result / 1000000;
                                                            return Math.round(result.toPrecision(3)) + "M"}
                                  else if (result >= 1000) {result = result / 1000;
                                                              return Math.round(result.toPrecision(3)) + "K"}
               return result;
             }'
        )
      )
    ) |>
    highcharter::hc_plotOptions(series = list(stacking = 'normal')) |>
    highcharter::hc_series(
      list(
        dataLabels = list(
          enabled = labels,
          inside = FALSE,
          color = '#8e5300',
          fontFamily = "Ariel",
          formatter = JS(
            'function () {
                                  result = this.y;
                                  if (result >= 1000000) {result = result / 1000000;
                                                            return Math.round(result.toPrecision(3)) + "M"}
                                  else if (result >= 1000) {result = result / 1000;
                                                              return Math.round(result.toPrecision(3)) + "K"}
                                  return result;
                                  }'
          )
        ),
        color = "#8e5300",
        fontFamily = "Ariel",
        name = 'Male',
        data = c(male$`Total Identified Patients`)
      ),
      list(
        dataLabels = list(
          enabled = labels,
          inside = FALSE,
          color = '#003087',
          fontFamily = "Ariel",
          formatter = JS(
            'function () {
                                  result = this.y * -1;
                                  if (result >= 1000000) {result = result / 1000000;
                                                            return result.toPrecision(3) + "M"}
                                  else if (result >= 1000) {result = result / 1000;
                                                              return result.toPrecision(3) + "K"}
                                  return result;
                                  }'
          )
        ),
        color = "#003087",
        name = 'Female',
        fontFamily = "Ariel",
        data = c(female$`Total Identified Patients`)
      )
    ) |>
    highcharter::hc_legend(reversed = T)
  
  return(hc)
  
}

### CSV Download button
get_download_button <-
  function(data = data,
           title = "Download chart data",
           filename = "data") {
    dt <- datatable(
      data,
      rownames = FALSE,
      extensions = 'Buttons',
      options = list(
        searching = FALSE,
        paging = TRUE,
        bInfo = FALSE,
        pageLength = 1,
        dom = '<"datatable-wrapper"B>',
        buttons = list(
          list(
            extend = 'csv',
            text = title,
            filename = filename,
            className = "nhs-button-style"
          )
        ),
        initComplete = JS(
          "function(settings, json) {",
          "$(this.api().table().node()).css('visibility', 'collapse');",
          "}"
        )
      )
    )
    
    return(dt)
  }

### Statistical Disclosure Control

apply_sdc <-
  function(data,
           level = 5,
           rounding = TRUE,
           round_val = 5,
           mask = "") {
    `%>%` <- magrittr::`%>%`
    
    rnd <- round_val
    
    if (is.character(mask)) {
      type <- function(x)
        as.character(x)
    } else {
      type <- function(x)
        x
    }
    
    data %>% dplyr::mutate(dplyr::across(
      where(is.numeric),
      .fns = ~ dplyr::case_when(
        .x >= level &
          rounding == T ~ as.numeric(type(rnd * round(.x / rnd))),
        .x < level & .x > 0 & rounding == T ~ as.numeric(mask),
        .x < level & .x > 0 & rounding == F ~ as.numeric(mask),
        TRUE ~ as.numeric(type(.x))
      ),
      .names = "{.col}"
    ))
  }
