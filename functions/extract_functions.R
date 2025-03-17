### extract functions for use in main pipeline
# these functions extract data at a selected level of granularity from the 
# PfD fact table in the data warehouse, in preparation for further analysis in R

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
      `Total Net Ingredient Cost (£)` = sum(ITEM_PAY_DR_NIC, na.rm = T) /
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
      `Total Net Ingredient Cost (£)` = sum(ITEM_PAY_DR_NIC, na.rm = T) /
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
  #count patients based on identified patient flag value
  fact <- dplyr::tbl(con,
                     from = dbplyr::in_schema(schema, table)) |>
    dplyr::mutate(PATIENT_COUNT = case_when(PATIENT_IDENTIFIED == "Y" ~ 1,
                                            TRUE ~ 0),
                  #apply fix to age of 169
                  CALC_AGE = case_when(
                    CALC_AGE == 169 ~ 69,
                    TRUE ~ CALC_AGE
                  )) |>
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
  #create child-adult split using patient age of under 18 or 18 and over
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
      `Total Net Ingredient Cost (£)` = sum(ITEM_PAY_DR_NIC, na.rm = T) /
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
      `Total Net Ingredient Cost (£)` = sum(ITEM_PAY_DR_NIC, na.rm = T) /
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
      `Total Net Ingredient Cost (£)` = sum(ITEM_PAY_DR_NIC, na.rm = T) /
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
                                            TRUE ~ 0),
                  #apply fix to age of 169
                  CALC_AGE = case_when(
                    CALC_AGE == 169 ~ 69,
                    TRUE ~ CALC_AGE
                  )) |>
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
      `Total Net Ingredient Cost (£)` = sum(ITEM_PAY_DR_NIC, na.rm = T) /
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
                                            TRUE ~ 0),
                  #apply fix to age of 169
                  CALC_AGE = case_when(
                    CALC_AGE == 169 ~ 69,
                    TRUE ~ CALC_AGE
                  )) |>
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
      `Total Net Ingredient Cost (£)` = sum(ITEM_PAY_DR_NIC, na.rm = T) /
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
    dplyr::mutate(
      PAT_GENDER = case_when(
        PAT_GENDER == "Female" ~ "Female",
        PAT_GENDER == "Male" ~ "Male",
        TRUE ~ "Unknown"
      )
    ) |>
    dplyr::group_by(
      FINANCIAL_YEAR,
      IDENTIFIED_PATIENT_ID,
      PATIENT_IDENTIFIED,
      PAT_GENDER,
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
      `Patient Gender` = PAT_GENDER,
      `Identified Patient Flag` = PATIENT_IDENTIFIED
    ) |>
    dplyr::summarise(
      `Total Identified Patients` = sum(PATIENT_COUNT, na.rm = T),
      `Total Items` = sum(ITEM_COUNT, na.rm = T),
      `Total Net Ingredient Cost (£)` = sum(ITEM_PAY_DR_NIC, na.rm = T) /
        100,
      .groups = "drop"
    ) |>
    dplyr::arrange(`Financial Year`,
                   `Patient Gender`,
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
    dplyr::mutate(
      PAT_GENDER = case_when(
        PAT_GENDER == "Female" ~ "Female",
        PAT_GENDER == "Male" ~ "Male",
        TRUE ~ "Unknown"
      )
    ) |>
    dplyr::group_by(
      FINANCIAL_YEAR,
      IDENTIFIED_PATIENT_ID,
      PATIENT_IDENTIFIED,
      PARAGRAPH_DESCR,
      BNF_PARAGRAPH,
      PAT_GENDER,
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
      `Patient Gender` = PAT_GENDER,
      `Identified Patient Flag` = PATIENT_IDENTIFIED
    ) |>
    dplyr::summarise(
      `Total Identified Patients` = sum(PATIENT_COUNT, na.rm = T),
      `Total Items` = sum(ITEM_COUNT, na.rm = T),
      `Total Net Ingredient Cost (£)` = sum(ITEM_PAY_DR_NIC, na.rm = T) /
        100,
      .groups = "drop"
    ) |>
    dplyr::arrange(`Financial Year`,
                   `Paragraph Code`,
                   `Patient Gender`,
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
                                            TRUE ~ 0),
                  #apply fix to age of 169
                  CALC_AGE = case_when(
                    CALC_AGE == 169 ~ 69,
                    TRUE ~ CALC_AGE
                  )) |>
    dplyr::mutate(
      PAT_GENDER = case_when(
        PAT_GENDER == "Female" ~ "Female",
        PAT_GENDER == "Male" ~ "Male",
        TRUE ~ "Unknown"
      )
    ) |>
    dplyr::group_by(
      FINANCIAL_YEAR,
      IDENTIFIED_PATIENT_ID,
      PATIENT_IDENTIFIED,
      CALC_AGE,
      PAT_GENDER,
      PATIENT_COUNT
    ) |>
    dplyr::summarise(
      ITEM_COUNT = sum(ITEM_COUNT, na.rm = T),
      ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC, na.rm = T),
      .groups = "drop"
    )
  
  fact_age_gender <- fact |>
    filter(PAT_GENDER != "Unknown") |>
    dplyr::inner_join(dplyr::tbl(con,
                                 from = dbplyr::in_schema("DIM", "AGE_DIM")),
                      by = c("CALC_AGE" = "AGE")) |>
    dplyr::mutate(AGE_BAND = dplyr::case_when(is.na(DALL_5YR_BAND) ~ "Unknown",
                                              TRUE ~ DALL_5YR_BAND)) |>
    dplyr::group_by(
      `Financial Year` = FINANCIAL_YEAR,
      `Age Band` = AGE_BAND,
      `Patient Gender` = PAT_GENDER,
      `Identified Patient Flag` = PATIENT_IDENTIFIED
    ) |>
    dplyr::summarise(
      `Total Identified Patients` = sum(PATIENT_COUNT, na.rm = T),
      `Total Items` = sum(ITEM_COUNT, na.rm = T),
      `Total Net Ingredient Cost (£)` = sum(ITEM_PAY_DR_NIC, na.rm = T) /
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
                                            TRUE ~ 0),
                  #apply fix to age of 169
                  CALC_AGE = case_when(
                    CALC_AGE == 169 ~ 69,
                    TRUE ~ CALC_AGE
                  )) |>
    dplyr::mutate(
      PAT_GENDER = case_when(
        PAT_GENDER == "Female" ~ "Female",
        PAT_GENDER == "Male" ~ "Male",
        TRUE ~ "Unknown"
      )
    ) |>
    dplyr::group_by(
      FINANCIAL_YEAR,
      IDENTIFIED_PATIENT_ID,
      PATIENT_IDENTIFIED,
      PARAGRAPH_DESCR,
      BNF_PARAGRAPH,
      CALC_AGE,
      PAT_GENDER,
      PATIENT_COUNT
    ) |>
    dplyr::summarise(
      ITEM_COUNT = sum(ITEM_COUNT, na.rm = T),
      ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC, na.rm = T),
      .groups = "drop"
    )
  
  fact_age_gender <- fact |>
    filter(PAT_GENDER != "Unknown") |>
    dplyr::inner_join(dplyr::tbl(con,
                                 from = dbplyr::in_schema("DIM", "AGE_DIM")),
                      by = c("CALC_AGE" = "AGE")) |>
    dplyr::mutate(AGE_BAND = dplyr::case_when(is.na(DALL_5YR_BAND) ~ "Unknown",
                                              TRUE ~ DALL_5YR_BAND)) |>
    dplyr::group_by(
      `Financial Year` = FINANCIAL_YEAR,
      `Age Band` = AGE_BAND,
      `Patient Gender` = PAT_GENDER,
      `Paragraph Name` = PARAGRAPH_DESCR,
      `Paragraph Code` = BNF_PARAGRAPH,
      `Identified Patient Flag` = PATIENT_IDENTIFIED
    ) |>
    dplyr::summarise(
      `Total Identified Patients` = sum(PATIENT_COUNT, na.rm = T),
      `Total Items` = sum(ITEM_COUNT, na.rm = T),
      `Total Net Ingredient Cost (£)` = sum(ITEM_PAY_DR_NIC, na.rm = T) /
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
      `Total Net Ingredient Cost (£)` = sum(ITEM_PAY_DR_NIC, na.rm = T) /
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

cost_per_icb_extract <- function(con,
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
      `Integrated Care Board Name` = ICB_NAME,
      `Integrated Care Board Code` = ICB_CODE,
      `Identified Patient Flag` = PATIENT_IDENTIFIED
    ) |>
    dplyr::summarise(
      `Total Identified Patients` = sum(PATIENT_COUNT, na.rm = T),
      `Total Items` = sum(ITEM_COUNT, na.rm = T),
      `Total Net Ingredient Cost (£)` = sum(ITEM_PAY_DR_NIC, na.rm = T) /
        100,
      .groups = "drop"
    ) |>
    dplyr::arrange(`Financial Year`,
                   `Integrated Care Board Code`,
                   desc(`Identified Patient Flag`)) |>
    collect()
  
  return(fact_icb)
  
}

cost_per_patient_extract <- function(con,
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
      `Total Net Ingredient Cost (£)` = sum(ITEM_PAY_DR_NIC, na.rm = T) /
        100,
      .groups = "drop"
    ) |>
    dplyr::mutate(`Total NIC per patient (£)` = `Total Net Ingredient Cost (£)` /
                    `Total Identified Patients`)  |>
    dplyr::arrange(`Financial Year`) |>
    collect()
  
  return(fact_pat)
  
}