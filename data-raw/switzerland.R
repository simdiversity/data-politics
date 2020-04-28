library("tidyverse")
library("readxl")
library("jsonlite")
library("usethis")

debug <- FALSE
# Prepare

# Download the swiss parlamentary xlsx file from <https://www.parlament.ch/de/ratsbetrieb/abstimmungen/abstimmung-nr-xls> into your *original-data_path* variable.

## Download original data

na_unique <- function(x){
  result = unique(x[!is.na(x)])
  result
}

check_import_errors <- function(imported_data, g_id) {

  g_id <- enquo(g_id)

  grouped_data <-
    imported_data %>%
    group_by(!!g_id)

  if (any(group_size(grouped_data) > 1)){
    imported_data <-
      imported_data %>%
      group_by(!!g_id, add= TRUE) %>%
      filter(row_number() == 1)
    if (debug) {
      message(
        "There where some import errors.
         Duplicated rows have been unified\n"
      )
    }
  }
  imported_data
}


import_swiss_politics <- function(legislative_period, tmp_data_dir) {

  votes_column_indeces <- list(
    "d" = c("VoteRegistrationNumber", "VoteDate", "Rat", "Kommission",
            "Dept.", "AffairId", "AffairTitle",
         "VoteMeaningYes", "VoteMeaningNo",
        "DivisionText", "VoteSubmissionText", "Decision", "Ja", "Nein", "Enth.",
        "Entschuldigung gem. Art. 57 Abs. 4", "Hat nicht teilgenommen",
        "Pr\u00e4sident"),
    "f" = c("VoteRegistrationNumber", "VoteDate", "Conseil", "Commission",
            "Dept.", "AffairId",
            "AffairTitle",  "VoteMeaningYes",
            "VoteMeaningNo", "DivisionText", "VoteSubmissionText", "Decision",
            "Oui", "Non", "Abstenu", "Excus\u00e9", "Absent", "Pr\u00e9sident")
,   "i" = c("VoteRegistrationNumber", "VoteDate", "Consiglio", "Commissione",
            "Dept.", "AffairId",
            "AffairTitle",  "VoteMeaningYes",
            "VoteMeaningNo", "DivisionText", "VoteSubmissionText", "Decision",
            "Si", "No", "Astenuto",  "Scusato", "Assente", "Presidente")
  )

  variables <- NULL
  individuals <- NULL
  scores <- NULL

  file_list <- list.files(tmp_data_dir,
                         pattern = paste0(legislative_period, ".*.xlsx")
  )

  for (filename in file_list ) {
    filepath <- paste0(tmp_data_dir, "/", filename)
    if (debug) {
      message(paste("Importing filename.", filepath))
    }
    matches <- str_match(filename, "([0-9]+?)-([0-9]+?)-(.*?)-([a-z]+?)\\.xlsx")
    session_code <- matches[,2]
    session_year <- matches[,3]
    session_name <- matches[,4]
    language <- matches[,5]

    if (debug) {
      message("Importing variables.")
    }
    variables <-
      read_xlsx(filepath, skip = 8) %>%
        select(votes_column_indeces[[language]]) %>%
        mutate(
          SessionCode = session_code,
          SessionYear = session_year,
          SessionName = session_name,
          AffairId = as.character(AffairId),
          VoteDate = as.character(VoteDate),
          VoteRegistrationNumber = as.character(VoteRegistrationNumber)
        ) %>%
        rename(v_id = VoteRegistrationNumber ) %>%
        distinct(.keep_all = TRUE) %>%
        arrange(v_id) %>%
        {
          if (is.null(variables)) {
            .
          } else {
            full_join(
              variables,
              .,
              by = c(
                "v_id", "VoteDate", "Rat", "Kommission", "Dept.", "AffairId",
                "AffairTitle", "VoteMeaningYes", "VoteMeaningNo",
                "DivisionText", "VoteSubmissionText", "Decision", "Ja", "Nein",
                "Enth.", "Entschuldigung gem. Art. 57 Abs. 4",
                "Hat nicht teilgenommen", "Pr\u00e4sident", "SessionCode",
                "SessionYear", "SessionName"
              ),
              suffix = c("",paste0(".",session_code))
              )
          }
        }  %>%
      arrange(v_id)

    if (debug) {
       message("Importing scores.")
    }
    scores <-
      read_xlsx(filepath, skip = 8) %>%
        select(-votes_column_indeces[[language]][
          -which(votes_column_indeces[[language]] == "VoteRegistrationNumber")
        ]) %>%
        select(-"...12") %>%
        pivot_longer( cols = -VoteRegistrationNumber, names_to = "i_id") %>%
        distinct(.keep_all = TRUE) %>%
        pivot_wider(
          names_from = VoteRegistrationNumber,
          values_from = value
        ) %>%
        arrange(i_id) %>%
      {
          if (is.null(scores)) {
            .
          } else {
            full_join(
              scores, .,
              by = "i_id",
              suffix = c("",paste0(".",session_code))
            )
          }
        } %>%
      arrange(i_id)

    if (debug) {
      message("Importing individuals.")
    }
    individuals <-
      read_xlsx(filepath, n_max=7) %>%
      select(-starts_with("...")) %>%
      bind_rows(rlang::set_names(colnames(.), colnames(.))) %>%
      pivot_longer(-CouncillorId, names_to = "i_id") %>%
      distinct(.keep_all = TRUE) %>%
      pivot_wider(names_from = CouncillorId, values_from = value) %>%
      mutate(Geburtsdatum = as.character(Geburtsdatum)) %>%
      select(-CouncillorId) %>%
      rename(
        bio_id = CouncillorBioId,
        name = CouncillorName,
        parlamentary_group = Fraktion,
        region = Kanton,
        chamber = Rat,
        birth_date = Geburtsdatum,
        swore_date = Vereidigungsdatum
      ) %>%
      arrange(i_id) %>%
      {
          if (is.null(individuals)) {
            .
          } else {
            full_join(
              individuals, .,
              by =  c(
                "i_id", "bio_id", "name", "chamber", "parlamentary_group",
                "region", "birth_date", "swore_date"
              ),
              suffix = c("", paste0(".",session_code)) )
          }
        } %>%
      arrange(i_id)
  }

  variables <-
    variables %>%
    check_import_errors(v_id)

  individuals <-
    individuals %>%
    check_import_errors(i_id)

  scores <-
    scores %>%
    check_import_errors(i_id)

  # Councillors

  individuals_ws = NULL
  for (bio_id in unique(lapply(individuals$bio_id, as.character))) {
    uri = paste0(
      "http://ws-old.parlament.ch/councillors/",
      bio_id,
      "?nativeLanguageFilter=false&format=json"
    )
    individuals_ws <-
      fromJSON(uri) %>%
      enframe() %>%
      mutate(bio_id = bio_id) %>%
      pivot_wider(names_from = name, values_from = value) %>%
      {
        if (is.null(individuals_ws)) {
          .
        } else {
          bind_rows(individuals_ws, .)
        }
      } %>%
      tibble()
  }

  individuals <- full_join(
    individuals,
    individuals_ws,
    by = "bio_id",
    suffix = c("", ".ws")
  )
  rm(individuals_ws)

  # Scores

  score_coding <- c(
      "Nein"  = 0, "No"  = 0, "Non"  = 0, "Ja" = 1, "Si" = 1, "Oui" = 1,
      "Enthaltung" = 2, "Astensione" = 2, "Abstention" = 2,
      "Der Pr\u00e4sident stimmt nicht" = 3, "Pr\u00e9sident ne vote pas" = 3,
      "Presidente non voto" = 3,
      "Entschuldigt" = 4, "Excus\u00e9" = 4, "Scusato" = 4,
      "NA"  = 5,
      "Hat nicht teilgenommen" = 6, "N'a pas particip\u00e9" = 6,
      "Non ha partecipato" = 6
    )


  if (debug) {
    values_sparql <- "SELECT DISTINCT ?infoAssenza ?espressione
      (COALESCE(?infoAssenza, ?espressione) as ?score)
      WHERE {
        ?vote ocd:rif_votazione [a ocd:votazione].
        OPTIONAL{ ?vote dc:type ?espressione}
        OPTIONAL{ ?vote dc:description ?infoAssenza}
    }"

    get.sparql_query_result(values_sparql) %>%
      tibble() %>%
      mutate( code = recode(score, !!!score_coding)) %>%
      arrange(code)
  }


  # Checks and errors

  individuals <-
    individuals %>%
    mutate(has_scores = (i_id %in% scores$i_id))

  list(
    individuals = individuals,
    variables = variables,
    scores = scores,
    score_codes = score_coding
  )
}

# Download data files ----------------------------------------------------------

urls = c(
  "https://www.parlament.ch/centers/documents/de/5102-2020-fruehjahrssession-d.xlsx",
  "https://www.parlament.ch/centers/documents/de/5101-2019-wintersession-d.xlsx",
  "https://www.parlament.ch/centers/documents/de/parlament_export_session_5019_de_CH.xlsx",
  "https://www.parlament.ch/centers/documents/de/5018-2019-sommersession-d.xlsx",
  "https://www.parlament.ch/centers/documents/de/5017-2019-maisession-d.xlsx",
  "https://www.parlament.ch/centers/documents/de/5016-2019-fruehjahrssession-d.xlsx",
  "https://www.parlament.ch/centers/documents/de/5015-2018-wintersession-d.xlsx",
  "https://www.parlament.ch/centers/documents/de/5014-2018-herbstsession-d.xlsx",
  "https://www.parlament.ch/centers/documents/de/5013-2018-sommersession-d.xlsx",
  "https://www.parlament.ch/centers/documents/de/5012-218-fruehjahrssession-d.xlsx",
  "https://www.parlament.ch/centers/documents/de/5011-2017-wintersession-d.xlsx",
  "https://www.parlament.ch/centers/documents/de/5010-2017-herbstsession-d.xlsx",
  "https://www.parlament.ch/centers/documents/de/5009-2017-sommersession-d.xlsx",
  "https://www.parlament.ch/centers/documents/de/5008-2017-maisession-d.xlsx",
  "https://www.parlament.ch/centers/documents/de/5007-2017-fruehjahrssession-d.xlsx",
  "https://www.parlament.ch/centers/documents/de/5006-2016-wintersession-d.xlsx",
  "https://www.parlament.ch/centers/documents/de/5005-2016-herbstsession-d.xlsx",
  "https://www.parlament.ch/centers/documents/de/5004-2016-sommersession-d.xlsx",
  "https://www.parlament.ch/centers/documents/de/5003-2016-aprilsession-d.xlsx",
  "https://www.parlament.ch/centers/documents/de/5002-2016-fruehjahrssession-d.xlsx",
  "https://www.parlament.ch/centers/documents/de/5001-2015-wintersession-d.xlsx",
  "https://www.parlament.ch/centers/documents/de/4920-2015-herbstsession-d.xlsx",
  "https://www.parlament.ch/centers/documents/de/4919-2015-sommersession-d.xlsx",
  "https://www.parlament.ch/centers/documents/de/4918-2015-sondersession-mai-d.xlsx",
  "https://www.parlament.ch/centers/documents/de/4917-2015-fr%c3%bchjahrssession-d.xlsx",
  "https://www.parlament.ch/centers/documents/de/4916-2014-wintersession-d.xlsx",
  "https://www.parlament.ch/centers/documents/de/4915-2014-herbstsession-d.xlsx",
  "https://www.parlament.ch/centers/documents/de/4914-2014-sommersession-d.xlsx",
  "https://www.parlament.ch/centers/documents/de/4913-2014-sondersession-mai-d.xlsx",
  "https://www.parlament.ch/centers/documents/de/4912-2014-fruehjahrssession-d.xlsx",
  "https://www.parlament.ch/centers/documents/de/4911-2013-wintersession-d.xlsx",
  "https://www.parlament.ch/centers/documents/de/4910-2013-herbstsession-d.xlsx",
  "https://www.parlament.ch/centers/documents/de/4909-2013-sommersession-d.xlsx",
  "https://www.parlament.ch/centers/documents/de/4908-2013-sondersession-april-d.xlsx",
  "https://www.parlament.ch/centers/documents/de/4907-2013-fruehjahrssession-d.xlsx",
  "https://www.parlament.ch/centers/documents/de/4906-2012-wintersession-d.xlsx",
  "https://www.parlament.ch/centers/documents/de/4905-2012-herbstsession-d.xlsx",
  "https://www.parlament.ch/centers/documents/de/4904-2012-sommersession-d.xlsx",
  "https://www.parlament.ch/centers/documents/de/4903-2012-sondersession-mai-d.xlsx",
  "https://www.parlament.ch/centers/documents/de/4902-2012-fruehjahrssession-d.xlsx",
  "https://www.parlament.ch/centers/documents/de/4901-2011-wintersession-d.xlsx"
)

# Create tmporary directory
tmp_data_dir <- tempdir()

for (url in urls) {
  if (basename(url) == "5012-218-fruehjahrssession-d.xlsx" ) {
    destfile <-  "5012-2018-fruehjahrssession-d.xlsx"
  } else
    if (basename(url) == "parlament_export_session_5019_de_CH.xlsx" ) {
      destfile <- "5019-2019-herbstsession-d.xlsx"
    } else {
      destfile <- basename(url)
    }

  destfile = paste0(tmp_data_dir,"/", destfile)
  download.file(url, destfile, quiet = TRUE)
}


# Import data for legislative periods 17 and 18  -------------------------------

swiss_legislator_49 = import_swiss_politics("17", tmp_data_dir)
usethis::use_data(
  swiss_legislator_49,
  compress = "bzip2",
  overwrite = TRUE,
  version = 3
)

swiss_legislator_50 = import_swiss_politics("18", tmp_data_dir)
usethis::use_data(
  swiss_legislator_50,
  compress = "bzip2",
  overwrite = TRUE,
  version = 3
)

# Clean  -----------------------------------------------------------------------
unlink(tmp_data_dir)

