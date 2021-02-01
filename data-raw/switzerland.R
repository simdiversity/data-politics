


library("tidyverse")
library("readxl")
library("httr")
library("usethis")
library("here")

debug <- FALSE


na_unique <- function(x) {
  result <- unique(x[!is.na(x)])
  result
}

check_import_errors <- function(imported_data, g_id) {
  g_id <- enquo(g_id)

  grouped_data <-
    imported_data %>%
    group_by(!!g_id)

  if (any(group_size(grouped_data) > 1)) {
    imported_data <-
      imported_data %>%
      group_by(!!g_id, add = TRUE) %>%
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


ws_api <- function(uri, params = c()) {
  params[["format"]] <- "json"
  params[["pretty"]] <- "true"
  json <- NULL
  get_data <- function(uri, params) {
    if (debug) message(paste(uri, params, "\n"))
    json <- GET(uri, query = params, add_headers("Accept" = "application/json", "accept-charset" = "utf-8"))
    if (debug) message(json$url)
    json <- content(json, "text")

    if (stringr::str_ends(json, fixed("\"hasMorePages\": true\r\n  }\r\n]"))) {
      if ("pageNumber" %in% names(params)) {
        params[["pageNumber"]] <- as.integer(params[["pageNumber"]]) + 1
      } else {
        params[["pageNumber"]] <- 2
      }
      json <- paste(stringr::str_sub(json, end = -2), ",", stringr::str_sub(get_data(uri, params), start = 2))
    }
    json
  }
  if (debug) message("returning fetched json as tibble")
  jsonlite::fromJSON(get_data(uri, params), simplifyDataFrame = FALSE, flatten = FALSE) %>%
    tibble() %>%
    `colnames<-`("entries") %>%
    unnest_wider(entries)
}

as_matrix <- function(x, rownames_column) {
  rownames_column <- enquo(rownames_column)
  if (!tibble::is_tibble(x)) stop("x must be a tibble")
  x <- x %>%
    select(sort(names(.))) %>%
    arrange(!!rownames_column)
  x <- column_to_rownames(x, quo_name(rownames_column))
  y <- as.matrix.data.frame(x)
  colnames(y) <- colnames(x)
  rownames(y) <- rownames(x)
  y
}


# Download data files ----------------------------------------------------------

urls <- c(
  "https://www.parlament.ch/centers/documents/de/5102-2020-fruehjahrssession-d.xlsx",
  "https://www.parlament.ch/centers/documents/de/5101-2019-wintersession-d.xlsx",
  "https://www.parlament.ch/centers/documents/de/parlament_export_session_5019_de_CH.xlsx",
  "https://www.parlament.ch/centers/documents/de/5018-2019-sommersession-d.xlsx",
  "https://www.parlament.ch/centers/documents/de/5017-2019-maisession-d.xlsx",
  "https://www.parlament.ch/centers/documents/de/5016-2019-fruehjahrssession-d.xlsx",
  "https://www.parlament.ch/centers/documents/de/5015-2018-wintersession-d.xlsx",
  "https://www.parlament.ch/centers/documents/de/5014-2018-herbstsession-d.xlsx",
  "https://www.parlament.ch/centers/documents/de/5013-2018-sommersession-d.xlsx",
  "https://www.parlament.ch/centers/documents/de/5012-2018-fruehjahrssession-d.xlsx",
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
tmp_data_dir <- tempdir(check = TRUE)

for (url in urls) {
  if (basename(url) == "5012-218-fruehjahrssession-d.xlsx") {
    destfile <- "5012-2018-fruehjahrssession-d.xlsx"
  } else {
    destfile <- basename(url)
  }

  destfile <- file.path(tmp_data_dir, destfile)
  download.file(url, destfile, quiet = TRUE)
  
}



import_swiss_politics <- function(legislative_period, tmp_data_dir) {

  file_list <- list.files(tmp_data_dir,
    pattern = paste0(legislative_period, ".*.xlsx")
  )

  # variables ------------------------------------------------------------------
  if (debug) message("Importing variables.")

  variables <- NULL

  votes_column_indeces <- c(
    "VoteRegistrationNumber", "VoteDate", "Rat", "Kommission",
    "Dept.", "AffairId", "AffairTitle",
    "VoteMeaningYes", "VoteMeaningNo",
    "DivisionText", "VoteSubmissionText"
  )

  for (filename in file_list) {
    filepath <- paste0(tmp_data_dir, "/", filename)
    if (debug) message(paste("Importing filename.", filepath))

    matches <- str_match(filename, "([0-9]+?)-([0-9]+?)-(.*?)-([a-z]+?)\\.xlsx")
    session_code <- matches[, 2]
    session_year <- matches[, 3]
    session_name <- matches[, 4]
    language <- matches[, 5]

    variables <-
      read_xlsx(filepath, skip = 8) %>%
      select(all_of(votes_column_indeces)) %>%
      rename(
        v_id = VoteRegistrationNumber,
        vote_date = VoteDate,
      ) %>%
      mutate(
        v_id = as.character(v_id),
        vote_date = as.character(vote_date),
        AffairId = as.character(AffairId)
      ) %>%
      pivot_longer(cols = -v_id) %>%
      mutate(
        session_code = session_code
      ) %>%
      {
        if (is.null(variables)) {
          .
        } else {
          bind_rows(
            variables,
            .
          )
        }
      }
  }
  variables <-
    variables %>%
    distinct(.keep_all = TRUE) %>%
    pivot_wider(names_from = name, values_from = value) %>%
    arrange(v_id) %>%
    type_convert(
      cols(
        v_id = col_character(),
        session_code = col_integer(),
        vote_date = col_datetime(format = ""),
        Rat = col_character(),
        Kommission = col_character(),
        Dept. = col_character(),
        AffairId = col_character(),
        AffairTitle = col_character(),
        VoteMeaningYes = col_character(),
        VoteMeaningNo = col_character(),
        DivisionText = col_character(),
        VoteSubmissionText = col_character()
      )
    ) %>%
    rename(
      affair_id = AffairId,
      affair_title = AffairTitle,
      committee = Kommission,
      concernes_department = Dept.,
      meaning_yes = VoteMeaningYes,
      meaning_no = VoteMeaningNo,
      vote_type = DivisionText,
      vote_text = VoteSubmissionText
    ) %>%
    select(-Rat)


  # Scores ---------------------------------------------------------------------
  if (debug) message("Importing scores.")
  scores <- NULL

  not_scores_column_indeces <- c(
    votes_column_indeces[
      -which(votes_column_indeces == "VoteRegistrationNumber")
    ], "Decision", "Ja", "Nein", "Enth.",
    "Entschuldigung gem. Art. 57 Abs. 4", "Hat nicht teilgenommen",
    "Pr\u00e4sident"
  )

  for (filename in file_list) {
    filepath <- paste0(tmp_data_dir, "/", filename)
    if (debug) message(paste("Importing filename.", filepath))

    matches <- str_match(filename, "([0-9]+?)-([0-9]+?)-(.*?)-([a-z]+?)\\.xlsx")
    session_code <- matches[, 2]
    session_year <- matches[, 3]
    session_name <- matches[, 4]
    language <- matches[, 5]

    scores <-
      read_xlsx(filepath, skip = 8) %>%
      select(-all_of(not_scores_column_indeces)) %>%
      select(-"...12") %>%
      pivot_longer(cols = -VoteRegistrationNumber, names_to = "i_id") %>%
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
            suffix = c("", paste0(".", session_code))
          )
        }
      } %>%
      arrange(i_id)
  }

  # Individuals ----------------------------------------------------------------
  if (debug) message("Importing individuals.")
  individuals <- NULL

  for (filename in file_list) {
    filepath <- paste0(tmp_data_dir, "/", filename)
    if (debug) message(paste("Importing filename.", filepath))

    matches <- str_match(filename, "([0-9]+?)-([0-9]+?)-(.*?)-([a-z]+?)\\.xlsx")
    session_code <- matches[, 2]
    session_year <- matches[, 3]
    session_name <- matches[, 4]
    language <- matches[, 5]

    individuals <-
      read_xlsx(filepath, n_max = 7) %>%
      select(-starts_with("...")) %>%
      bind_rows(rlang::set_names(colnames(.), colnames(.))) %>%
      pivot_longer(-CouncillorId, names_to = "c_id") %>%
      rename(name = CouncillorId) %>%
      select(c_id, name, value) %>%
      mutate(
        c_id = as.character(c_id),
        session_code = session_code
      ) %>%
      distinct(.keep_all = TRUE) %>%
      {
        if (is.null(individuals)) {
          .
        } else {
          bind_rows(
            individuals,
            .
          )
        }
      } %>%
      distinct(.keep_all = TRUE) %>%
      arrange(c_id)
  }

  individuals <-
    individuals %>% pivot_wider(c(c_id, session_code))

  sessions <-
    individuals %>%
    select(session_code) %>%
    distinct() %>%
    unlist(use.names = FALSE)

  individuals <-
    individuals %>%
    distinct(c_id, CouncillorBioId, .keep_all = TRUE) %>%
    mutate(uri = paste0(
      "http://ws-old.parlament.ch/councillors/",
      CouncillorBioId
    )) %>%
    mutate(i_id = as.integer(CouncillorBioId)) %>%
    select(i_id, c_id, uri) %>%
    distinct()


  # Councillors Web Service ----------------------------------------------------

  legislative_period_end = "2015-09-25T00:00:00Z"
  if (legislative_period == "50") {
    legislative_period_end = "2019-09-27T00:00:00Z"
  }

  individuals_ws <- ws_api("http://ws-old.parlament.ch/councillors/historic", list(legislativePeriodFromFilter = legislative_period))

  individuals_ws <-
    individuals_ws %>%
    rename(
      i_id = id,
      home_place = placeOfCitizenship,
      birth_date = birthDate,
      death_date = dateOfDeath,
      first_name = firstName,
      last_name = lastName
    ) %>%
    select(-`function`, -hasMorePages, -council, -ends_with("Mask"), -updated) %>%
    unnest_wider(party) %>%
    rename(party_name = abbreviation, ) %>%
    select(-id) %>%
    unnest_wider(canton) %>%
    select(-code, -id) %>%
    rename(canton = abbreviation) %>%
    unnest_wider(faction) %>%
    rename(faction = abbreviation) %>%
    select(-id) %>%
    unnest_wider(membership) %>%
    select(-id) %>%
    distinct(.keep_all = TRUE) %>%
    replace_na(list(leavingDate = legislative_period_end)) %>%
    mutate(time = as.Date(leavingDate) - as.Date(entryDate)) %>%
    mutate(start_date = entryDate, end_date = leavingDate) %>%
    nest(party = c(party_name, start_date, end_date)) %>%
    mutate(start_date = entryDate, end_date = leavingDate) %>%
    nest(faction = c(faction, start_date, end_date)) %>%
    rename(
      start_mandate = entryDate,
      end_mandate = leavingDate
    )

  membership_beginnings <-
    individuals_ws %>%
    group_by(i_id) %>%
    summarise_at(vars(start_mandate), min) %>%
    distinct(.keep_all = TRUE)

  end_mandateings <-
    individuals_ws %>%
    group_by(i_id) %>%
    summarise_at(vars(end_mandate), max) %>%
    distinct(.keep_all = TRUE)

  factions <-
    individuals_ws %>%
    select(i_id, faction) %>%
    unnest(faction) %>%
    distinct(.keep_all = TRUE) %>%
    type_convert(
      cols(
        faction = col_character(),
        start_date = col_datetime(format = "%Y-%m-%dT00:00:00Z"),
        end_date = col_datetime(format = "%Y-%m-%dT00:00:00Z")
      )
    ) %>%
    group_by(i_id) %>%
    group_nest() %>%
    rename(factions = data)

  parties <-
    individuals_ws %>%
    select(i_id, party) %>%
    unnest(party) %>%
    distinct(.keep_all = TRUE) %>%
    type_convert(
      cols(
        party_name = col_character(),
        start_date = col_datetime(format = "%Y-%m-%dT00:00:00Z"),
        end_date = col_datetime(format = "%Y-%m-%dT00:00:00Z")
      )
    ) %>%
    group_by(i_id) %>%
    group_nest() %>%
    rename(parties = data)

  individuals <-
    individuals_ws %>%
    select(-party, -faction, -start_mandate, -end_mandate, -time) %>%
    distinct(.keep_all = TRUE) %>%
    inner_join(parties, ., by = "i_id") %>%
    inner_join(factions, ., by = "i_id") %>%
    inner_join(membership_beginnings, ., by = "i_id") %>%
    inner_join(end_mandateings, ., by = "i_id") %>%
    arrange(i_id) %>%
    full_join(
      individuals,
      .,
      by = "i_id",
      suffix = c("", ".ws")
    )

  rm(individuals_ws)


  # indicate individuals with no scores
  individuals <-
    individuals %>%
    mutate(has_scores = i_id %in% as.character(scores$i_id)) %>%
    replace_na(list(end_mandate = legislative_period_end)) %>%
    type_convert(
      cols(
        c_id = col_integer(),
        uri = col_character(),
        first_name = col_character(),
        last_name = col_character(),
        canton = col_character(),
        home_place = col_character(),
        gender = col_character(),
        birth_date = col_date(format = "%Y-%m-%dT00:00:00Z"),
        death_date = col_date(format = "%Y-%m-%dT00:00:00Z"),
        end_mandate = col_date(format = "%Y-%m-%dT00:00:00Z"),
        start_mandate = col_date(format = "%Y-%m-%dT00:00:00Z")
      )
    ) %>%
    select(i_id, c_id, uri, first_name, last_name, birth_date, death_date,
           gender, home_place, parties, factions, canton, start_mandate,
           end_mandate, has_scores)

  # Scores
  score_codes <- c(
    "Nein" = 0,
    "Ja" = 1,
    "Enthaltung" = 2,
    "Der Pr\u00e4sident stimmt nicht" = 3,
    "Entschuldigt" = 4,
    "NA" = 5,
    "Hat nicht teilgenommen" = 6
  )

  # Recode ---------------------------------------------------------------------
  if (debug) message("recode scores")
  scores <-
    scores %>%
    mutate_at(vars(-i_id), ~ replace_na(., "NA")) %>%
    mutate_at(vars(-i_id), ~ recode(., !!!score_codes)) %>%
    mutate_at(vars(-i_id), as.integer) %>%
    as_matrix(i_id)


  score_codes <- c(
    `0` = "No",
    `1` = "Yes",
    `2` = "Abstention",
    `3` = "The president is not voting",
    `4` = "Excused",
    `5` = "NA",
    `6` = "Did not participate"
  )

  if (debug) message("return")
  list(
    members_of_parliment = individuals,
    voting_items = variables,
    polls = scores,
    poll_codes = score_codes
  )
}


# Import data for legislative periods 17 and 18  -------------------------------

swiss_legislator_49 <- import_swiss_politics("49", tmp_data_dir)
usethis::use_data(
  swiss_legislator_49,
  compress = "xz",
  overwrite = TRUE,
  version = 3
)

swiss_legislator_50 <- import_swiss_politics("50", tmp_data_dir)
usethis::use_data(
  swiss_legislator_50,
  compress = "xz",
  overwrite = TRUE,
  version = 3
)

# Clean  -----------------------------------------------------------------------
unlink(tmp_data_dir)
