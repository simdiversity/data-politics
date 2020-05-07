
# The italian parlamentary data is avilable on http://dati.camera.it in two distinct forms:
#
# 1. a SPARQL endpoint
# 2. rdf files
#
# From the SPARQL endpoint the vote data of the legilative periods 17 and 18 can be harvested.
# Votes data for previous legislative periods is only available via the rdf files.

library("tools")
library("tidyverse")
library("SPARQL")

debug <- TRUE


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

get.sparql_query_result <-
  function(query,
           endpoint = "http://dati.camera.it/sparql",
           options = "format=application/sparql-results+xml") {
    query_result <- SPARQL(url = endpoint, query = query, extra = options)
    tibble(query_result$results)
  }

individuals_sparql <- "
  SELECT DISTINCT
    ?i_id
    ?councillor_uri
    ?first_name
    ?last_name
    ?info
    ?birth_date
    ?birth_place
    ?death_date
    ?gender
  WHERE {
    ?persona ocd:rif_mandatoCamera ?mandato; a foaf:Person.

    ?councillor_uri a ocd:deputato;
        ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_%s>;
        ocd:rif_mandatoCamera ?mandato.

    OPTIONAL{?councillor_uri dc:description ?info}

    ?councillor_uri foaf:surname ?last_name;
        foaf:firstName ?first_name.

    OPTIONAL{?councillor_uri foaf:gender ?gender}

    OPTIONAL{
      ?persona <http://purl.org/vocab/bio/0.1/Birth> ?nascita.
      ?nascita <http://purl.org/vocab/bio/0.1/date> ?birth_date;
          rdfs:label ?nato; ocd:rif_luogo ?birth_placeUri.
      ?birth_placeUri dc:title ?birth_place.
    }
    OPTIONAL{
      ?persona <http://purl.org/vocab/bio/0.1/Death> ?morte.
      ?morte <http://purl.org/vocab/bio/0.1/date> ?death_date.
    }
    BIND(
      REPLACE(
        str(?councillor_uri), 'http://dati.camera.it/ocd/deputato.rdf/', ''
      )
      AS ?i_id
    )
  }
"

party_names_sparql <- "
SELECT DISTINCT ?party_name_long ?party_name (GROUP_CONCAT(DISTINCT ?old_name; separator=';') AS ?old_names)
WHERE {

  ?gruppo a ocd:gruppoParlamentare;
                   ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_%s>.

  OPTIONAL{?gruppo <http://purl.org/dc/terms/alternative> ?party_name}
  ?gruppo dc:title ?party_name_long.
  OPTIONAL{?gruppo ocd:denominazione [dc:title ?old_name_long]}
  OPTIONAL{?gruppo ocd:denominazione [<http://purl.org/dc/terms/alternative> ?old_name]}
  OPTIONAL{?gruppo ocd:denominazione [dc:date ?name_change_date]}
  FILTER(?old_name != ?party_name)
} GROUP BY ?party_name_long ?party_name
"

individuals_partys_sparql <- "
SELECT DISTINCT ?i_id ?party_name ?start_date	?end_date
WHERE {
  ?councillor_uri a ocd:deputato;
      ocd:rif_mandatoCamera ?mandato;
      ocd:aderisce ?aderisce;
      ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_%s>.

  ?aderisce ocd:rif_gruppoParlamentare ?gruppo.

  ?gruppo dc:title ?long_party_name.
  ?gruppo <http://purl.org/dc/terms/alternative> ?party_name.
  ?aderisce ocd:startDate ?start_date.
  OPTIONAL{?aderisce ocd:endDate ?end_date}

  BIND(
    REPLACE(
      str(?councillor_uri), 'http://dati.camera.it/ocd/deputato.rdf/', ''
    )
    AS ?i_id
  )
}
"

individuals_mandates_sparql <- "
SELECT DISTINCT ?i_id ?electoral_district ?election_list ?election_date
  ?start_mandate ?end_mandate ?end_motive
WHERE {
?mandato a ocd:mandatoCamera;
  ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_%s>;
  ocd:rif_deputato ?councillor_uri.

## mandato

OPTIONAL{?mandato ocd:endDate ?end_mandate}
OPTIONAL{?mandato ocd:startDate ?start_mandate}
OPTIONAL{?mandato ocd:motivoTermine ?end_motive}

## elezione
OPTIONAL{?mandato ocd:rif_elezione ?elezione}
OPTIONAL{?elezione dc:coverage ?electoral_district}
OPTIONAL{?elezione ocd:lista ?election_list}
OPTIONAL{?elezione dc:date ?election_date}


  BIND(
    REPLACE(
      str(?councillor_uri), 'http://dati.camera.it/ocd/deputato.rdf/', ''
    )
    AS ?i_id
  )
}
"

votes_sparql <- "
  SELECT DISTINCT
    ?v_id
    ?date
    ?type
    ?act_id
    ?act_type
    ?act_demanded_by
    ?description
    ?final_vote
    ?trust_vote
    ?secret_vote
  WHERE {
    ?vote_uri a ocd:votazione;
        dc:identifier ?v_id;
        ocd:rif_leg
            <http://dati.camera.it/ocd/legislatura.rdf/repubblica_%s>.

    OPTIONAL{?vote_uri dc:date ?date}
    OPTIONAL{?vote_uri dc:type ?type}
    OPTIONAL{?vote_uri ocd:rif_attoCamera [dc:identifier ?act_id]}
    OPTIONAL{?vote_uri ocd:rif_attoCamera [dc:type ?act_type]}
    OPTIONAL{?vote_uri ocd:rif_attoCamera [ocd:iniziativa ?act_demanded_by]}
    OPTIONAL{?vote_uri rdfs:label ?title}
    OPTIONAL{?vote_uri dc:description ?description}
    OPTIONAL{?vote_uri ocd:votazioneFinale ?final_vote}
    OPTIONAL{?vote_uri ocd:richiestaFiducia ?trust_vote}
    OPTIONAL{?vote_uri ocd:votazioneSegreta ?secret_vote}
  }
  LIMIT %s OFFSET %s
"

scores_party_sparql <- "
  SELECT DISTINCT
    ?v_id
    (GROUP_CONCAT(REPLACE(
          str(?councillor_uri),
          'http://dati.camera.it/ocd/deputato.rdf/', ''
        ); separator=';' ) AS ?i_id )
    (GROUP_CONCAT(?score; separator=';') AS ?scores)
    (GROUP_CONCAT(?party; separator=';') AS ?parties)
  WHERE {
    {
      SELECT DISTINCT ?vote_uri
      WHERE {
          ?vote_uri a ocd:votazione;
              ocd:rif_leg
                    <http://dati.camera.it/ocd/legislatura.rdf/repubblica_%s>.
      } LIMIT %s OFFSET %s
    }

    ?vote_uri dc:identifier ?v_id.
    OPTIONAL {
      ?voto ocd:rif_votazione ?vote_uri;
          ocd:rif_deputato ?councillor_uri.
    }

    OPTIONAL {?voto dc:type ?espressione}
    OPTIONAL {?voto dc:description ?infoAssenza}
    OPTIONAL {?voto ocd:siglaGruppo ?party}
    BIND(COALESCE(?infoAssenza, ?espressione) AS ?score)
  }
  GROUP BY ?v_id
"

scores_sparql <- "
  SELECT DISTINCT
    ?v_id
    (GROUP_CONCAT(REPLACE(
      str(?councillor_uri),
      'http://dati.camera.it/ocd/deputato.rdf/', ''
    ); separator=';') AS ?i_id)
    (GROUP_CONCAT(?score; separator=';')AS ?score)
  WHERE {
    {
      SELECT DISTINCT ?vote_uri
      WHERE {
          ?vote_uri a ocd:votazione;
              ocd:rif_leg
                    <http://dati.camera.it/ocd/legislatura.rdf/repubblica_%s>.
      } LIMIT %s OFFSET %s
    }

    ?vote_uri dc:identifier ?v_id.

      ?voto ocd:rif_votazione ?vote_uri;
          ocd:rif_deputato ?councillor_uri.

     FILTER EXISTS{?councillor_uri a ocd:deputato;
          ocd:rif_mandatoCamera ?mandato.}


    OPTIONAL {?voto dc:type ?espressione}
    OPTIONAL {?voto dc:description ?infoAssenza}
    BIND(COALESCE(?infoAssenza, ?espressione) AS ?score)
  }
  GROUP BY ?v_id
"

region_codes <- c(
  "ABRUZZO - 01" = "ABR", "ABRUZZO - 02" = "ABR", "ABRUZZO" = "ABR",
  "AFRICA ASIA OCEANIA ANTARTIDE" = "AAO",
  "AFRICA, ASIA, OCEANIA E ANTARTIDE" = "AAO", "AMERICA MERIDIONALE" = "AMs",
  "AMERICA MERIDIONALE" = "AMs", "AMERICA SETTENTRIONALE E CENTRALE" = "AMc",
  "AMERICA SETTENTRIONALE E CENTRALE" = "AMc", "BASILICATA - 01" = "BAS",
  "BASILICATA" = "BAS", "CALABRIA - 01" = "CAL", "CALABRIA - 02" = "CAL",
  "CALABRIA" = "CAL", "CAMPANIA 1 - 01" = "CAM1", "CAMPANIA 1 - 02" = "CAM1",
  "CAMPANIA 1 - 03" = "CAM1", "CAMPANIA 1" = "CAM1", "CAMPANIA 2 - 01" = "CAM2",
  "CAMPANIA 2 - 02" = "CAM2", "CAMPANIA 2 - 03" = "CAM2", "CAMPANIA 2" = "CAM2",
  "EMILIA-ROMAGNA - 01" = "EM-R", "EMILIA-ROMAGNA - 02" = "EM-R",
  "EMILIA-ROMAGNA - 03" = "EM-R", "EMILIA-ROMAGNA - 04" = "EM-R",
  "EMILIA-ROMAGNA" = "EM-R", "EUROPA" = "EUR", "EUROPA" = "EUR",
  "FRIULI-VENEZIA GIULIA - 01" = "FVG", "FRIULI-VENEZIA GIULIA" = "FVG",
  "LAZIO 1 - 01" = "LAZ1", "LAZIO 1 - 02" = "LAZ1", "LAZIO 1 - 03" = "LAZ1",
  "LAZIO 1" = "LAZ1", "LAZIO 1" = "LAZ1", "LAZIO 2 - 01" = "LAZ2",
  "LAZIO 2 - 02" = "LAZ2", "LAZIO 2" = "LAZ2", "LIGURIA - 01" = "LIG",
  "LIGURIA - 02" = "LIG", "LIGURIA" = "LIG", "LOMBARDIA 1 - 01" = "LOM1",
  "LOMBARDIA 1 - 02" = "LOM1", "LOMBARDIA 1 - 03" = "LOM1",
  "LOMBARDIA 1 - 04" = "LOM1", "LOMBARDIA 1" = "LOM1", "SARDEGNA - 01" = "SAR",
  "LOMBARDIA 2 - 01" = "LOM2", "LOMBARDIA 2 - 02" = "LOM2", "PUGLIA" = "PUG",
  "LOMBARDIA 2" = "LOM2", "LOMBARDIA 3 - 01" = "LOM3", "SARDEGNA - 02" = "SAR",
  "LOMBARDIA 3 - 02" = "LOM3", "LOMBARDIA 3" = "LOM3", "SARDEGNA" = "SAR",
  "LOMBARDIA 4 - 01" = "LOM4", "LOMBARDIA 4 - 02" = "LOM4",
  "MARCHE - 01" = "MAR", "MARCHE - 02" = "MAR", "MARCHE" = "MAR",
  "MOLISE - 01" = "MOL", "MOLISE" = "MOL", "PIEMONTE 1 - 01" = "PIE1",
  "PIEMONTE 1 - 02" = "PIE1", "PIEMONTE 1" = "PIE1", "PIEMONTE 2 - 01" = "PIE2",
  "PIEMONTE 2 - 02" = "PIE2", "PIEMONTE 2" = "PIE2", "PUGLIA - 01" = "PUG",
  "PUGLIA - 02" = "PUG", "PUGLIA - 03" = "PUG", "PUGLIA - 04" = "PUG",
  "SARDEGNA" = "SAR", "SICILIA 1 - 01" = "SIC1", "SICILIA 1 - 02" = "SIC1",
  "SICILIA 1 - 03" = "SIC1", "SICILIA 1" = "SIC1", "SICILIA 2 - 01" = "SIC2",
  "SICILIA 2 - 02" = "SIC2", "SICILIA 2 - 03" = "SIC2", "SICILIA 2" = "SIC2",
  "TOSCANA - 01" = "TOS", "TOSCANA - 02" = "TOS", "TOSCANA - 03" = "TOS",
  "TOSCANA - 04" = "TOS", "TOSCANA" = "TOS", "TRENTINO-ALTO ADIGE" = "TRE",
  "TRENTINO-ALTO ADIGE/S??DTIROL - 01" = "TRE", "VENETO 2" = "VEN2",
  "TRENTINO-ALTO ADIGE/S??DTIROL" = "TRE", "UMBRIA - 01" = "UMB",
  "UMBRIA" = "UMB", "VALLE D'AOSTA" = "AOS", "VENETO 1" = "VEN1",
  "VENETO 2 - 01" = "VEN2", "VENETO 2 - 02" = "VEN2", "VENETO 2 - 03" = "VEN2"
)

import_itanian_politics <- function(legislative_period) {

  # if (debug) {
  #   periods_sparql <- "SELECT DISTINCT ?leg_with_scores
  #   WHERE {
  #     [] ocd:rif_votazione [];
  #           ocd:rif_deputato [ocd:rif_leg ?leg_with_scores].
  #   }"
  #   get.sparql_query_result(periods_sparql) %>%
  #     unlist(use.names = FALSE) %>%
  #     str_extract("[0-9]+") %>%
  #       {
  #         result <- (legislative_period %in% .)
  #         if (!result) message(
  #           "The scores data is not avilable for the selected legislative perriod."
  #         )
  #       }
  # }

  # Individuals ----------------------------------------------------------------
  if (debug) message("collect parties")

  parties <-
    sprintf(party_names_sparql, legislative_period) %>%
    get.sparql_query_result()

  if (debug) message("collect individuals_parties")
  individuals_parties <-
    sprintf(individuals_partys_sparql, legislative_period) %>%
    get.sparql_query_result() %>%
    arrange(i_id) %>%
    group_by(i_id) %>%
    nest()

  if (debug) message("collect individuals_mandates")
  individuals_mandates <-
    sprintf(individuals_mandates_sparql, legislative_period) %>%
    get.sparql_query_result() %>%
    mutate(
      region = recode(electoral_district, !!!region_codes)
    ) %>%
    group_by(i_id) %>%
    nest()

  if (debug) message("collect individuals")
  individuals <-
    sprintf(individuals_sparql, legislative_period) %>%
    get.sparql_query_result() %>%
    arrange(i_id) %>%
    full_join(individuals_parties, by = "i_id") %>%
    full_join(individuals_mandates, by = "i_id") %>%
    unnest(data.y) %>%
    rename(parties = data.x) %>%
    mutate(
      first_name = toTitleCase(tolower(first_name)),
      last_name = toTitleCase(tolower(last_name)),
      birth_place = toTitleCase(tolower(birth_place))
    ) %>%
    type_convert(cols(
      i_id = col_character(),
      councillor_uri = col_character(),
      first_name = col_character(),
      last_name = col_character(),
      info = col_character(),
      birth_date = col_date(format = "%Y%m%d"),
      birth_place = col_character(),
      death_date = col_date(format = "%Y%m%d"),
      gender = col_factor(),
      electoral_district = col_character(),
      election_list = col_character(),
      election_date = col_date(format = "%Y%m%d"),
      start_mandate = col_date(format = "%Y%m%d"),
      end_mandate = col_date(format = "%Y%m%d"),
      end_motive = col_character(),
      region = col_character()
    )) %>%
    arrange(i_id)



  # Votes ----------------------------------------------------------------------
  if (debug) message("collect variables")
  limit <- 10000
  variables <- NULL
  iter <- 0
  trys <- 10
  repeat {
    query_result <- tryCatch(
      {
        sparql_query <- sprintf(votes_sparql, legislative_period, limit, iter * limit)
        get.sparql_query_result(sparql_query)
      },
      error = function(e) {
        warning(paste("Retrying with offset:", iter * limit, "\n"))
        message(sparql_query)
        message(e)
        trys <- trys - 1
        Sys.sleep(120)
      }
    )


    if (inherits(query_result, "error")) {
      if (trys > 0) {
        next
      }
      stop(query_result)
    }

    if (nrow(query_result) == 0) {
      break
    }

    variables <-
      query_result %>%
      {
        if (is.null(variables)) {
          .
        } else {
          full_join(
            variables,
            .,
            suffix = c("", as.character(iter))
          )
        }
      } %>%
      arrange(v_id)

    iter <- iter + 1
  }
  variables <-
    variables %>%
    mutate(
      final_vote = final_vote == 1,
      trust_vote = trust_vote == 1,
      secret_vote = secret_vote == 1
    ) %>%
    type_convert(cols(
      v_id = col_character(),
      date = col_date(format = "%Y%m%d"),
      type = col_factor(),
      act_id = col_character(),
      act_type = col_factor(),
      act_demanded_by = col_factor(),
      description = col_character(),
      final_vote = col_logical(),
      trust_vote = col_logical(),
      secret_vote = col_logical()
    ))

  # Scores ---------------------------------------------------------------------
  if (debug) message("collect scores")
  party <- FALSE
  limit <- 600
  query <- ifelse(party, scores_party_sparql, scores_sparql)
  scores <- NULL
  iter <- 0
  trys <- 30
  repeat {
    sparql_query <- sprintf(query, legislative_period, limit, (limit * iter))

    query_result <- tryCatch(
      {
        get.sparql_query_result(sparql_query)
      },
      error = function(e) {
        warning(sprintf("Retrying with offset: %i\n", limit * iter))
        message(sparql_query)
        print(e)
        trys <- trys - 1
        Sys.sleep(120)
      }
    )

    if (inherits(query_result, "error")) {
      if (trys > 0) {
        next
      }
      stop(query_result)
    }

    if (nrow(query_result) == 0) {
      break
    }

    scores <-
      query_result %>%
      mutate(
        score = str_split(score, ";"),
        i_id = str_split(i_id, ";")
      ) %>%
      unnest(cols = c(i_id, score)) %>%
      pivot_wider(id_cols = i_id, names_from = v_id, values_from = score) %>%
      {
        if (is.null(scores)) {
          .
        } else {
          full_join(
            scores,
            .,
            by = "i_id",
            suffix = c("", as.character(iter))
          )
        }
      } %>%
      arrange(i_id)

    if (debug) message(sprintf("%s. n of results = %s", iter, nrow(query_result)))

    iter <- iter + 1
  }

  # No scores individuals ------------------------------------------------------
  if (debug) message("add no_scores variable to individuals")
  individuals <-
    individuals %>%
    mutate(has_scores = i_id %in% scores$i_id)

  # Recode ---------------------------------------------------------------------
  if (debug) message("recode scores")
  score_codes <- c(
    "Contrario" = 0,
    "Favorevole" = 1,
    "Astensione" = 2,
    "Presidente di turno" = 3,
    "In missione" = 4,
    "NA" = 5,
    "Non ha partecipato" = 6,
    "Ha votato" = 7
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
      mutate(code = recode(score, !!!score_codes)) %>%
      arrange(code)
  }

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
    `6` = "Did not participate",
    `7` = "Has voted"
  )

  # return ---------------------------------------------------------------------
  if (debug) message("return")
  list(
    members_of_parliment = individuals,
    voting_items = variables,
    polls = scores,
    poll_codes = score_codes,
    partys = parties
  )
}

italian_legislator_18 <- import_itanian_politics("18")
usethis::use_data(
  italian_legislator_18,
  compress = "xz",
  overwrite = TRUE,
  version = 3
)

italian_legislator_17 <- import_itanian_politics("17")
usethis::use_data(
  italian_legislator_17,
  compress = "xz",
  overwrite = TRUE,
  version = 3
)
