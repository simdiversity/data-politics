
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

debug = FALSE

get.sparql_query_result <-
  function(query,
           endpoint = "http://dati.camera.it/sparql",
           options = "format=application/sparql-results+xml"
  ) {
    query_result <- SPARQL(url = endpoint, query = query, extra = options)
    tibble(query_result$results)
  }

individuals_sparql <-  "
  SELECT DISTINCT
    ?i_id
    ?councillor_uri
    (CONCAT(?last_name, ' ', ?first_name) as ?names)
    ?info
    ?birth_date
    ?birth_place
    ?gender
    ?inizioMandato
    ?fineMandato
    ?aggiornamento
    ?electoral_district
    COUNT(DISTINCT ?madatoCamera) as ?numeroMandati
    (CONCAT('[',GROUP_CONCAT(DISTINCT ?groupe_id;separator=','),']')
    as ?groupe)
    (CONCAT('[',GROUP_CONCAT(DISTINCT ?groupe_name;separator=','),']')
    as ?groupe_full)
    (CONCAT('[',GROUP_CONCAT(DISTINCT ?groupe_short;separator=','),']')
    as ?party)
    (CONCAT('[',GROUP_CONCAT(DISTINCT ?start_date_groupe;separator=','),']')
    as ?groupes_start_date)
    (CONCAT('[',GROUP_CONCAT(DISTINCT ?end_date_groupe;separator=','),']')
    as ?groupes_end_date)
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

    ?mandato ocd:rif_elezione ?elezione.
    OPTIONAL{ ?mandato ocd:endDate ?fineMandato }
    OPTIONAL{ ?mandato ocd:startDate ?inizioMandato }

    ?persona ocd:rif_mandatoCamera ?madatoCamera.

    ?elezione dc:coverage ?electoral_district.

    OPTIONAL{ ?councillor_uri ocd:aderisce ?aderisce }
    OPTIONAL{ ?aderisce ocd:rif_gruppoParlamentare ?groupe_id }
    OPTIONAL{ ?aderisce ocd:startDate ?start_date_groupe}
    OPTIONAL{ ?aderisce ocd:endDate ?end_date_groupe }
    OPTIONAL{
      ?groupe_id <http://purl.org/dc/terms/alternative> ?groupe_short
    }
    OPTIONAL{ ?groupe_id dc:title ?groupe_name}
    OPTIONAL{
    ?councillor_uri
        <http://lod.xdams.org/ontologies/ods/modified> ?aggiornamento.
    }

    BIND(
      REPLACE(
        str(?councillor_uri), 'http://dati.camera.it/ocd/deputato.rdf/', ''
      )
      AS ?i_id
    )
  }
  GROUP BY ?i_id ?councillor_uri ?info ?birth_date ?birth_place
    ?gender ?inizioMandato ?fineMandato ?electoral_district ?aggiornamento
    ?last_name ?first_name
"

votes_sparql <- "
  SELECT DISTINCT
    ?v_id
    ?vote_uri
    ?date
    ?title
    ?description
    ?final_vote
    ?voters_n
    ?result
    ?in_favor
    ?against
    ?absetions
  WHERE {
    ?vote_uri a ocd:votazione;
        dc:identifier ?v_id;
        ocd:rif_leg
            <http://dati.camera.it/ocd/legislatura.rdf/repubblica_%s>.

    OPTIONAL{?vote_uri ocd:votazioneFinale ?final_vote}
    OPTIONAL{?vote_uri dc:date ?date}
    OPTIONAL{?vote_uri rdfs:label ?title}
    OPTIONAL{?vote_uri dc:description ?description}
    OPTIONAL{?vote_uri ocd:approvato ?result}
    OPTIONAL{?vote_uri ocd:votanti ?voters_n }
    OPTIONAL{?vote_uri ocd:favorevoli ?in_favor }
    OPTIONAL{?vote_uri ocd:contrari ?against }
    OPTIONAL{?vote_uri ocd:astenuti ?absetions }
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
    OPTIONAL {
      ?voto ocd:rif_votazione ?vote_uri;
          ocd:rif_deputato ?councillor_uri.
    }

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
  "LOMBARDIA 3 - 02" = "LOM3", "LOMBARDIA 3" = "LOM3",  "SARDEGNA" = "SAR",
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

  if (debug) {
    periods_sparql <- "SELECT DISTINCT ?leg_with_scores
    WHERE {
      [] ocd:rif_votazione [];
            ocd:rif_deputato [ocd:rif_leg ?leg_with_scores].
    }"
    get.sparql_query_result(periods_sparql) %>%
      unlist(use.names = FALSE) %>%
      str_extract("[0-9]+") %>%
        {
          result <- (legislative_period %in% .)
          if (!result) message(
            "The scores data is not avilable for the selected legislative perriod."
          )
        }
  }

  # Individuals ----------------------------------------------------------------
  individuals <-
    sprintf(individuals_sparql, legislative_period) %>%
    get.sparql_query_result() %>%
    mutate(
      names = toTitleCase(tolower(names)),
      region = recode(electoral_district, !!!region_codes)
    ) %>%
    arrange(i_id)

  # Votes ----------------------------------------------------------------------
  limit = 10000
  variables <- NULL
  iter <- 0
  trys <- 10
  repeat {
      query_result <- tryCatch(
        {
          sprintf(votes_sparql, legislative_period, limit, iter * limit) %>%
            get.sparql_query_result()
        }, error = function(e) {
          iter * limit %>%
            paste("Retrying with offset:", ., "\n") %>%
            warning()
          trys <- trys - 1
          Sys.sleep(120)
          print(e)
      }
    )


    if (inherits(query_result, "error")) {
      ifelse(trys > 0, next, stop(query_result))
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
            by = c(
              "v_id", "vote_uri", "date", "title", "description", "final_vote",
              "voters_n", "result", "in_favor", "against", "absetions"
            ),
            suffix = c("", as.character(iter))
          )
        }
      } %>%
      arrange(v_id)

    iter <- iter + 1

  }

  # Scores ---------------------------------------------------------------------
  party <- FALSE
  limit <- 600
  query <- ifelse(party, scores_party_sparql, scores_sparql)
  scores <- NULL
  iter <- 0
  trys <- 10
  repeat {
    sparql_query <- sprintf(query, legislative_period, limit, (limit * iter))

    query_result <- tryCatch(
      {
          get.sparql_query_result(sparql_query)
      },
      error = function(e) {
        "Retrying with offset: %i\n" %>%
          sprintf(limit * iter) %>%
          warning()
        print(e)
        trys <- trys - 1
        Sys.sleep(120)
      }
    )

    if (inherits(query_result, "error")) {
      ifelse(trys > 0, next, stop(query_result))
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

    if (debug) {
      "%s. n of results = %s" %>%
        sprintf(iter, nrow(results)) %>%
        message()
    }

    iter <- iter + 1
  }

  # indicate individuals with no scores
  individuals <-
    individuals %>%
    mutate(has_scores = (i_id %in% scores$i_id))

  # Recode ---------------------------------------------------------------------
  score_coding <- c(
    "Favorevole" = 0,
    "Astensione" = 1,
    "Ha votato" = 2,
    "Presidente di turno" = 3,
    "In missione" = 3,
    "Contrario" = 4,
    "NA" = 5,
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
      mutate(code = recode(score, !!!score_coding)) %>%
      arrange(code)
  }

  # return ---------------------------------------------------------------------
  list(
    individuals = individuals,
    variables = variables,
    scores = scores,
    score_codes = score_coding
  )
}

italian_legislator_18 <- import_itanian_politics("18")
usethis::use_data(
  italian_legislator_18,
  compress = "bzip2",
  overwrite = TRUE,
  version = 3
)

italian_legislator_17 <- import_itanian_politics("17")
usethis::use_data(
  italian_legislator_17,
  compress = "bzip2",
  overwrite = TRUE,
  version = 3
)
