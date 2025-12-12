#' Get WRDS data
#'
#' Returns WRDS data based on user query
#'
#' @param identifier A string of format product.file. Product and file can be found on the Variable Descriptions page of your dataset in WRDS
#' @param filters A string in URL format specifying filters. Can easily be generated using the applyFilters function.
#' @param authentication The user's WRDS API key, accessible at wrds-api.wharton.upenn.edu. By default, the function looks for a .wrdstoken file in user's home directory that contains the API key.
#' @param root The root of your desired WRDS data's API endpoint. Usually either data or data-full. If one doesn't work, try the other.
#' @param quiet When set to TRUE, suppresses the progress bar indicating download status.
#' @param multithread Accepts either "auto" or "off" (default is "auto"). When set to "auto", getWRDS will determine if the dataset is large enough to justify the overhead of parallelization.
#'
#' @import foreach
#' @export
getWRDS = function(identifier, filters = "", authentication = getTokenFromHome(),
                   root = "data", quiet = F, multithread = c("auto", "off")) {
  count = getWRDScount(identifier, filters, authentication, root)

  url_vector = getURLvector(identifier, filters, root, count)

  if (multithread[1] == "auto") {
    available_cores = as.numeric(parallelly::availableCores())
    use_cores = max(min(floor(length(url_vector) / 2), available_cores - 1), 1)
  } else {
    use_cores = 1
  }

  use_multithread = as.logical(use_cores > 1)

  .fetch_chunk = function(.url, .progress_bar = NULL, .curl_handle = NULL) {
    if (is.null(.curl_handle)) {
      .curl_handle = curl::new_handle(useragent = "Lynx")
      curl::handle_setheaders(.curl_handle, Authorization = getAuthHeader(authentication))
    }
    .this_response = curl::curl_fetch_memory(.url, handle = .curl_handle)
    .this_parsed = jsonlite::fromJSON(rawToChar(.this_response$content))
    if (!is.null(.progress_bar)) .progress_bar(amount = nrow(.this_parsed$results))
    return(.this_parsed$results)
  }

  .do_single_thread = function(.progress_bar = NULL) {
    .curl_handle = curl::new_handle(useragent = "Lynx")
    curl::handle_setheaders(.curl_handle, Authorization = getAuthHeader(authentication))

    .fetched = vector("list", length(url_vector))

    for (i in seq_along(url_vector)) {
      .fetched[[i]] = .fetch_chunk(url_vector[i], .progress_bar, .curl_handle)
    }

    return(data.table::rbindlist(.fetched, use.names = T, fill = T))
  }

  .do_multi_thread = function(.progress_bar = NULL) {
    future::plan("multisession", workers = use_cores)
    .fetched = future.apply::future_lapply(
      url_vector,
      .fetch_chunk,
      .progress_bar = .progress_bar,
      future.packages = c("curl", "jsonlite", "getWRDS")
    )
    return(data.table::rbindlist(.fetched, use.names = T, fill = T))
  }

  .do_work = function(.progress_bar = NULL) {
    if (use_multithread) return(.do_multi_thread(.progress_bar))
    return(.do_single_thread(.progress_bar))
  }

  cat(paste0(
    "Downloading ", identifier, " from WRDS (", count, " observations found)... \n"
  ))

  if (!quiet) {
    progressr::handlers(global = T)
    if (length(progressr::handlers()) == 0) {
      progressr::handlers("txtprogressbar")
    }
    progressr::with_progress({
      progress_bar = progressr::progressor(steps = count)
      return(.do_work(.progress_bar = progress_bar))
    })
  } else {
    return(.do_work())
  }
}

#' Get count of rows to be returned in WRDS query
#'
#' Returns a number indicating the number of rows that match the query
#'
#' @param identifier A string of format product.file. Product and file can be found on the Variable Descriptions page of your dataset in WRDS
#' @param filters A string in URL format specifying filters. Can easily be generated using the applyFilters function.
#' @param authentication The user's WRDS API key, accessible at wrds-api.wharton.upenn.edu. By default, the function looks for a .wrdstoken file in user's home directory that contains the API key.
#' @param root The root of your desired WRDS data's API endpoint. Usually either data or data-full. If one doesn't work, try the other.
#' @export
getWRDScount = function(identifier, filters = "", authentication = getTokenFromHome(),
                        root = "data") {
  .curl_handle = curl::new_handle(useragent = "Lynx")
  curl::handle_setheaders(.curl_handle, Authorization = getAuthHeader(authentication))
  .fetched_single = curl::curl_fetch_memory(getURL(identifier = identifier,
                                                   filters = filters,
                                                   root = root,
                                                   limit = 5), handle = .curl_handle)
  .parsed_single = jsonlite::fromJSON(rawToChar(.fetched_single$content))
  return(.parsed_single$count)
}

#' Apply filters to WRDS query
#'
#' A helper function to easily specify one or several filters. All specified filters are joined by "and" (i.e., all must be true).
#'
#' @param ... A list of filters generated using the filter_* functions
#' @export
applyFilters = function(...){
  return(paste(..., sep = "&"))
}

#' Filter less than
#'
#' All filter_* functions restrict your query based on the value of a variable x, as compared to a condition. The second part of the functions' names (after the underscore) match the comparison types available in the WRDS API. For more info, read: https://wrds-api.wharton.upenn.edu/tutorial-data-operators/
#'
#' @param x The name of the variable on which the condition should be evaluated
#' @param condition The value to which the filter should compare
#' @export
#' @importFrom utils URLencode
filter_lt = function(x, condition) {
  return(paste0(x, "__lt=", URLencode(condition)))
}

#' Filter exact
#'
#' All filter_* functions restrict your query based on the value of a variable x, as compared to a condition. The second part of the functions' names (after the underscore) match the comparison types available in the WRDS API. For more info, read: https://wrds-api.wharton.upenn.edu/tutorial-data-operators/
#'
#' @param x The name of the variable on which the condition should be evaluated
#' @param condition The value to which the filter should compare
#' @export
#' @importFrom utils URLencode
filter_exact = function(x, condition) {
  return(paste0(x, "__exact=", URLencode(condition)))
}

#' Filter greater than
#'
#' All filter_* functions restrict your query based on the value of a variable x, as compared to a condition. The second part of the functions' names (after the underscore) match the comparison types available in the WRDS API. For more info, read: https://wrds-api.wharton.upenn.edu/tutorial-data-operators/
#'
#' @param x The name of the variable on which the condition should be evaluated
#' @param condition The value to which the filter should compare
#' @export
#' @importFrom utils URLencode
filter_gt = function(x, condition) {
  return(paste0(x, "__gt=", URLencode(condition)))
}

#' Filter less than or equal to
#'
#' All filter_* functions restrict your query based on the value of a variable x, as compared to a condition. The second part of the functions' names (after the underscore) match the comparison types available in the WRDS API. For more info, read: https://wrds-api.wharton.upenn.edu/tutorial-data-operators/
#'
#' @param x The name of the variable on which the condition should be evaluated
#' @param condition The value to which the filter should compare
#' @export
#' @importFrom utils URLencode
filter_lte = function(x, condition) {
  return(paste0(x, "__lte=", URLencode(condition)))
}

#' Filter greater than or equal to
#'
#' All filter_* functions restrict your query based on the value of a variable x, as compared to a condition. The second part of the functions' names (after the underscore) match the comparison types available in the WRDS API. For more info, read: https://wrds-api.wharton.upenn.edu/tutorial-data-operators/
#'
#' @param x The name of the variable on which the condition should be evaluated
#' @param condition The value to which the filter should compare
#' @export
#' @importFrom utils URLencode
filter_gte = function(x, condition) {
  return(paste0(x, "__gte=", URLencode(condition)))
}

#' Filter contains
#'
#' All filter_* functions restrict your query based on the value of a variable x, as compared to a condition. The second part of the functions' names (after the underscore) match the comparison types available in the WRDS API. For more info, read: https://wrds-api.wharton.upenn.edu/tutorial-data-operators/
#'
#' @param x The name of the variable on which the condition should be evaluated
#' @param condition The value to which the filter should compare
#' @export
#' @importFrom utils URLencode
filter_contains = function(x, condition) {
  return(paste0(x, "__contains=", URLencode(condition)))
}

#' Filter starts with
#'
#' All filter_* functions restrict your query based on the value of a variable x, as compared to a condition. The second part of the functions' names (after the underscore) match the comparison types available in the WRDS API. For more info, read: https://wrds-api.wharton.upenn.edu/tutorial-data-operators/
#'
#' @param x The name of the variable on which the condition should be evaluated
#' @param condition The value to which the filter should compare
#' @export
#' @importFrom utils URLencode
filter_startswith = function(x, condition) {
  return(paste0(x, "__startswith=", URLencode(condition)))
}

#' Filter ends with
#'
#' All filter_* functions restrict your query based on the value of a variable x, as compared to a condition. The second part of the functions' names (after the underscore) match the comparison types available in the WRDS API. For more info, read: https://wrds-api.wharton.upenn.edu/tutorial-data-operators/
#'
#' @param x The name of the variable on which the condition should be evaluated
#' @param condition The value to which the filter should compare
#' @export
#' @importFrom utils URLencode
filter_endswith = function(x, condition) {
  return(paste0(x, "__endswith=", URLencode(condition)))
}

#' Get WRDS API key from home directory
#'
#' Looks for a .wrdstoken file in the user's home directory and, if present, returns it. If not, an error is thrown. Note: to function properly, the .wrdstoken file must contain the user's API key ONLY. This key can be found at wrds-api.wharton.upenn.edu.
#' @export
getTokenFromHome = function() {
  if(file.exists("~/.wrdstoken")) {
    c = file("~/.wrdstoken")
    return(readLines(c))
  }
  stop("No auth token specified")
}
