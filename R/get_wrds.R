#' Get WRDS data
#'
#' Returns WRDS data based on user query
#'
#' @param identifier A string of format product.file. Product and file can be found on the Variable Descriptions page of your dataset in WRDS
#' @param filters A string in URL format specifying filters. Can easily be generated using the applyFilters function.
#' @param authentication The user's WRDS API key, accessible at wrds-api.wharton.upenn.edu. By default, the function looks for a .wrdstoken file in user's home directory that contains the API key.
#' @param root The root of your desired WRDS data's API endpoint. Usually either data or data-full. If one doesn't work, try the other.
#' @param quiet When set to TRUE, suppresses the progress bar indicating download status.
#' @param multithread When set to TRUE, downloads the dataset in chunks to improve performance. The number of chunks depends on your machine's available cores.
#'
#' @import foreach
#' @export
getWRDS = function(identifier, filters = "", authentication = getTokenFromHome(),
                   root = "data", quiet = F, multithread = as.numeric(parallelly::availableCores()) >= 4) {
  count = getWRDScount(identifier, filters, authentication, root)

  url = getURL(identifier, filters, root)

  .do_work = function(u) {
    .loop = function(start = 0, stop = count) {
      retrieved_data = NULL
      if (start > 0) u = paste0(u, "&offset=", format(start, scientific = F))
      while(!is.null(u)) {
        if(!is.null(retrieved_data)) {
          if(nrow(retrieved_data) >= stop - start - 1) return(retrieved_data)
        }
        cmd <- paste(
          "wget",
          "--quiet",
          "--header", shQuote(getAuthHeader(authentication)),
          "-O", "-",
          shQuote(u)
        )

        raw_output <- system(cmd, intern = TRUE)
        json_string <- paste(raw_output, collapse = "\n")
        parsed_data <- jsonlite::fromJSON(json_string)

        if(!is.null(retrieved_data)){
          retrieved_data <- rbind(retrieved_data, parsed_data$results)
        } else {
          retrieved_data <- parsed_data$results
        }
        progress_bar(amount = nrow(parsed_data$results))
        u <- parsed_data$`next`
      }
      return(retrieved_data)
    }

    if (length(progressr::handlers()) == 0) {
      progressr::handlers("txtprogressbar")
    }
    cat(paste0(
      "Downloading ", identifier, " from WRDS (", count, " observations found)... \n"
    ))
    progress_bar = progressr::progressor(steps = count)

    if(multithread){
      cores = max(2, as.numeric(parallelly::availableCores()-2))
      doFuture::registerDoFuture()
      future::plan("multisession", workers = cores)
      start_end = data.frame(start = seq(0, count, by=round(count/cores, -3)))
      start_end$end = c(start_end$start[-1], count)
      return(
        foreach::foreach(i = iterators::iter(start_end, by="row"), .combine = "rbind", .export = c("u", "authentication"), .packages = c("getWRDS", "foreach")) %dopar% {
          .loop(start = i$start, stop = i$end)
        }
      )
    } else {
      return(.loop())
    }
  }

  if(!quiet) {
    progressr::with_progress({
      return(.do_work(url))
    })
  } else {
    return(.do_work(url))
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
  cmd <- paste(
    "wget",
    "--quiet",
    "--header", shQuote(getAuthHeader(authentication)),
    "-O", "-",
    shQuote(getURL(identifier = identifier, filters = filters, root = root))
  )
  raw_output <- system(cmd, intern = TRUE)
  json_string <- paste(raw_output, collapse = "\n")
  parsed_data <- jsonlite::fromJSON(json_string)
  return(parsed_data$count)
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
