#' @keywords internal
getAuthHeader = function(token) {
  return(paste0("Authorization: Token ", token))
}

#' Returns WRDS url based on parameters
#' @keywords internal
getURL = function(identifier, filters, root) {
  return(paste0(
    "https://wrds-api.wharton.upenn.edu/",
    root,
    "/",
    identifier,
    "?format=json&limit=1000&",
    filters
  ))
}
