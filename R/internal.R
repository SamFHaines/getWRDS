#' @keywords internal
getAuthHeader = function(token) {
  return(paste0("Token ", token))
}

#' Returns WRDS url based on parameters
#' @keywords internal
getURL = function(identifier, filters, root, limit = 2000) {
  return(paste0(
    "https://wrds-api.wharton.upenn.edu/",
    root,
    "/",
    identifier,
    "?format=json&limit=",format(limit, scientific = F),"&",
    filters
  ))
}

#' Returns WRDS url vector based on parameters
#' @keywords internal
getURLvector = function(identifier, filters, root, count, limit = 2000) {
  base_url = paste0(
    "https://wrds-api.wharton.upenn.edu/",
    root,
    "/",
    identifier,
    "?format=json&",
    filters
  )
  base_url = trimws(base_url, "right", "&")
  offsets = seq(0, count - 1, by = limit)
  return(sprintf("%s&offset=%d&limit=%d", base_url, offsets, limit))
}
