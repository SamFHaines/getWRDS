% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/get_wrds.R
\name{getWRDScount}
\alias{getWRDScount}
\title{Get count of rows to be returned in WRDS query}
\usage{
getWRDScount(
  identifier,
  filters = "",
  authentication = getTokenFromHome(),
  root = "data"
)
}
\arguments{
\item{identifier}{A string of format product.file. Product and file can be found on the Variable Descriptions page of your dataset in WRDS}

\item{filters}{A string in URL format specifying filters. Can easily be generated using the applyFilters function.}

\item{authentication}{The user's WRDS API key, accessible at wrds-api.wharton.upenn.edu. By default, the function looks for a .wrdstoken file in user's home directory that contains the API key.}

\item{root}{The root of your desired WRDS data's API endpoint. Usually either data or data-full. If one doesn't work, try the other.}
}
\description{
Returns a number indicating the number of rows that match the query
}
