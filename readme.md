# getWRDS: A simple R package to get data from WRDS

Welcome to getWRDS! This package loads data from WRDS using their web API. This can make it easier to transition from a web query to R without having to use PostgreSQL syntax and, in cases where a web query is generated from multiple PostgreSQL tables, without having to worry about joining multiple files.

## Getting started

If the user wishes to use file-based authentication, they should create a text file in their home directory called `.wrdstoken` and, within it, save their WRDS API key (and nothing else!). You can find your API key at <https://wrds-api.wharton.upenn.edu/>.

## Usage

### A standard query

``` r
data = getWRDS("phlx.coph")
```

The `getWRDS` function returns data from WRDS by identifier. In this example, we use `phlx.coph`, an example dataset. This identifier follows the format `product.file`. You can find a dataset's product and file names on its Variable Descriptions page on the WRDS website.

Note: by default, the `getWRDS` function pulls your API key from the `.wrdstoken` file in your home directory.

### A filtered query

``` r
data = getWRDS("phlx.coph", applyFilters(
  filter_gte("date", "1995-01-01"),
  filter_lte("date", "1998-12-19")
))
```

To retrieve only a subset of a full dataset, you can apply filters to your query using the `applyFilters` function. The `applyFilters` function takes as its argument(s) a list of filters created using the `filter_*` functions. The second half of each `filter_*` function's name matches the naming convention given at <https://wrds-api.wharton.upenn.edu/tutorial-data-operators/>. For example, `filter_gte("date", "1995-01-01")` restricts your query to observations where the date variable has a value *greater than or equal to (gte)* 1995-01-01.

### Custom authentication

``` r
data = getWRDS("phlx.coph", authentication = "YOUR_API_KEY")
```

If you'd prefer to pass in your API key directly, you can do so using the `authentication` argument in `getWRDS`.

### Alternative URL roots

``` r
data = getWRDS("crsp_m_stock.wrds_dsfv2_query", root = "data-full")
```

For some endpoints, WRDS uses the "data-full" URL root rather than "data" (the default). If the default root doesn't work, try changing it.
