# Project Plan

## Title
<!-- Give your project a short title. -->
Correlations between general happiness and released/consumed media genres

## Main Question

<!-- Think about one main question you want to answer based on the data. -->
1. Does the general life satisfaction in a country have an influence on the theming of movies that are being released/watched?

## Description

<!-- Describe your data science project in max. 200 words. Consider writing about why and how you attempt it. -->
Historically, in many mediums and art forms, the general life Circumstances of the population had a major impact on the nature of the creative work.
e.g. The dire circumstances of people led to the rise of romanticism in the art and music space.
This project aims to find out if the general life circumstances and happiness correlates to the releases of modern media, e.g. movies.
Maybe there is some trend to escapism in times of less happiness.

## Datasources

<!-- Describe each datasources you plan to use in a section. Use the prefic "DatasourceX" where X is the id of the datasource. -->

### Datasource1: General happiness in european countries
* Metadata URL: https://ec.europa.eu/eurostat/databrowser/view/ILC_PW01__custom_71016/bookmark/table?lang=de&bookmarkId=59450ed9-2039-47a5-80b7-b34a948aef30
* Data URL: https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/ilc_pw01/?format=TSV&compressed=false
* Data Type: TSV

Official life satisfaction data from european countries by year.


### Datasource2: OMDB (Open Media Database)

#### Datasource2.1: OMDB Movie Catalogue
* Metadata URL: https://www.omdb.org/en/us/content/Help:DataDownload
* Data URL: http://www.omdb.org/data/all_movies.csv.bz2
* Data Type: Zipped CSV
#### Datasource2.2: OMDB Movie Category Catalogue
* Metadata URL: https://www.omdb.org/en/us/content/Help:DataDownload
* Data URL: http://www.omdb.org/data/movie_categories.csv.bz2
* Data Type: Zipped CSV
#### Datasource2.3: OMDB Movie Category Names
* Metadata URL: https://www.omdb.org/en/us/content/Help:DataDownload
* Data URL: http://www.omdb.org/data/category_names.csv.bz2
* Data Type: Zipped CSV

## Work Packages

<!-- List of work packages ordered sequentially, each pointing to an issue with more details. -->

1. Analyze datasets [#1][i1]
2. Clean data for analysis [#2][i2]
3. Evaluate the data [#3][i3]
4. Write the report [#4][i4]

[i1]: https://github.com/engelharddirk/made-course/issues/1
[i2]: https://github.com/engelharddirk/made-course/issues/2
[i3]: https://github.com/engelharddirk/made-course/issues/3
[i4]: https://github.com/engelharddirk/made-course/issues/4
