from scripts.data_ingestion import generate_params, fetch_multiple_pages
from scripts.data_processing import process_multiple_pages
from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.appName("MovieList").getOrCreate()
    api_url = "https://www.kobis.or.kr/kobisopenapi/webservice/rest/movie/searchMovieList.json"

    params = generate_params(
        key="",  # Add your key here
        openStartDt="1919",  # Add your `open` start date here
        openEndDt="1950",  # Add your `open` end date here
        itemPerPage="100",  # Add your item per page here
    )

    data = fetch_multiple_pages(api_url, params=params)
    df = process_multiple_pages(data, spark).drop(*["directors", "companys"])
    df.show()
    spark.stop()


if __name__ == "__main__":
    main()
