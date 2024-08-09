from scripts.data_ingestion import generate_params, fetch_multiple_pages
from scripts.data_processing import process_multiple_pages, save_data
from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.appName("MovieList").getOrCreate()
    api_url = "https://www.kobis.or.kr/kobisopenapi/webservice/rest/movie/searchMovieList.json"

    params = generate_params(
        key="",  # Add your key here
        openStartDt="2000",  # Add your `open` start date here
        openEndDt="2023",  # Add your `open` end date here
        itemPerPage="100",  # Add your item per page here
    )

    data = fetch_multiple_pages(api_url, params=params)
    df = process_multiple_pages(data, spark).drop(*["directors", "companys"])
    save_data(df, partition=["prdtYear", "repGenreNm"], path="data/movies")
    spark.stop()


if __name__ == "__main__":
    main()
