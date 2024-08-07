from scripts.data_ingestion import fetch_data_from_api, generate_params
from scripts.data_processing import process_ml_data
from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.appName("MovieList").getOrCreate()
    api_url = "https://www.kobis.or.kr/kobisopenapi/webservice/rest/movie/searchMovieList.json"

    params = generate_params(
        key="",  # Add your key here
        openStartDt="1919",  # Add your `open` start date here
        openEndDt="1919",  # Add your `open` end date here
        itemPerPage="10",  # Add your item per page here
        curPage="1",  # Add your current page here
    )

    data = fetch_data_from_api(api_url, params=params)
    df = process_ml_data(data, spark).drop(*["directors", "companys"])
    df.show()
    spark.stop()


if __name__ == "__main__":
    main()
